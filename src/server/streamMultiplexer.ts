import { isUnrefableTimer } from '../shared/guards.ts';
import {
	normalizeSyncError,
	syncError,
	syncErrorToHttpStatus,
	type ResetManifest,
	type SyncEnvelope,
	type SyncError
} from '../shared/index.ts';
import { syncHttpError } from '../shared/protocol.ts';
import { err, ok, type SyncResult } from '../shared/result.ts';
import { encodeSseFrame, encodeSyncEnvelopeSseFrames, MAX_SSE_FRAME_BYTES } from '../shared/sse.ts';
import type { ManagerSyncPersistence } from './types.ts';

// The durable outbox remains the correctness source; this module only multiplexes live delivery and replay.
const MAX_BACKPRESSURE_QUEUE = 4096;
const JSON_HEADERS = {
	'content-type': 'application/json; charset=utf-8',
	'cache-control': 'no-store'
};

interface SharedStreamConfig {
	readonly heartbeatMs: number;
	readonly idleTtlMs: number;
	readonly maxConnectionsPerIp: number;
	readonly maxEventBytes: number;
}

interface RegisteredManager {
	readonly key: string;
	onScopeIdle?(this: void, scope: string): void;
	handleError?(
		this: void,
		error: SyncError,
		context: {
			readonly manager: string;
			readonly method: string;
			readonly scope: string;
		}
	): Promise<void> | void;
}

interface StreamState {
	readonly signal: AbortSignal;
	readonly sentCursors: Set<string>;
	readonly sentCursorOrder: string[];
	sentCursorStart: number;
	readonly replayingScopes: Map<string, number>;
	readonly bufferedByScope: Map<string, SyncEnvelope[]>;
	safeEnqueue(chunk: Uint8Array): void;
	sendEnvelope(envelope: SyncEnvelope): void;
	close(): void;
}

interface SubscriptionDetail {
	readonly managerKey: string;
	readonly scope: string;
	readonly afterCursor?: string;
	readonly replayLimit: number;
	readonly persistence: ManagerSyncPersistence;
	nextCursor(this: void): string;
}

export interface RegisterSharedStreamManagerOptions {
	readonly key: string;
	readonly heartbeatMs?: number;
	readonly idleTtlMs?: number;
	readonly maxConnectionsPerIp?: number;
	readonly maxEventBytes?: number;
	onScopeIdle?(this: void, scope: string): void;
	handleError?(
		this: void,
		error: SyncError,
		context: {
			readonly manager: string;
			readonly method: string;
			readonly scope: string;
		}
	): Promise<void> | void;
}

export interface RegisterSharedStreamSubscriptionOptions extends SubscriptionDetail {
	readonly request: Request;
}

const registeredManagers = new Map<string, RegisteredManager>();
const streamsByTransportId = new Map<string, StreamState>();
const transportIdsByScope = new Map<string, Set<string>>();
const scopesByTransportId = new Map<string, Set<string>>();
const subscriptionDetails = new Map<string, SubscriptionDetail>();
// One unref'd timer per transport and per idle scope; it is replaced on activity and cleared on teardown.
const transportTimers = new Map<string, ReturnType<typeof setTimeout>>();
// Limit duplicate physical streams per browser transport id and IP, not all users behind one IP.
const connectionsPerTransport = new Map<string, Map<string, number>>();
const idleTimers = new Map<string, ReturnType<typeof setTimeout>>();

let cachedManagerKeys: readonly string[] | undefined;

let config: SharedStreamConfig = {
	heartbeatMs: 60_000,
	idleTtlMs: 5 * 60_000,
	maxConnectionsPerIp: 2,
	maxEventBytes: 256 * 1024
};

export function registerSharedStreamManager(options: RegisterSharedStreamManagerOptions): void {
	if (registeredManagers.has(options.key)) {
		return;
	}

	registeredManagers.set(options.key, {
		key: options.key,
		onScopeIdle: options.onScopeIdle,
		handleError: options.handleError
	});
	cachedManagerKeys = undefined;

	if (options.heartbeatMs !== undefined) {
		config = { ...config, heartbeatMs: Math.min(config.heartbeatMs, options.heartbeatMs) };
	}
	if (options.idleTtlMs !== undefined) {
		config = { ...config, idleTtlMs: Math.max(config.idleTtlMs, options.idleTtlMs) };
	}
	if (options.maxConnectionsPerIp !== undefined) {
		config = { ...config, maxConnectionsPerIp: Math.max(config.maxConnectionsPerIp, options.maxConnectionsPerIp) };
	}
	if (options.maxEventBytes !== undefined) {
		config = { ...config, maxEventBytes: Math.max(config.maxEventBytes, options.maxEventBytes) };
	}
}

export async function registerSharedStreamSubscription(
	options: RegisterSharedStreamSubscriptionOptions
): Promise<SyncResult<void, SyncError>> {
	const transportId = getTransportId(options.request);
	if (!transportId) {
		return err('bad_request', 'Missing required header: x-sync-transport-id.');
	}

	clearIdle(options.scope);
	touchTransport(transportId);

	const scopeTransports = transportIdsByScope.get(options.scope) ?? new Set<string>();
	scopeTransports.add(transportId);
	transportIdsByScope.set(options.scope, scopeTransports);

	const transportScopes = scopesByTransportId.get(transportId) ?? new Set<string>();
	transportScopes.add(options.scope);
	scopesByTransportId.set(transportId, transportScopes);

	const detail = {
		managerKey: options.managerKey,
		scope: options.scope,
		afterCursor: options.afterCursor,
		replayLimit: options.replayLimit,
		persistence: options.persistence,
		nextCursor: options.nextCursor
	};
	subscriptionDetails.set(subscriptionKey(transportId, options.scope), detail);

	const stream = streamsByTransportId.get(transportId);
	if (stream) {
		// Replays are per logical scope even though the physical stream is shared by the whole browser session.
		// Defer replay until after /connect can return the authoritative scope, otherwise the SSE event can race the client response.
		startReplay(stream, detail, true);
	}

	return ok();
}

export function publishSharedStreamEnvelope(envelope: SyncEnvelope): void {
	const transportIds = transportIdsByScope.get(envelope.scope);
	if (!transportIds || transportIds.size === 0) {
		return;
	}

	// Encode once per committed envelope. Fanout can be very hot when many tabs/stores watch the same scope.
	const frames = encodeSyncEnvelopeSseFrames(envelope, config.maxEventBytes);
	for (const transportId of transportIds) {
		const stream = streamsByTransportId.get(transportId);
		if (!stream) {
			continue;
		}
		if (stream.replayingScopes.has(envelope.scope)) {
			const buffered = stream.bufferedByScope.get(envelope.scope) ?? [];
			buffered.push(envelope);
			stream.bufferedByScope.set(envelope.scope, buffered);
			continue;
		}
		// A live envelope reaches a stream once, so only replayed cursors need the window to drop a later live copy.
		if (!stream.sentCursors.has(envelope.cursor)) {
			sendFrames(stream, frames);
		}
	}
}

export async function httpSharedSyncStream(request: Request): Promise<Response> {
	let heartbeat: ReturnType<typeof setInterval> | undefined;
	let streamState: StreamState | undefined;
	let closed = false;
	let ipCounted = false;
	const streamController = new AbortController();
	const streamSignal = AbortSignal.any([request.signal, streamController.signal]);

	const transportId = getTransportId(request);
	if (!transportId) {
		return errorResponse(syncError('bad_request', 'Missing required header: x-sync-transport-id.'));
	}
	const resolvedTransportId = transportId;

	const ip = getClientIp(request);
	if (!incrementIp(resolvedTransportId, ip)) {
		return errorResponse(syncError('rate_limited', 'Too many sync streams for this IP.'));
	}
	ipCounted = true;

	streamsByTransportId.get(resolvedTransportId)?.close();
	touchTransport(resolvedTransportId);

	const stream = new ReadableStream<Uint8Array>(
		{
			start(controller) {
				function close(): void {
					if (closed) {
						return;
					}
					closed = true;
					streamController.abort();
					request.signal.removeEventListener('abort', close);
					if (heartbeat) {
						clearInterval(heartbeat);
						heartbeat = undefined;
					}
					if (ipCounted) {
						decrementIp(resolvedTransportId, ip);
						ipCounted = false;
					}
					const ownsTransport = streamState && streamsByTransportId.get(resolvedTransportId) === streamState;
					if (ownsTransport) {
						streamsByTransportId.delete(resolvedTransportId);
					}
					if (streamState) {
						clearStreamState(streamState);
					}
					if (ownsTransport) {
						clearTransportTimer(resolvedTransportId);
						removeTransportSubscriptions(resolvedTransportId);
					}
					try {
						controller.close();
					} catch {}
				}

				function safeEnqueue(chunk: Uint8Array): void {
					if (closed) {
						return;
					}
					if (chunk.byteLength > MAX_SSE_FRAME_BYTES) {
						close();
						return;
					}
					if (controller.desiredSize !== null && controller.desiredSize < -MAX_BACKPRESSURE_QUEUE) {
						close();
						return;
					}
					try {
						controller.enqueue(chunk);
					} catch {
						close();
					}
				}

				streamState = {
					signal: streamSignal,
					sentCursors: new Set(),
					sentCursorOrder: [],
					sentCursorStart: 0,
					replayingScopes: new Map(),
					bufferedByScope: new Map(),
					safeEnqueue,
					sendEnvelope(envelope) {
						if (rememberSentCursor(this, envelope.cursor)) {
							sendFrames(this, encodeSyncEnvelopeSseFrames(envelope, config.maxEventBytes));
						}
					},
					close
				};
				streamsByTransportId.set(resolvedTransportId, streamState);

				safeEnqueue(
					encodeSseFrame('hello', {
						type: 'hello',
						transportId: resolvedTransportId,
						protocol: 1,
						heartbeatMs: config.heartbeatMs,
						managers: getRegisteredSharedStreamManagerKeys()
					})
				);

				const ping = { type: 'ping' as const, transportId: resolvedTransportId, ts: 0 };
				heartbeat = setInterval(() => {
					// Reuse the heartbeat object; only the timestamp changes on this hot timer path.
					ping.ts = Date.now();
					touchTransport(resolvedTransportId);
					safeEnqueue(encodeSseFrame('ping', ping));
				}, config.heartbeatMs);
				unrefTimer(heartbeat);

				request.signal.addEventListener('abort', close, { once: true });
				if (request.signal.aborted) {
					close();
					return;
				}

				const activeStream = streamState;
				for (const scope of scopesByTransportId.get(resolvedTransportId) ?? []) {
					const detail = subscriptionDetails.get(subscriptionKey(resolvedTransportId, scope));
					if (detail) {
						startReplay(activeStream, detail, false);
					}
				}
			},
			cancel() {
				streamState?.close();
			}
		},
		{ highWaterMark: 1 }
	);

	return new Response(stream, {
		headers: {
			'content-type': 'text/event-stream; charset=utf-8',
			'cache-control': 'no-cache, no-transform',
			connection: 'keep-alive',
			'x-accel-buffering': 'no'
		}
	});
}

export function getSharedStreamConfig(): SharedStreamConfig {
	return config;
}

function getRegisteredSharedStreamManagerKeys(): readonly string[] {
	if (!cachedManagerKeys) {
		cachedManagerKeys = [...registeredManagers.keys()];
	}
	return cachedManagerKeys;
}

/** @internal Test inspection of a stream's replay dedup window. */
export function sharedStreamSentCursorCount(transportId: string): number | undefined {
	return streamsByTransportId.get(transportId)?.sentCursors.size;
}

/** @internal Test isolation for the maintained source-level Vitest suite. */
export function resetSharedStreamForTests(): void {
	for (const stream of streamsByTransportId.values()) {
		stream.close();
	}
	for (const timer of [...transportTimers.values(), ...idleTimers.values()]) {
		clearTimeout(timer);
	}
	registeredManagers.clear();
	streamsByTransportId.clear();
	transportIdsByScope.clear();
	scopesByTransportId.clear();
	subscriptionDetails.clear();
	transportTimers.clear();
	connectionsPerTransport.clear();
	idleTimers.clear();
	cachedManagerKeys = undefined;
	config = {
		heartbeatMs: 60_000,
		idleTtlMs: 5 * 60_000,
		maxConnectionsPerIp: 2,
		maxEventBytes: 256 * 1024
	};
}

function sendFrames(stream: StreamState, frames: readonly Uint8Array[] | undefined): void {
	if (!frames) {
		stream.close();
		return;
	}
	for (const frame of frames) {
		stream.safeEnqueue(frame);
	}
}

function startReplay(stream: StreamState, detail: SubscriptionDetail, deferred: boolean): void {
	const { afterCursor } = detail;
	if (!afterCursor || stream.signal.aborted) {
		return;
	}

	// Count the replay now so live publishes buffer from registration, and overlapping replays of one scope keep buffering until the last ends.
	stream.replayingScopes.set(detail.scope, (stream.replayingScopes.get(detail.scope) ?? 0) + 1);
	const run = () =>
		replaySubscription(stream, detail, afterCursor).catch((cause) => handleReplayFailure(stream, detail, cause));
	if (deferred) {
		unrefTimer(setTimeout(run, 0));
	} else {
		run();
	}
}

async function replaySubscription(stream: StreamState, detail: SubscriptionDetail, afterCursor: string): Promise<void> {
	let replaySucceeded = false;
	try {
		const replay = await detail.persistence.readAfter(detail.scope, afterCursor, detail.replayLimit + 1, {
			signal: stream.signal
		});
		if (!replay.cursorFound || replay.envelopes.length > detail.replayLimit) {
			stream.sendEnvelope(buildResetEnvelope(detail.managerKey, detail.scope, afterCursor, detail.nextCursor()));
		} else {
			for (const envelope of replay.envelopes) {
				stream.sendEnvelope(envelope);
			}
		}
		replaySucceeded = true;
	} finally {
		const remaining = (stream.replayingScopes.get(detail.scope) ?? 1) - 1;
		if (remaining > 0) {
			stream.replayingScopes.set(detail.scope, remaining);
		} else {
			stream.replayingScopes.delete(detail.scope);
			if (replaySucceeded) {
				flushBufferedScope(stream, detail.scope);
			}
		}
	}
}

async function handleReplayFailure(stream: StreamState, detail: SubscriptionDetail, cause: unknown): Promise<void> {
	if (stream.signal.aborted) {
		return;
	}
	const errorValue = normalizeSyncError(cause);
	await reportSharedStreamError(detail, errorValue);
	stream.bufferedByScope.delete(detail.scope);
	if (detail.afterCursor) {
		stream.sendEnvelope(
			buildResetEnvelope(
				detail.managerKey,
				detail.scope,
				detail.afterCursor,
				detail.nextCursor(),
				'replay_failed'
			)
		);
	}
	stream.close();
}

function flushBufferedScope(stream: StreamState, scope: string): void {
	const buffered = stream.bufferedByScope.get(scope);
	if (!buffered) {
		return;
	}
	stream.bufferedByScope.delete(scope);
	for (const envelope of buffered) {
		stream.sendEnvelope(envelope);
	}
}

function rememberSentCursor(stream: StreamState, cursor: string): boolean {
	if (stream.sentCursors.has(cursor)) {
		return false;
	}
	stream.sentCursors.add(cursor);
	stream.sentCursorOrder.push(cursor);
	while (stream.sentCursorOrder.length - stream.sentCursorStart > 4096) {
		const staleCursor = stream.sentCursorOrder[stream.sentCursorStart];
		stream.sentCursorStart += 1;
		if (staleCursor !== undefined) {
			stream.sentCursors.delete(staleCursor);
		}
	}
	if (stream.sentCursorStart > 1024 && stream.sentCursorStart * 2 > stream.sentCursorOrder.length) {
		stream.sentCursorOrder.splice(0, stream.sentCursorStart);
		stream.sentCursorStart = 0;
	}
	return true;
}

function clearStreamState(stream: StreamState): void {
	stream.sentCursors.clear();
	stream.sentCursorOrder.length = 0;
	stream.sentCursorStart = 0;
	stream.replayingScopes.clear();
	stream.bufferedByScope.clear();
}

function buildResetEnvelope(
	managerKey: string,
	scope: string,
	previousCursor: string,
	cursor: string,
	reason: ResetManifest['reason'] = 'retention_gap'
): SyncEnvelope {
	const manifest: ResetManifest = {
		scope,
		reason,
		previousCursor,
		nextCursor: cursor
	};
	return {
		managerKey,
		scope,
		cursor,
		changes: [
			{
				type: 'reset',
				manifest
			}
		],
		reset: manifest
	};
}

function getTransportId(request: Request): string | undefined {
	return (
		request.headers.get('x-sync-transport-id') ??
		new URL(request.url, 'http://sync.local').searchParams.get('transportId') ??
		undefined
	);
}

function getClientIp(request: Request): string {
	const forwarded = request.headers.get('x-forwarded-for') ?? request.headers.get('x-real-ip');
	if (forwarded) {
		const first = forwarded.split(',')[0]?.trim();
		if (first) {
			return first;
		}
	}
	return new URL(request.url).hostname || 'unknown';
}

function subscriptionKey(transportId: string, scope: string): string {
	return `${transportId}:${scope}`;
}

function getTransportTtlMs(): number {
	return Math.max(config.heartbeatMs * 3, 60_000);
}

function touchTransport(transportId: string): void {
	clearTimeout(transportTimers.get(transportId));
	transportTimers.set(transportId, unrefTimer(setTimeout(() => evictTransport(transportId), getTransportTtlMs())));
}

function clearTransportTimer(transportId: string): void {
	clearTimeout(transportTimers.get(transportId));
	transportTimers.delete(transportId);
}

function incrementIp(transportId: string, ip: string): boolean {
	const connections = connectionsPerTransport.get(transportId) ?? new Map<string, number>();
	const current = connections.get(ip) ?? 0;
	if (current >= config.maxConnectionsPerIp) {
		connectionsPerTransport.set(transportId, connections);
		return false;
	}
	connections.set(ip, current + 1);
	connectionsPerTransport.set(transportId, connections);
	return true;
}

function decrementIp(transportId: string, ip: string): void {
	const connections = connectionsPerTransport.get(transportId);
	if (!connections) {
		return;
	}
	const current = connections.get(ip) ?? 0;
	if (current <= 1) {
		connections.delete(ip);
	} else {
		connections.set(ip, current - 1);
	}
	if (connections.size === 0) {
		connectionsPerTransport.delete(transportId);
	}
}

function evictTransport(transportId: string): void {
	streamsByTransportId.get(transportId)?.close();
	streamsByTransportId.delete(transportId);
	clearTransportTimer(transportId);
	removeTransportSubscriptions(transportId);
}

function removeTransportSubscriptions(transportId: string): void {
	const scopes = scopesByTransportId.get(transportId);
	if (!scopes) {
		return;
	}
	scopesByTransportId.delete(transportId);
	for (const scope of scopes) {
		const detailKey = subscriptionKey(transportId, scope);
		const detail = subscriptionDetails.get(detailKey);
		subscriptionDetails.delete(detailKey);
		const transportIds = transportIdsByScope.get(scope);
		transportIds?.delete(transportId);
		if (transportIds && transportIds.size > 0) {
			continue;
		}
		transportIdsByScope.delete(scope);
		// Scope cleanup is delayed through a heap timer so busy apps do not pay per-scope polling cost.
		if (detail) {
			markIdle(detail.managerKey, scope);
		}
	}
}

function markIdle(managerKey: string, scope: string): void {
	clearIdle(scope);
	if (config.idleTtlMs <= 0) {
		notifyScopeIdle(managerKey, scope);
		return;
	}
	idleTimers.set(scope, unrefTimer(setTimeout(() => notifyScopeIdle(managerKey, scope), config.idleTtlMs)));
}

function clearIdle(scope: string): void {
	clearTimeout(idleTimers.get(scope));
	idleTimers.delete(scope);
}

function notifyScopeIdle(managerKey: string, scope: string): void {
	idleTimers.delete(scope);
	registeredManagers.get(managerKey)?.onScopeIdle?.(scope);
}

async function reportSharedStreamError(detail: SubscriptionDetail, errorValue: SyncError): Promise<void> {
	try {
		await registeredManagers.get(detail.managerKey)?.handleError?.(errorValue, {
			manager: detail.managerKey,
			method: 'stream.replay',
			scope: detail.scope
		});
	} catch {}
}

function unrefTimer<TTimer extends ReturnType<typeof setTimeout>>(timer: TTimer): TTimer {
	if (isUnrefableTimer(timer)) {
		timer.unref();
	}
	return timer;
}

export function jsonResponse<TBody>(body: TBody, status = 200): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: JSON_HEADERS
	});
}

export function errorResponse(error: SyncError): Response {
	return jsonResponse(syncHttpError(error), syncErrorToHttpStatus(error));
}
