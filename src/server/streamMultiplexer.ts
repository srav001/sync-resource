import { isUnrefableTimer } from '../shared/guards.ts';
import { err, ok, type SyncResult } from '../shared/result.js';
import { encodeSseFrame, encodeSyncEnvelopeSseFrames, MAX_SSE_FRAME_BYTES } from '../shared/sse.ts';
import { normalizeSyncError, syncError, toSyncProtocolError, type SyncError } from './errors.js';
import type { ManagerSyncPersistence, ResetManifest, SyncEnvelope } from './types.js';

// V1 keeps one physical browser-session stream and registers manager/scope subscriptions separately.
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

interface IdleHeapEntry {
	readonly managerKey: string;
	readonly scope: string;
	readonly expiry: number;
	readonly epoch: number;
}

interface StreamState {
	readonly signal: AbortSignal;
	readonly sentCursors: Set<string>;
	readonly sentCursorOrder: string[];
	sentCursorStart: number;
	readonly replayingScopes: Set<string>;
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
const transportExpiresAtByTransportId = new Map<string, number>();
// Limit duplicate physical streams per browser transport id and IP, not all users behind one IP.
const connectionsPerTransport = new Map<string, Map<string, number>>();
const idleExpiryByScope = new Map<string, number>();
const idleEpochByScope = new Map<string, number>();
const idleExpiryHeap: IdleHeapEntry[] = [];

let cachedManagerKeys: readonly string[] | undefined;
let transportSweepTimer: ReturnType<typeof setTimeout> | undefined;
let idleSweepTimer: ReturnType<typeof setTimeout> | undefined;

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
		scheduleReplaySubscription(stream, detail);
	}

	return ok();
}

export function publishSharedStreamEnvelope(envelope: SyncEnvelope): void {
	const transportIds = transportIdsByScope.get(envelope.scope);
	if (!transportIds || transportIds.size === 0) {
		return;
	}

	// Encode once per committed envelope. Fanout can be very hot when many tabs/stores watch the same scope.
	const frames = encodeSyncEnvelopeSseFrames(envelope, { maxEnvelopeBytes: config.maxEventBytes });
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
		if (rememberSentCursor(stream, envelope.cursor)) {
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
		return jsonResponse(
			{
				isOk: false,
				isError: true,
				error: toSyncProtocolError(syncError('bad_request', 'Missing required header: x-sync-transport-id.'))
			},
			400
		);
	}
	const resolvedTransportId = transportId;

	const ip = getClientIp(request);
	if (!incrementIp(resolvedTransportId, ip)) {
		return jsonResponse(
			{
				isOk: false,
				isError: true,
				error: toSyncProtocolError(syncError('rate_limited', 'Too many sync streams for this IP.'))
			},
			429
		);
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
						transportExpiresAtByTransportId.delete(resolvedTransportId);
						removeTransportSubscriptions(resolvedTransportId);
						clearTransportSweepTimerIfIdle();
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
					replayingScopes: new Set(),
					bufferedByScope: new Map(),
					safeEnqueue,
					sendEnvelope(envelope) {
						if (rememberSentCursor(this, envelope.cursor)) {
							sendFrames(
								this,
								encodeSyncEnvelopeSseFrames(envelope, { maxEnvelopeBytes: config.maxEventBytes })
							);
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
						replaySubscription(activeStream, detail).catch((cause) =>
							handleReplayFailure(activeStream, detail, cause)
						);
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

export function getRegisteredSharedStreamManagerKeys(): readonly string[] {
	if (!cachedManagerKeys) {
		cachedManagerKeys = [...registeredManagers.keys()];
	}
	return cachedManagerKeys;
}

/** @internal Test isolation for the maintained source-level Vitest suite. */
export function resetSharedStreamForTests(): void {
	for (const stream of streamsByTransportId.values()) {
		stream.close();
	}
	if (transportSweepTimer) {
		clearTimeout(transportSweepTimer);
		transportSweepTimer = undefined;
	}
	if (idleSweepTimer) {
		clearTimeout(idleSweepTimer);
		idleSweepTimer = undefined;
	}
	registeredManagers.clear();
	streamsByTransportId.clear();
	transportIdsByScope.clear();
	scopesByTransportId.clear();
	subscriptionDetails.clear();
	transportExpiresAtByTransportId.clear();
	connectionsPerTransport.clear();
	idleExpiryByScope.clear();
	idleEpochByScope.clear();
	idleExpiryHeap.length = 0;
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

async function replaySubscription(stream: StreamState, detail: SubscriptionDetail): Promise<void> {
	if (!detail.afterCursor || stream.signal.aborted) {
		return;
	}

	stream.replayingScopes.add(detail.scope);
	let replaySucceeded = false;
	try {
		const replay = await detail.persistence.readAfter(detail.scope, detail.afterCursor, detail.replayLimit, {
			signal: stream.signal
		});
		if (!replay.cursorFound && replay.retainedEnvelopeCount > 0) {
			stream.sendEnvelope(
				buildResetEnvelope(detail.managerKey, detail.scope, detail.afterCursor, detail.nextCursor())
			);
		}
		for (const envelope of replay.envelopes) {
			stream.sendEnvelope(envelope);
		}
		replaySucceeded = true;
	} finally {
		stream.replayingScopes.delete(detail.scope);
		if (replaySucceeded) {
			flushBufferedScope(stream, detail.scope);
		}
	}
}

function scheduleReplaySubscription(stream: StreamState, detail: SubscriptionDetail): void {
	const timer = setTimeout(() => {
		replaySubscription(stream, detail).catch((cause) => handleReplayFailure(stream, detail, cause));
	}, 0);
	unrefTimer(timer);
}

async function handleReplayFailure(stream: StreamState, detail: SubscriptionDetail, cause: unknown): Promise<void> {
	if (stream.signal.aborted) {
		return;
	}
	const errorValue = normalizeSyncError(cause);
	await reportSharedStreamError(detail, errorValue);
	stream.replayingScopes.delete(detail.scope);
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
	return new URL(request.url, 'http://sync.local').hostname || 'unknown';
}

function subscriptionKey(transportId: string, scope: string): string {
	return `${transportId}:${scope}`;
}

function getTransportTtlMs(): number {
	return Math.max(config.heartbeatMs * 3, 60_000);
}

function touchTransport(transportId: string): void {
	transportExpiresAtByTransportId.set(transportId, Date.now() + getTransportTtlMs());
	scheduleTransportSweep();
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
	transportExpiresAtByTransportId.delete(transportId);
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

function scheduleTransportSweep(): void {
	if (transportSweepTimer || transportExpiresAtByTransportId.size === 0) {
		return;
	}

	let nextExpiry = Infinity;
	for (const expiresAt of transportExpiresAtByTransportId.values()) {
		if (expiresAt < nextExpiry) {
			nextExpiry = expiresAt;
		}
	}
	const delay = Math.max(0, nextExpiry - Date.now());
	transportSweepTimer = setTimeout(() => {
		transportSweepTimer = undefined;
		const now = Date.now();
		for (const [transportId, expiresAt] of transportExpiresAtByTransportId) {
			if (expiresAt <= now) {
				evictTransport(transportId);
			}
		}
		scheduleTransportSweep();
	}, delay);
	unrefTimer(transportSweepTimer);
}

function markIdle(managerKey: string, scope: string): void {
	if (config.idleTtlMs <= 0) {
		notifyScopeIdle(managerKey, scope);
		return;
	}

	const expiry = Date.now() + config.idleTtlMs;
	const epoch = (idleEpochByScope.get(scope) ?? 0) + 1;
	idleEpochByScope.set(scope, epoch);
	idleExpiryByScope.set(scope, expiry);
	pushIdleEntry({ managerKey, scope, expiry, epoch });
	scheduleIdleSweep();
}

function clearIdle(scope: string): void {
	idleExpiryByScope.delete(scope);
	idleEpochByScope.delete(scope);
}

function notifyScopeIdle(managerKey: string, scope: string): void {
	idleExpiryByScope.delete(scope);
	idleEpochByScope.delete(scope);
	registeredManagers.get(managerKey)?.onScopeIdle?.(scope);
}

async function reportSharedStreamError(
	detail: SubscriptionDetail,
	errorValue: SyncError
): Promise<SyncResult<void, SyncError>> {
	const manager = registeredManagers.get(detail.managerKey);
	if (!manager?.handleError) {
		return ok();
	}
	try {
		await manager.handleError(errorValue, {
			manager: detail.managerKey,
			method: 'events',
			scope: detail.scope
		});
		return ok();
	} catch (cause) {
		return err(normalizeSyncError(cause));
	}
}

function scheduleIdleSweep(): void {
	if (idleSweepTimer || idleExpiryByScope.size === 0) {
		return;
	}

	const next = peekActiveIdleEntry();
	if (!next) {
		return;
	}

	idleSweepTimer = setTimeout(
		() => {
			idleSweepTimer = undefined;
			const now = Date.now();
			while (true) {
				const entry = peekActiveIdleEntry();
				if (!entry || entry.expiry > now) {
					break;
				}
				popIdleEntry();
				if ((transportIdsByScope.get(entry.scope)?.size ?? 0) > 0) {
					clearIdle(entry.scope);
					continue;
				}
				notifyScopeIdle(entry.managerKey, entry.scope);
			}
			scheduleIdleSweep();
		},
		Math.max(0, next.expiry - Date.now())
	);
	unrefTimer(idleSweepTimer);
}

function clearTransportSweepTimerIfIdle(): void {
	if (transportExpiresAtByTransportId.size > 0 || !transportSweepTimer) {
		return;
	}
	clearTimeout(transportSweepTimer);
	transportSweepTimer = undefined;
}

function unrefTimer(timer: ReturnType<typeof setTimeout> | undefined): void {
	if (timer && isUnrefableTimer(timer)) {
		timer.unref();
	}
}

function pushIdleEntry(entry: IdleHeapEntry): void {
	idleExpiryHeap.push(entry);
	let current = idleExpiryHeap.length - 1;
	while (current > 0) {
		const parent = Math.floor((current - 1) / 2);
		const parentEntry = idleExpiryHeap[parent];
		if (parentEntry === undefined || parentEntry.expiry <= entry.expiry) {
			return;
		}
		idleExpiryHeap[current] = parentEntry;
		current = parent;
	}
	idleExpiryHeap[current] = entry;
}

function popIdleEntry(): IdleHeapEntry | undefined {
	if (idleExpiryHeap.length === 0) {
		return undefined;
	}
	const first = idleExpiryHeap[0];
	if (first === undefined) {
		return undefined;
	}
	const last = idleExpiryHeap.pop();
	if (last !== undefined && idleExpiryHeap.length > 0) {
		let current = 0;
		while (true) {
			const left = current * 2 + 1;
			const right = left + 1;
			let next = current;
			const currentEntry = idleExpiryHeap[next];
			const leftEntry = idleExpiryHeap[left];
			if (currentEntry !== undefined && leftEntry !== undefined && leftEntry.expiry < currentEntry.expiry) {
				next = left;
			}
			const nextEntry = idleExpiryHeap[next];
			const rightEntry = idleExpiryHeap[right];
			if (nextEntry !== undefined && rightEntry !== undefined && rightEntry.expiry < nextEntry.expiry) {
				next = right;
			}
			if (next === current) {
				break;
			}
			const selectedEntry = idleExpiryHeap[next];
			if (selectedEntry === undefined) {
				break;
			}
			idleExpiryHeap[current] = selectedEntry;
			current = next;
		}
		idleExpiryHeap[current] = last;
	}
	return first;
}

function peekActiveIdleEntry(): IdleHeapEntry | undefined {
	while (idleExpiryHeap.length > 0) {
		const entry = idleExpiryHeap[0];
		if (entry === undefined) {
			return undefined;
		}
		if (idleExpiryByScope.get(entry.scope) === entry.expiry && idleEpochByScope.get(entry.scope) === entry.epoch) {
			return entry;
		}
		popIdleEntry();
	}
	return undefined;
}

function jsonResponse<TBody>(body: TBody, status = 200): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: JSON_HEADERS
	});
}
