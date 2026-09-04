import { isFiniteNumber, isRecord, isString } from '../shared/guards.ts';
import { isSyncEnvelope, isSyncHttpResult, normalizeSyncError } from '../shared/index.ts';
import { err, ok } from '../shared/result.ts';
import type { SyncResult } from '../shared/result.ts';
import {
	MAX_CHUNKED_SYNC_BYTES,
	MAX_SSE_FRAME_BYTES,
	parseSseEvent,
	parseSyncEnvelopeChunkJson,
	parseSyncEnvelopeJson,
	type SyncEnvelopeChunk
} from '../shared/sse.ts';
import type { RuntimeTransport, SyncCacheConfiguration, SyncEnvelope, SyncError, SyncRuntime } from './types.ts';

export interface CreateRuntimeOptions {
	readonly fetch?: typeof fetch;
	readonly cache: SyncRuntime['cache'];
	readonly streamUrl?: string;
	now?(this: void): number;
	createId?(this: void, prefix: string): string;
	readonly transport?: RuntimeTransport;
}

export interface ConfigureSyncOptions extends Omit<CreateRuntimeOptions, 'cache'> {
	readonly cache: SyncCacheConfiguration;
}

interface RuntimeTransportWithDispose extends RuntimeTransport {
	dispose?(): void;
}

interface LogicalSubscription {
	readonly key: string;
	readonly connectUrl: string;
	readonly managerKey: string;
	readonly subscribers: Set<(envelope: SyncEnvelope) => void>;
	readonly reconnectHydrators: Set<() => void>;
	scope: string;
	scopeConfirmed: boolean;
	registrationRevision: number;
	getCursor?(this: void): string | undefined;
	promise?: Promise<SyncResult<void, SyncError>>;
	connectAbort?: AbortController;
}

interface PendingSyncEnvelopeChunks {
	readonly chunks: string[];
	readonly total: number;
	readonly totalBytes: number;
	received: number;
}

const TRANSPORT_ID_STORAGE_KEY = 'sync-resource:transport-id';
const OWNER_STORAGE_KEY = 'sync-resource:owner';
const OWNER_HEARTBEAT_MS = 3000;
const OWNER_EXPIRES_MS = 10_000;
const LOGICAL_CONNECT_TIMEOUT_MS = 30_000;
const BROADCAST_CHANNEL = 'sync-resource:v1:transport';
const LOCK_NAME = 'sync-resource:transport';
const MAX_SSE_BUFFER_BYTES = MAX_SSE_FRAME_BYTES;
const MAX_PENDING_SYNC_CHUNKS = 32;
const SSE_PING_PREFIX = 'event: ping\n';
const sseTextEncoder = new TextEncoder();
type BroadcastPayload =
	| { readonly type: 'envelope'; readonly envelope: unknown }
	| { readonly type: 'reconnected'; readonly hydrate?: boolean };

function isBroadcastPayload<TValue>(value: TValue): value is TValue & BroadcastPayload {
	return (
		isRecord(value) &&
		((value.type === 'envelope' && 'envelope' in value) ||
			(value.type === 'reconnected' &&
				(value.hydrate === undefined || value.hydrate === true || value.hydrate === false)))
	);
}

function isConnectScope<TValue>(value: TValue): value is TValue & { readonly scope?: string } {
	return isRecord(value) && (value.scope === undefined || isString(value.scope));
}

function isOwnerRecord<TValue>(
	value: TValue
): value is TValue & { readonly ownerId: string; readonly expires: number } {
	return isRecord(value) && isString(value.ownerId) && isFiniteNumber(value.expires);
}

// Default runtime transport uses one browser-session stream,
// plus logical manager/scope subscriptions registered through manager-specific /connect routes.
class BrowserSessionRuntimeTransport implements RuntimeTransportWithDispose {
	private readonly fetchImpl: typeof fetch;
	private readonly streamUrl: string;
	private readonly transportId: string;
	private readonly clientId: string;
	private readonly subscriptions = new Map<string, LogicalSubscription>();
	private readonly tabId: string;
	private started = false;
	private disposed = false;
	private isOwner = false;
	private streamAbort: AbortController | undefined;
	private reconnectAttempt = 0;
	private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
	private ownerHeartbeatTimer: ReturnType<typeof setInterval> | undefined;
	private broadcast: BroadcastChannel | undefined;
	private releaseOwnerLock: (() => void) | undefined;

	constructor(
		fetchImpl: typeof fetch,
		streamUrl: string,
		transportId: string,
		clientId: string,
		createId: (prefix: string) => string
	) {
		this.fetchImpl = fetchImpl;
		this.streamUrl = streamUrl;
		this.transportId = transportId;
		this.clientId = clientId;
		this.tabId = createId('tab');
	}

	async subscribe(options: {
		readonly url: string;
		readonly connectUrl?: string;
		readonly streamUrl?: string;
		readonly managerKey?: string;
		readonly scope?: string;
		readonly signal: AbortSignal;
		getCursor?(this: void): string | undefined;
		onEnvelope(this: void, envelope: SyncEnvelope): void;
		onReconnect?(this: void): void;
	}): Promise<SyncResult<() => void, SyncError>> {
		if (!options.connectUrl || !options.managerKey || !options.scope) {
			return this.subscribeDirect(options);
		}

		const connectUrl = options.connectUrl;
		const managerKey = options.managerKey;
		const scope = options.scope;
		const key = `${managerKey}:${scope}:${connectUrl}`;
		const entry = this.getOrCreateSubscription(key, {
			connectUrl,
			managerKey,
			scope,
			getCursor: options.getCursor
		});
		entry.subscribers.add(options.onEnvelope);
		if (options.onReconnect) {
			entry.reconnectHydrators.add(options.onReconnect);
		}
		this.start();

		let settleCallerAbort: (() => void) | undefined;
		const callerAbort = new Promise<SyncResult<void, SyncError>>((resolve) => {
			settleCallerAbort = () => resolve(err('aborted', 'Realtime subscription was aborted.'));
		});
		const abort = () => {
			this.unsubscribe(entry, options.onEnvelope, options.onReconnect);
			settleCallerAbort?.();
		};
		options.signal.addEventListener('abort', abort, { once: true });
		if (options.signal.aborted) {
			abort();
			options.signal.removeEventListener('abort', abort);
			return err('aborted', 'Realtime subscription was aborted.');
		}

		const ready = await Promise.race([this.ensureLogicalSubscription(entry), callerAbort]);
		if (options.signal.aborted) {
			options.signal.removeEventListener('abort', abort);
			this.unsubscribe(entry, options.onEnvelope, options.onReconnect);
			return err('aborted', 'Realtime subscription was aborted.');
		}
		if (ready.isErr()) {
			options.signal.removeEventListener('abort', abort);
			this.unsubscribe(entry, options.onEnvelope, options.onReconnect);
			return ready;
		}

		return ok(() => {
			options.signal.removeEventListener('abort', abort);
			this.unsubscribe(entry, options.onEnvelope, options.onReconnect);
		});
	}

	dispose(): void {
		if (this.disposed) {
			return;
		}
		this.disposed = true;
		this.streamAbort?.abort();
		this.streamAbort = undefined;
		if (this.reconnectTimer) {
			clearTimeout(this.reconnectTimer);
			this.reconnectTimer = undefined;
		}
		if (this.ownerHeartbeatTimer) {
			clearInterval(this.ownerHeartbeatTimer);
			this.ownerHeartbeatTimer = undefined;
		}
		this.broadcast?.close();
		this.broadcast = undefined;
		this.releaseOwnerLock?.();
		this.releaseOwnerLock = undefined;
		for (const entry of this.subscriptions.values()) {
			entry.connectAbort?.abort();
		}
		this.subscriptions.clear();
	}

	private getOrCreateSubscription(
		key: string,
		options: {
			readonly connectUrl: string;
			readonly managerKey: string;
			readonly scope: string;
			getCursor?(this: void): string | undefined;
		}
	): LogicalSubscription {
		const existing = this.subscriptions.get(key);
		if (existing) {
			existing.getCursor = options.getCursor;
			return existing;
		}
		const entry = {
			key,
			connectUrl: options.connectUrl,
			managerKey: options.managerKey,
			scope: options.scope,
			scopeConfirmed: false,
			registrationRevision: 0,
			subscribers: new Set<(envelope: SyncEnvelope) => void>(),
			reconnectHydrators: new Set<() => void>(),
			getCursor: options.getCursor
		};
		this.subscriptions.set(key, entry);
		return entry;
	}

	private async ensureLogicalSubscription(entry: LogicalSubscription): Promise<SyncResult<void, SyncError>> {
		if (entry.promise) {
			return entry.promise;
		}

		const promise = this.runLogicalSubscription(entry).then((result) => {
			if (entry.promise === promise && result.isErr() && this.subscriptions.get(entry.key) === entry) {
				entry.promise = undefined;
			}
			return result;
		});
		entry.promise = promise;
		return entry.promise;
	}

	private async runLogicalSubscription(entry: LogicalSubscription): Promise<SyncResult<void, SyncError>> {
		while (!this.disposed && this.subscriptions.get(entry.key) === entry && entry.subscribers.size > 0) {
			const revision = entry.registrationRevision;
			const controller = new AbortController();
			entry.connectAbort = controller;
			const result = await new Promise<SyncResult<void, SyncError>>((resolve) => {
				let settled = false;
				let timedOut = false;
				function finish(next: SyncResult<void, SyncError>): void {
					if (settled) {
						return;
					}
					settled = true;
					clearTimeout(timeout);
					controller.signal.removeEventListener('abort', handleAbort);
					resolve(next);
				}
				function handleAbort(): void {
					finish(
						timedOut
							? err('timeout', 'Realtime subscription timed out.')
							: err('aborted', 'Realtime subscription was aborted.')
					);
				}
				controller.signal.addEventListener('abort', handleAbort, { once: true });
				const timeout = setTimeout(() => {
					timedOut = true;
					controller.abort();
				}, LOGICAL_CONNECT_TIMEOUT_MS);
				this.connectLogicalSubscription(entry, controller.signal).then(finish);
			});
			if (entry.connectAbort === controller) {
				entry.connectAbort = undefined;
			}
			if (this.disposed || this.subscriptions.get(entry.key) !== entry || entry.subscribers.size === 0) {
				return err('aborted', 'Realtime subscription was aborted.');
			}
			if (entry.registrationRevision !== revision) {
				continue;
			}
			return result;
		}
		return err('aborted', 'Realtime subscription was aborted.');
	}

	private async connectLogicalSubscription(
		entry: LogicalSubscription,
		signal: AbortSignal
	): Promise<SyncResult<void, SyncError>> {
		try {
			const response = await this.fetchImpl(urlWithCursor(entry.connectUrl, entry.getCursor?.()), {
				method: 'POST',
				headers: {
					'content-type': 'application/json',
					'x-sync-transport-id': this.transportId,
					'x-client-id': this.clientId
				},
				signal
			});
			if (signal.aborted) {
				return err('aborted', 'Realtime subscription was aborted.');
			}
			if (!response.ok) {
				return err(
					response.status === 429 ? 'rate_limited' : 'bad_request',
					`Sync subscribe failed: ${response.status}`
				);
			}
			const payload = await response.json();
			if (signal.aborted) {
				return err('aborted', 'Realtime subscription was aborted.');
			}
			if (!isSyncHttpResult<{ readonly scope?: unknown }>(payload)) {
				return err('validation', 'Sync subscribe response payload was malformed.', { details: payload });
			}
			if (payload.isError) {
				return err(normalizeSyncError(payload.error));
			}
			if (isConnectScope(payload.value) && payload.value.scope !== undefined) {
				// The server owns scope encoding because manager.scope(params) can be narrower than frontend params.
				entry.scope = payload.value.scope;
				entry.scopeConfirmed = true;
			}
			return ok(undefined);
		} catch (cause) {
			if (signal.aborted) {
				return err('aborted', 'Realtime subscription was aborted.');
			}
			return err('internal', cause instanceof Error ? cause.message : String(cause), { cause });
		}
	}

	private unsubscribe(
		entry: LogicalSubscription,
		subscriber: (envelope: SyncEnvelope) => void,
		reconnectHydrator: (() => void) | undefined
	): void {
		entry.subscribers.delete(subscriber);
		if (reconnectHydrator) {
			entry.reconnectHydrators.delete(reconnectHydrator);
		}
		if (entry.subscribers.size > 0) {
			return;
		}
		this.subscriptions.delete(entry.key);
		entry.connectAbort?.abort();
		entry.connectAbort = undefined;
		this.stopOwnerStreamIfIdle();
	}

	private stopOwnerStreamIfIdle(): void {
		if (this.subscriptions.size > 0) {
			return;
		}
		// Do not keep a server subscription consuming cursors when no local store can observe the events.
		this.isOwner = false;
		this.started = false;
		this.streamAbort?.abort();
		this.streamAbort = undefined;
		this.reconnectAttempt = 0;
		if (this.reconnectTimer) {
			clearTimeout(this.reconnectTimer);
			this.reconnectTimer = undefined;
		}
		if (this.ownerHeartbeatTimer) {
			clearInterval(this.ownerHeartbeatTimer);
			this.ownerHeartbeatTimer = undefined;
		}
		this.broadcast?.close();
		this.broadcast = undefined;
		this.releaseOwnerLock?.();
		this.releaseOwnerLock = undefined;
	}

	private start(): void {
		if (this.started || this.disposed) {
			return;
		}
		this.started = true;
		this.openBroadcastChannel();
		if (!isBrowserRuntime()) {
			this.isOwner = true;
			this.connectOwnerStream();
			return;
		}
		const locks = navigator.locks;
		if (locks) {
			locks
				.request(LOCK_NAME, async () => {
					if (this.disposed || this.subscriptions.size === 0) {
						return;
					}
					this.isOwner = true;
					this.startOwnerHeartbeat();
					this.connectOwnerStream();
					await new Promise<void>((resolve) => {
						this.releaseOwnerLock = resolve;
					});
				})
				.catch(() => this.startLocalStorageOwnership());
			return;
		}
		this.startLocalStorageOwnership();
	}

	private openBroadcastChannel(): void {
		if (!isBrowserRuntime() || !globalThis.BroadcastChannel) {
			return;
		}
		if (this.broadcast) {
			return;
		}
		this.broadcast = new BroadcastChannel(BROADCAST_CHANNEL);
		this.broadcast.onmessage = (message) => {
			const data = message.data;
			if (!isBroadcastPayload(data)) {
				return;
			}
			if (data.type === 'envelope' && isSyncEnvelope(data.envelope)) {
				this.dispatchEnvelope(data.envelope);
			}
			if (data.type === 'reconnected') {
				this.handleReconnect(data.hydrate !== false);
			}
		};
	}

	private startLocalStorageOwnership(): void {
		if (this.disposed || this.isOwner) {
			return;
		}
		const now = Date.now();
		const owner = readOwner();
		if (owner && owner.ownerId !== this.tabId && owner.expires > now) {
			this.ownerHeartbeatTimer = setTimeout(() => this.startLocalStorageOwnership(), OWNER_EXPIRES_MS);
			return;
		}
		this.isOwner = true;
		this.writeOwnerHeartbeat();
		this.startOwnerHeartbeat();
		this.connectOwnerStream();
	}

	private startOwnerHeartbeat(): void {
		if (!isBrowserRuntime()) {
			return;
		}
		if (this.ownerHeartbeatTimer) {
			clearInterval(this.ownerHeartbeatTimer);
		}
		this.ownerHeartbeatTimer = setInterval(() => this.writeOwnerHeartbeat(), OWNER_HEARTBEAT_MS);
	}

	private writeOwnerHeartbeat(): void {
		try {
			window.localStorage.setItem(
				OWNER_STORAGE_KEY,
				JSON.stringify({
					ownerId: this.tabId,
					expires: Date.now() + OWNER_EXPIRES_MS
				})
			);
		} catch {}
	}

	private async connectOwnerStream(): Promise<SyncResult<void, SyncError>> {
		if (this.disposed || !this.isOwner) {
			return ok(undefined);
		}
		const controller = new AbortController();
		this.streamAbort = controller;
		try {
			const response = await this.fetchImpl(this.streamUrl, {
				method: 'POST',
				headers: {
					accept: 'text/event-stream',
					'x-sync-transport-id': this.transportId,
					'x-client-id': this.clientId
				},
				signal: controller.signal
			});
			if (!response.ok || !response.body) {
				throw new Error(`Sync stream failed: ${response.status}`);
			}

			const wasReconnect = this.reconnectAttempt > 0;
			this.reconnectAttempt = 0;
			this.handleReconnect(wasReconnect);
			this.broadcast?.postMessage({ type: 'reconnected', hydrate: wasReconnect });

			const readResult = await readSseStream(
				response.body.getReader(),
				controller.signal,
				(envelope) => {
					this.dispatchEnvelope(envelope);
					this.broadcast?.postMessage({ type: 'envelope', envelope });
				},
				() => controller.abort()
			);
			if (!this.disposed && this.isOwner) {
				this.scheduleReconnect();
			}
			return readResult;
		} catch (cause) {
			if (!this.disposed && this.isOwner) {
				this.scheduleReconnect();
			}
			return err(
				controller.signal.aborted ? 'aborted' : 'internal',
				cause instanceof Error ? cause.message : String(cause),
				{ cause }
			);
		}
	}

	private scheduleReconnect(): void {
		if (this.reconnectTimer || this.disposed || !this.isOwner) {
			return;
		}
		const baseDelay = Math.min(1000 * Math.pow(2, this.reconnectAttempt), 60_000);
		const jitteredDelay = baseDelay + (Math.random() * 2 - 1) * baseDelay * 0.2;
		this.reconnectAttempt += 1;
		this.reconnectTimer = setTimeout(
			() => {
				this.reconnectTimer = undefined;
				this.connectOwnerStream();
			},
			Math.max(0, Math.min(60_000, Math.round(jitteredDelay)))
		);
	}

	private handleReconnect(hydrate: boolean): void {
		for (const entry of this.subscriptions.values()) {
			entry.registrationRevision += 1;
			if (entry.connectAbort) {
				entry.connectAbort.abort();
			} else {
				entry.promise = undefined;
				this.ensureLogicalSubscription(entry);
			}
			if (!hydrate) {
				continue;
			}
			for (const reconnectHydrator of entry.reconnectHydrators) {
				reconnectHydrator();
			}
		}
	}

	private dispatchEnvelope(envelope: SyncEnvelope): void {
		let delivered = false;
		for (const entry of this.subscriptions.values()) {
			if (entry.managerKey !== envelope.managerKey || entry.scope !== envelope.scope) {
				continue;
			}
			for (const subscriber of entry.subscribers) {
				subscriber(envelope);
			}
			delivered = true;
		}
		if (delivered) {
			return;
		}
		const unconfirmedCandidates: LogicalSubscription[] = [];
		for (const entry of this.subscriptions.values()) {
			if (entry.managerKey !== envelope.managerKey || entry.scopeConfirmed) {
				continue;
			}
			unconfirmedCandidates.push(entry);
		}
		if (unconfirmedCandidates.length !== 1) {
			return;
		}
		const entry = unconfirmedCandidates[0];
		if (!entry) {
			return;
		}
		// Server scope is authoritative; only auto-adopt it when there is one possible pending subscription.
		entry.scope = envelope.scope;
		entry.scopeConfirmed = true;
		for (const subscriber of entry.subscribers) {
			subscriber(envelope);
		}
	}

	private async subscribeDirect(options: {
		readonly url: string;
		readonly signal: AbortSignal;
		getCursor?(this: void): string | undefined;
		onEnvelope(this: void, envelope: SyncEnvelope): void;
	}): Promise<SyncResult<() => void, SyncError>> {
		const controller = new AbortController();
		const abort = () => controller.abort(options.signal.reason);
		if (options.signal.aborted) {
			abort();
			return err('aborted', 'Realtime subscription was aborted.');
		}
		options.signal.addEventListener('abort', abort, { once: true });
		try {
			const response = await this.fetchImpl(urlWithCursor(options.url, options.getCursor?.()), {
				headers: {
					accept: 'text/event-stream'
				},
				signal: controller.signal
			});
			if (controller.signal.aborted) {
				options.signal.removeEventListener('abort', abort);
				return err('aborted', 'Realtime subscription was aborted.');
			}
			if (!response.ok || !response.body) {
				options.signal.removeEventListener('abort', abort);
				return err('bad_request', `Failed to subscribe: ${response.status}`);
			}
			readSseStream(response.body.getReader(), controller.signal, options.onEnvelope, () => controller.abort());
			return ok(() => {
				options.signal.removeEventListener('abort', abort);
				controller.abort();
			});
		} catch (cause) {
			options.signal.removeEventListener('abort', abort);
			return err(
				controller.signal.aborted ? 'aborted' : 'internal',
				cause instanceof Error ? cause.message : String(cause),
				{ cause }
			);
		}
	}
}

export function createRuntime(options: CreateRuntimeOptions): SyncRuntime {
	let idIndex = 0;
	const fetchImpl = options.fetch ?? fetch;
	const runtimeIdSeed = createRuntimeIdSeed();
	const createId = options.createId ?? ((prefix: string) => `${prefix}_${runtimeIdSeed}_${++idIndex}`);
	const clientId = createId('client');
	const transportId = getStoredTransportId(createId);
	const transport: RuntimeTransportWithDispose =
		options.transport ??
		new BrowserSessionRuntimeTransport(
			fetchImpl,
			options.streamUrl ?? '/api/sync-resource/stream',
			transportId,
			clientId,
			createId
		);
	return {
		fetch: fetchImpl,
		cache: options.cache,
		transport,
		clientId,
		now: options.now ?? (() => Date.now()),
		createId,
		dispose() {
			transport.dispose?.();
		}
	};
}

let configuredRuntime: SyncRuntime | undefined;
let configuredCache: SyncCacheConfiguration | undefined;

export function configureSync(options: ConfigureSyncOptions): void {
	configuredRuntime?.dispose();
	configuredCache = options.cache;
	configuredRuntime = createRuntime({
		...options,
		cache: options.cache.adapter
	});
}

export function getSyncRuntime(): SyncRuntime {
	if (!configuredRuntime) {
		throw new Error('Sync runtime is not configured. Call configureSync(...) before creating sync stores.');
	}
	return configuredRuntime;
}

export function resetSyncConfiguration(): void {
	configuredRuntime?.dispose();
	configuredRuntime = undefined;
	configuredCache = undefined;
}

export function getSyncCacheOptions(): SyncCacheConfiguration {
	if (!configuredCache) {
		throw new Error('Sync cache is not configured. Call configureSync(...) before creating sync stores.');
	}
	return configuredCache;
}

function urlWithCursor(urlText: string, cursor: string | undefined): string {
	if (!cursor) {
		return urlText;
	}

	const isAbsolute = /^https?:\/\//.test(urlText);
	const url = new URL(urlText, 'http://sync.local');
	url.searchParams.set('after', cursor);
	return isAbsolute ? url.href : url.pathname + url.search;
}

function getStoredTransportId(createId: (prefix: string) => string): string {
	if (!isBrowserRuntime()) {
		return createId('transport');
	}
	try {
		const existing = window.localStorage.getItem(TRANSPORT_ID_STORAGE_KEY);
		if (existing) {
			return existing;
		}
		const next = createId('transport');
		window.localStorage.setItem(TRANSPORT_ID_STORAGE_KEY, next);
		return next;
	} catch {
		return createId('transport');
	}
}

function isBrowserRuntime(): boolean {
	return globalThis.window !== undefined && globalThis.navigator !== undefined;
}

function createRuntimeIdSeed(): string {
	const cryptoValue = globalThis.crypto;
	if (cryptoValue?.randomUUID) {
		// Client and mutation ids are app-internal finality keys; a per-runtime seed avoids cross-session counter collisions.
		return cryptoValue.randomUUID().replaceAll('-', '');
	}
	return `${Date.now().toString(36)}${Math.random().toString(36).slice(2)}`;
}

function readOwner(): { readonly ownerId: string; readonly expires: number } | undefined {
	if (!isBrowserRuntime()) {
		return undefined;
	}
	try {
		const raw = window.localStorage.getItem(OWNER_STORAGE_KEY);
		if (!raw) {
			return undefined;
		}
		const parsed = JSON.parse(raw);
		if (!isOwnerRecord(parsed)) {
			return undefined;
		}
		return {
			ownerId: parsed.ownerId,
			expires: parsed.expires
		};
	} catch {
		return undefined;
	}
}

async function readSseStream(
	reader: ReadableStreamDefaultReader<Uint8Array>,
	signal: AbortSignal,
	onEnvelope: (envelope: SyncEnvelope) => void,
	onTerminalFailure: () => void
): Promise<SyncResult<void, SyncError>> {
	let buffer = '';
	let bufferBytes = 0;
	let terminalFailure = false;
	const textDecoder = new TextDecoder('utf-8', { fatal: true });
	const pendingSyncChunks = new Map<string, PendingSyncEnvelopeChunks>();
	const abort = () => {
		cancelReader(reader);
	};
	signal.addEventListener('abort', abort, { once: true });
	try {
		while (!signal.aborted) {
			const chunk = await reader.read();
			if (chunk.done) {
				return ok(undefined);
			}
			bufferBytes += chunk.value.byteLength;
			buffer += textDecoder.decode(chunk.value, { stream: true });
			let eventEnd = buffer.indexOf('\n\n');
			while (eventEnd >= 0) {
				const eventText = buffer.slice(0, eventEnd);
				buffer = buffer.slice(eventEnd + 2);
				const frameBytes = sseTextEncoder.encode(eventText).byteLength + 2;
				bufferBytes = Math.max(0, bufferBytes - frameBytes);
				if (frameBytes > MAX_SSE_BUFFER_BYTES) {
					terminalFailure = true;
					return err('payload_too_large', 'Sync SSE frame exceeded max size.');
				}
				const envelope = parseSseEnvelope(eventText, pendingSyncChunks);
				if (envelope) {
					onEnvelope(envelope);
				}
				eventEnd = buffer.indexOf('\n\n');
			}
			if (bufferBytes > MAX_SSE_BUFFER_BYTES) {
				// Bound malformed or stalled streams so a broken SSE response cannot grow memory without limit.
				terminalFailure = true;
				return err('payload_too_large', 'Sync SSE buffer exceeded max size.');
			}
		}
		return ok(undefined);
	} catch (cause) {
		terminalFailure = !signal.aborted;
		return err(signal.aborted ? 'aborted' : 'internal', cause instanceof Error ? cause.message : String(cause), {
			cause
		});
	} finally {
		signal.removeEventListener('abort', abort);
		try {
			reader.releaseLock();
		} catch {}
		if (terminalFailure) {
			onTerminalFailure();
		}
	}
}

async function cancelReader(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<SyncResult<void, SyncError>> {
	try {
		await reader.cancel();
		return ok(undefined);
	} catch (cause) {
		return err('internal', cause instanceof Error ? cause.message : String(cause), { cause });
	}
}

function parseSseEnvelope(
	eventText: string,
	pendingSyncChunks: Map<string, PendingSyncEnvelopeChunks>
): SyncEnvelope | undefined {
	if (eventText.startsWith(SSE_PING_PREFIX)) {
		return undefined;
	}

	const { eventName, data } = parseSseEvent(eventText);

	if (eventName === 'ping') {
		return undefined;
	}
	if (!data) {
		return undefined;
	}
	if (eventName === 'sync') {
		return parseSyncEnvelopeJson(data);
	}
	if (eventName !== 'sync-chunk') {
		return undefined;
	}

	return acceptSyncEnvelopeChunk(parseSyncEnvelopeChunkJson(data), pendingSyncChunks);
}

function acceptSyncEnvelopeChunk(
	chunk: SyncEnvelopeChunk | undefined,
	pendingSyncChunks: Map<string, PendingSyncEnvelopeChunks>
): SyncEnvelope | undefined {
	if (
		!chunk ||
		chunk.type !== 'sync-chunk' ||
		chunk.total <= 0 ||
		chunk.index < 0 ||
		chunk.index >= chunk.total ||
		chunk.totalBytes <= 0 ||
		chunk.totalBytes > MAX_CHUNKED_SYNC_BYTES
	) {
		return undefined;
	}

	let pending = pendingSyncChunks.get(chunk.id);
	if (!pending) {
		if (pendingSyncChunks.size >= MAX_PENDING_SYNC_CHUNKS) {
			pruneOldestPendingSyncChunk(pendingSyncChunks);
		}
		pending = {
			chunks: [],
			total: chunk.total,
			totalBytes: chunk.totalBytes,
			received: 0
		};
		pendingSyncChunks.set(chunk.id, pending);
	}

	if (pending.total !== chunk.total || pending.totalBytes !== chunk.totalBytes) {
		pendingSyncChunks.delete(chunk.id);
		return undefined;
	}

	if (pending.chunks[chunk.index] === undefined) {
		pending.chunks[chunk.index] = chunk.data;
		pending.received += 1;
	}

	if (pending.received < pending.total) {
		return undefined;
	}

	pendingSyncChunks.delete(chunk.id);
	const serialized = pending.chunks.join('');
	if (sseTextEncoder.encode(serialized).byteLength !== pending.totalBytes) {
		return undefined;
	}

	return parseSyncEnvelopeJson(serialized);
}

function pruneOldestPendingSyncChunk(pendingSyncChunks: Map<string, PendingSyncEnvelopeChunks>): void {
	const first = pendingSyncChunks.keys().next();
	if (!first.done) {
		pendingSyncChunks.delete(first.value);
	}
}
