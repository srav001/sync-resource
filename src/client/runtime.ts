import { isFiniteNumber, isRecord, isString } from '../shared/guards.ts';
import { isSyncHttpResult, normalizeSyncError, type SyncHttpOk } from '../shared/index.ts';
import { MAX_BATCH_CONNECTS, syncErrorCodeFromHttpStatus } from '../shared/protocol.ts';
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
import type {
	CacheAdapter,
	RuntimeTransport,
	SyncCacheConfiguration,
	SyncEnvelope,
	SyncError,
	SyncRuntime
} from './types.ts';

export interface CreateRuntimeOptions {
	readonly fetch?: typeof fetch;
	readonly cache: SyncRuntime['cache'];
	readonly streamUrl?: string;
	readonly connectUrl?: string;
	readonly batchConnect?: boolean;
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
	retryAttempt: number;
	retryTimer?: ReturnType<typeof setTimeout>;
	getCursor?(this: void): string | undefined;
	promise?: Promise<SyncResult<void, SyncError>>;
	connectAbort?: AbortController;
}

interface QueuedConnect {
	readonly url: string;
	readonly signal: AbortSignal;
	resolve(this: void, response: Response): void;
	reject(this: void, cause: unknown): void;
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
const MAX_REGISTRATION_HOLD_MS = 2_000;
const MAX_STREAM_RECONNECT_MS = 60_000;
const MAX_REGISTRATION_RETRY_MS = 30_000;
const BROADCAST_CHANNEL = 'sync-resource:v2:transport';
const CACHE_SWEEP_DELAY_MS = 60_000;
const CACHE_SWEEP_INTERVAL_MS = 24 * 60 * 60 * 1000;
const CACHE_SWEEP_KEY = 'sync-resource:cache-sweep';
const LOCK_NAME = 'sync-resource:transport';
const MAX_PENDING_SYNC_CHUNKS = 32;
const sseTextEncoder = new TextEncoder();
function backoffDelay(attempt: number, maxMs: number): number {
	const baseDelay = Math.min(1000 * Math.pow(2, attempt), maxMs);
	const jitteredDelay = baseDelay + (Math.random() * 2 - 1) * baseDelay * 0.2;
	return Math.max(0, Math.min(maxMs, Math.round(jitteredDelay)));
}

function isConnectScope<TValue>(value: TValue): value is TValue & { readonly scope?: string } {
	return isRecord(value) && (value.scope === undefined || isString(value.scope));
}

function isBatchPayload<TValue>(value: TValue): value is TValue & { readonly results: readonly unknown[] } {
	return isRecord(value) && Array.isArray(value.results);
}

function isBatchResult<TValue>(value: TValue): value is TValue & { readonly status: number; readonly body?: unknown } {
	return (
		isRecord(value) && Number.isInteger(value.status) && Number(value.status) >= 200 && Number(value.status) <= 599
	);
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
	private readonly connectUrl: string;
	private readonly batchConnect: boolean;
	private readonly transportId: string;
	private readonly clientId: string;
	private readonly subscriptions = new Map<string, LogicalSubscription>();
	private queuedConnects: QueuedConnect[] = [];
	private registrationHolds = 0;
	private registrationHoldTimer: ReturnType<typeof setTimeout> | undefined;
	private readonly tabId: string;
	private started = false;
	private disposed = false;
	private isOwner = false;
	private streamAbort: AbortController | undefined;
	private streamAttempted: Promise<void> = Promise.resolve();
	private settleStreamAttempt: () => void = () => {};
	private reconnectAttempt = 0;
	private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
	private ownerHeartbeatTimer: ReturnType<typeof setInterval> | undefined;
	private broadcast: BroadcastChannel | undefined;
	private releaseOwnerLock: (() => void) | undefined;

	constructor(
		fetchImpl: typeof fetch,
		streamUrl: string,
		connectUrl: string,
		batchConnect: boolean,
		transportId: string,
		clientId: string,
		createId: (prefix: string) => string
	) {
		this.fetchImpl = fetchImpl;
		this.streamUrl = streamUrl;
		this.connectUrl = connectUrl;
		this.batchConnect = batchConnect;
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
			// A failed registration keeps its subscriber so the transport retries it; the store's abort signal removes it.
			if (ready.error.code === 'aborted') {
				options.signal.removeEventListener('abort', abort);
				this.unsubscribe(entry, options.onEnvelope, options.onReconnect);
			}
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
		this.settleStreamAttempt();
		clearTimeout(this.registrationHoldTimer);
		this.stopStream();
		for (const entry of this.subscriptions.values()) {
			entry.connectAbort?.abort();
			clearTimeout(entry.retryTimer);
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
			retryAttempt: 0,
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

		clearTimeout(entry.retryTimer);
		const promise = this.runLogicalSubscription(entry).then((result) => {
			if (entry.promise === promise && this.subscriptions.get(entry.key) === entry) {
				if (result.isOk()) {
					entry.retryAttempt = 0;
				} else {
					entry.promise = undefined;
					if (result.error.code !== 'aborted') {
						entry.retryTimer = setTimeout(
							() => this.ensureLogicalSubscription(entry),
							backoffDelay(entry.retryAttempt++, MAX_REGISTRATION_RETRY_MS)
						);
					}
				}
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
			// The server drops a transport's registrations when its stream is replaced or closed, so the owner registers
			// after its first stream attempt; opening the stream would otherwise abort and re-send every registration.
			await this.streamAttempted;
			if (signal.aborted) {
				return err('aborted', 'Realtime subscription was aborted.');
			}
			const url = urlWithCursor(entry.connectUrl, entry.getCursor?.());
			const path = this.batchConnect ? batchPath(url, this.connectUrl) : undefined;
			const response = path
				? await this.queueConnect(path, signal)
				: await this.fetchImpl(url, {
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
			const result = await readSyncResponse<{ readonly scope?: unknown }>(response);
			if (signal.aborted) {
				return err('aborted', 'Realtime subscription was aborted.');
			}
			if (result.isErr()) {
				return result;
			}
			if (isConnectScope(result.value.value) && result.value.value.scope !== undefined) {
				// The server owns scope encoding because manager.scope(params) can be narrower than frontend params.
				entry.scope = result.value.value.scope;
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

	// Stores hold registrations while they read, so the registrations that follow a burst of reads go out together.
	holdRegistrations(): () => void {
		this.registrationHolds += 1;
		let released = false;
		return () => {
			if (released) {
				return;
			}
			released = true;
			this.registrationHolds -= 1;
			if (this.registrationHolds === 0) {
				// A task later, so the registration a store queues right after its read joins this batch.
				setTimeout(() => this.flushConnects(), 0);
			}
		};
	}

	private queueConnect(url: string, signal: AbortSignal): Promise<Response> {
		return new Promise((resolve, reject) => {
			this.queuedConnects.push({ url, signal, resolve, reject });
			if (this.queuedConnects.length > 1) {
				return;
			}
			if (this.registrationHolds === 0) {
				queueMicrotask(() => this.flushConnects());
			} else {
				this.registrationHoldTimer = setTimeout(() => this.flushConnects(), MAX_REGISTRATION_HOLD_MS);
			}
		});
	}

	private flushConnects(): void {
		clearTimeout(this.registrationHoldTimer);
		const queued = this.queuedConnects;
		this.queuedConnects = [];
		const live: QueuedConnect[] = [];
		for (const item of queued) {
			if (item.signal.aborted) {
				item.reject(new DOMException('Realtime subscription was aborted.', 'AbortError'));
			} else {
				live.push(item);
			}
		}
		for (let start = 0; start < live.length; start += MAX_BATCH_CONNECTS) {
			this.sendConnects(live.slice(start, start + MAX_BATCH_CONNECTS));
		}
	}

	private async sendConnects(items: readonly QueuedConnect[]): Promise<void> {
		try {
			const response = await this.fetchImpl(this.connectUrl, {
				method: 'POST',
				headers: {
					'content-type': 'application/json',
					'x-sync-transport-id': this.transportId,
					'x-client-id': this.clientId
				},
				body: JSON.stringify({ connects: items.map((item) => item.url) }),
				signal: AbortSignal.timeout(LOGICAL_CONNECT_TIMEOUT_MS)
			});
			if (!response.ok) {
				const body = await response.text();
				for (const item of items) {
					item.resolve(new Response(body, { status: response.status }));
				}
				return;
			}
			const payload: unknown = await response.json();
			items.forEach((item, index) => {
				const result = isBatchPayload(payload) ? payload.results[index] : undefined;
				item.resolve(
					isBatchResult(result)
						? Response.json(result.body ?? null, { status: result.status })
						: Response.json(null)
				);
			});
		} catch (cause) {
			for (const item of items) {
				item.reject(cause);
			}
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
		if (entry.subscribers.size > 0 || this.subscriptions.get(entry.key) !== entry) {
			return;
		}
		this.subscriptions.delete(entry.key);
		clearTimeout(entry.retryTimer);
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
		this.reconnectAttempt = 0;
		this.stopStream();
	}

	private stopStream(): void {
		this.streamAbort?.abort();
		this.streamAbort = undefined;
		clearTimeout(this.reconnectTimer);
		this.reconnectTimer = undefined;
		clearInterval(this.ownerHeartbeatTimer);
		this.ownerHeartbeatTimer = undefined;
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
		this.streamAttempted = new Promise<void>((resolve) => {
			this.settleStreamAttempt = resolve;
		});
		this.openBroadcastChannel();
		if (!isBrowserRuntime()) {
			this.isOwner = true;
			this.connectOwnerStream();
			return;
		}
		const locks = navigator.locks;
		if (locks) {
			const ownStream = async () => {
				if (this.disposed || this.subscriptions.size === 0) {
					return;
				}
				this.isOwner = true;
				this.connectOwnerStream();
				await new Promise<void>((resolve) => {
					this.releaseOwnerLock = resolve;
				});
			};
			locks
				.request(LOCK_NAME, { ifAvailable: true }, (lock) => {
					if (lock) {
						return ownStream();
					}
					this.settleStreamAttempt();
					return locks.request(LOCK_NAME, ownStream);
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
		// Followers receive the owner's envelope JSON, a string copy that is cheaper than a structured clone of the object.
		this.broadcast.onmessage = (message) => {
			const data: unknown = message.data;
			if (isString(data)) {
				const envelope = parseSyncEnvelopeJson(data);
				if (envelope) {
					this.dispatchEnvelope(envelope);
				}
			} else if (isRecord(data) && data.type === 'reconnected') {
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
			this.settleStreamAttempt();
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

	private async connectOwnerStream(): Promise<void> {
		if (this.disposed || !this.isOwner) {
			return;
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
			this.settleStreamAttempt();
			this.broadcast?.postMessage({ type: 'reconnected', hydrate: wasReconnect });

			await readSseStream(
				response.body.getReader(),
				controller.signal,
				(envelope, json) => {
					this.dispatchEnvelope(envelope);
					this.broadcast?.postMessage(json);
				},
				() => controller.abort()
			);
		} catch {
			this.settleStreamAttempt();
		}
		if (!this.disposed && this.isOwner) {
			this.scheduleReconnect();
		}
	}

	private scheduleReconnect(): void {
		if (this.reconnectTimer || this.disposed || !this.isOwner) {
			return;
		}
		this.reconnectTimer = setTimeout(
			() => {
				this.reconnectTimer = undefined;
				this.connectOwnerStream();
			},
			backoffDelay(this.reconnectAttempt++, MAX_STREAM_RECONNECT_MS)
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

// A snapshot is otherwise deleted only when its own store reads it after expiry, so scopes never opened again stay
// stored. The marker expires when the next sweep is due, so tabs and reloads sweep at most once per interval.
async function sweepExpiredCache(cache: CacheAdapter, keys: () => Promise<readonly string[]>, now: number) {
	const marker = await cache.get(CACHE_SWEEP_KEY);
	if (marker && marker.expiry > now) {
		return;
	}
	for (const key of await keys()) {
		const item = await cache.get(key);
		if (item && item.expiry > 0 && item.expiry < now) {
			await cache.del(key);
		}
	}
	await cache.set(CACHE_SWEEP_KEY, { value: now, expiry: now + CACHE_SWEEP_INTERVAL_MS });
}

// A sync error body carries the server's error code; a response without one, such as a proxy failure, is classified by its status.
export async function readSyncResponse<TValue>(response: Response): Promise<SyncResult<SyncHttpOk<TValue>, SyncError>> {
	const payload: unknown = await response.json().catch(() => undefined);
	if (isSyncHttpResult<TValue>(payload)) {
		return payload.isError ? err(normalizeSyncError(payload.error)) : ok(payload);
	}
	return response.ok
		? err('validation', 'Sync response payload was malformed.', { details: payload })
		: err(syncErrorCodeFromHttpStatus(response.status), `Sync request failed: ${response.status}`);
}

export function createRuntime(options: CreateRuntimeOptions): SyncRuntime {
	let idIndex = 0;
	const now = options.now ?? (() => Date.now());
	const cache = options.cache;
	const keys = cache.keys?.bind(cache);
	const sweepTimer = keys
		? setTimeout(() => sweepExpiredCache(cache, keys, now()).catch(() => undefined), CACHE_SWEEP_DELAY_MS)
		: undefined;
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
			options.connectUrl ?? '/api/sync-resource/connect',
			options.batchConnect ?? true,
			transportId,
			clientId,
			createId
		);
	return {
		fetch: fetchImpl,
		cache,
		transport,
		clientId,
		now,
		createId,
		dispose() {
			clearTimeout(sweepTimer);
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

// The batch route dispatches each item on its own origin, so a store URL on another origin connects directly.
function batchPath(urlText: string, connectUrl: string): string | undefined {
	const base = new URL(connectUrl, globalThis.location?.href ?? 'http://sync.local');
	const url = new URL(urlText, base);
	return url.origin === base.origin ? url.pathname + url.search : undefined;
}

export function urlWithCursor(urlText: string, cursor: string | undefined): string {
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
		const parsed: unknown = JSON.parse(window.localStorage.getItem(OWNER_STORAGE_KEY) ?? 'null');
		return isOwnerRecord(parsed) ? parsed : undefined;
	} catch {
		return undefined;
	}
}

async function readSseStream(
	reader: ReadableStreamDefaultReader<Uint8Array>,
	signal: AbortSignal,
	onEnvelope: (envelope: SyncEnvelope, json: string) => void,
	onTerminalFailure: () => void
): Promise<void> {
	let buffer = '';
	let terminalFailure = false;
	const textDecoder = new TextDecoder('utf-8', { fatal: true });
	const pendingSyncChunks = new Map<string, PendingSyncEnvelopeChunks>();
	const abort = () => {
		reader.cancel().catch(() => undefined);
	};
	signal.addEventListener('abort', abort, { once: true });
	try {
		while (!signal.aborted) {
			const chunk = await reader.read();
			if (chunk.done) {
				return;
			}
			buffer += textDecoder.decode(chunk.value, { stream: true });
			let eventEnd = buffer.indexOf('\n\n');
			while (eventEnd >= 0) {
				const eventText = buffer.slice(0, eventEnd);
				buffer = buffer.slice(eventEnd + 2);
				if (exceedsFrameBound(eventText, 2)) {
					terminalFailure = true;
					return;
				}
				const json = parseSseJson(eventText, pendingSyncChunks);
				if (json !== undefined) {
					const envelope = parseSyncEnvelopeJson(json);
					if (envelope) {
						onEnvelope(envelope, json);
					}
				}
				eventEnd = buffer.indexOf('\n\n');
			}
			if (exceedsFrameBound(buffer, 0)) {
				// Bound malformed or stalled streams so a broken SSE response cannot grow memory without limit.
				terminalFailure = true;
				return;
			}
		}
	} catch {
		terminalFailure = !signal.aborted;
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

// UTF-8 takes at most 3 bytes per UTF-16 unit, so only a text above a third of the bound needs its exact byte length.
function exceedsFrameBound(text: string, extraBytes: number): boolean {
	return (
		text.length * 3 + extraBytes > MAX_SSE_FRAME_BYTES &&
		sseTextEncoder.encode(text).byteLength + extraBytes > MAX_SSE_FRAME_BYTES
	);
}

function parseSseJson(
	eventText: string,
	pendingSyncChunks: Map<string, PendingSyncEnvelopeChunks>
): string | undefined {
	const { eventName, data } = parseSseEvent(eventText);
	if (!data) {
		return undefined;
	}
	if (eventName === 'sync') {
		return data;
	}
	return eventName === 'sync-chunk'
		? acceptSyncEnvelopeChunk(parseSyncEnvelopeChunkJson(data), pendingSyncChunks)
		: undefined;
}

function acceptSyncEnvelopeChunk(
	chunk: SyncEnvelopeChunk | undefined,
	pendingSyncChunks: Map<string, PendingSyncEnvelopeChunks>
): string | undefined {
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

	return serialized;
}

function pruneOldestPendingSyncChunk(pendingSyncChunks: Map<string, PendingSyncEnvelopeChunks>): void {
	const first = pendingSyncChunks.keys().next();
	if (!first.done) {
		pendingSyncChunks.delete(first.value);
	}
}
