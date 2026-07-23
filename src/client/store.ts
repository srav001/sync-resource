import { isSyncError, isSyncHttpResult, normalizeSyncError, syncError } from '../shared/index.ts';
import { err, ok, type SyncResult } from '../shared/result.ts';
import { stableStringify } from '../shared/stableJson.ts';
import {
	ackPendingCommand,
	applyEnvelope as applyOptimisticEnvelope,
	applyPageOutput,
	captureOptimisticState,
	createOptimisticState,
	createPendingCommand,
	hasQueryFamily,
	itemsForQueryFamily,
	metaForQueryFamily,
	pendingSnapshot,
	pagesForQueryFamily,
	rebuildVisible,
	reconcileFromConfig,
	restoreOptimisticState,
	selectQueryFamily,
	setBaseData,
	updateVisibleItems,
	type CachedOptimisticState,
	type InternalPendingCommand,
	type PendingCommand
} from './optimistic.ts';
import { createRepairScheduler, noRepairNeeded, type RepairScheduler } from './repair.ts';
import { getSyncCacheOptions, getSyncRuntime } from './runtime.ts';
import type {
	CollectionMeta,
	CollectionItem,
	InputOf,
	ManagerTypeShape,
	NeedsReconcile,
	OutputOf,
	PageState,
	QueryOf,
	ReconcileBuilder,
	ReconcileConfig,
	StoreConfig,
	SyncEnvelope,
	SyncError,
	SyncHttpResult,
	SyncRuntime,
	SyncSignal
} from './types.ts';

type StoreSubscriber<TManager extends ManagerTypeShape> = (snapshot: StoreSnapshot<TManager>) => void;
type StoreEventCallback<TValue> = (value: TValue) => void;
type StoreSignalCallback<TPayload = unknown> = (payload: TPayload) => void;
interface ForcedHydrationState {
	requestedGeneration: number;
	runningGeneration: number;
	readStarted: boolean;
}

interface ForcedHydrationQueue {
	readonly state: ForcedHydrationState;
	readonly task: Promise<SyncResult<void, SyncError>>;
}

type StoreEventKey =
	| 'data'
	| 'items'
	| 'listMeta'
	| 'pages'
	| 'pending'
	| 'hydrating'
	| 'refreshing'
	| 'disposed'
	| 'error';
const CACHE_SAVE_DELAY_MS = 50;
const CACHE_SAVE_MAX_WAIT_MS = 250;

export interface StoreSnapshot<TManager extends ManagerTypeShape> {
	readonly data: OutputOf<TManager, 'get'> | undefined;
	readonly items: readonly CollectionItem<TManager>[];
	readonly listMeta: CollectionMeta<TManager> | undefined;
	readonly pages: readonly PageState[];
	readonly pending: readonly PendingCommand[];
	readonly hydrating: boolean;
	readonly refreshing: boolean;
	readonly disposed: boolean;
	readonly error: SyncError | null;
}

export interface StoreEventHandlers<TManager extends ManagerTypeShape> {
	data(callback: StoreEventCallback<StoreSnapshot<TManager>['data']>): () => void;
	items(callback: StoreEventCallback<StoreSnapshot<TManager>['items']>): () => void;
	listMeta(callback: StoreEventCallback<StoreSnapshot<TManager>['listMeta']>): () => void;
	pages(callback: StoreEventCallback<StoreSnapshot<TManager>['pages']>): () => void;
	signal<TPayload = unknown>(type: string, callback: StoreSignalCallback<TPayload>): () => void;
	pending(callback: StoreEventCallback<StoreSnapshot<TManager>['pending']>): () => void;
	hydrating(callback: StoreEventCallback<StoreSnapshot<TManager>['hydrating']>): () => void;
	refreshing(callback: StoreEventCallback<StoreSnapshot<TManager>['refreshing']>): () => void;
	disposed(callback: StoreEventCallback<StoreSnapshot<TManager>['disposed']>): () => void;
	error(callback: StoreEventCallback<StoreSnapshot<TManager>['error']>): () => void;
}

export interface StoreListHandle<TManager extends ManagerTypeShape> {
	items(): readonly CollectionItem<TManager>[];
	meta(): CollectionMeta<TManager> | undefined;
	pages(): readonly PageState[];
	refresh(): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>>;
	loadMore(): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>>;
}

export interface StoreBase<TManager extends ManagerTypeShape> {
	readonly on: StoreEventHandlers<TManager>;
	snapshot(): StoreSnapshot<TManager>;
	pending(): readonly PendingCommand[];
	error(): SyncError | null;
	isHydrating(): boolean;
	isRefreshing(): boolean;
	isPending(): boolean;
	restore(): Promise<SyncResult<void, SyncError>>;
	hydrate(): Promise<SyncResult<void, SyncError>>;
	connect(): Promise<SyncResult<void, SyncError>>;
	repair(): Promise<SyncResult<void, SyncError>>;
	subscribe(callback: StoreSubscriber<TManager>): () => void;
	unsubscribe(callback: StoreSubscriber<TManager>): void;
	dispose(): void;
}

export type StoreWithGet<TManager extends ManagerTypeShape> = 'get' extends keyof TManager['methods']
	? [QueryOf<TManager, 'get'>] extends [never]
		? {
				data(): OutputOf<TManager, 'get'> | undefined;
				get(): Promise<SyncResult<OutputOf<TManager, 'get'>, SyncError>>;
			}
		: {
				data(): OutputOf<TManager, 'get'> | undefined;
				get(args: {
					readonly query: QueryOf<TManager, 'get'>;
				}): Promise<SyncResult<OutputOf<TManager, 'get'>, SyncError>>;
			}
	: {};

export type StoreWithList<TManager extends ManagerTypeShape> = 'list' extends keyof TManager['methods']
	? [QueryOf<TManager, 'list'>] extends [never]
		? {
				list(): StoreListHandle<TManager>;
				items(): readonly CollectionItem<TManager>[];
				listMeta(): CollectionMeta<TManager> | undefined;
				pages(): readonly PageState[];
				refresh(): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>>;
				loadMore(): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>>;
			}
		: {
				list(query: QueryOf<TManager, 'list'>): StoreListHandle<TManager>;
				items(): readonly CollectionItem<TManager>[];
				listMeta(): CollectionMeta<TManager> | undefined;
				pages(): readonly PageState[];
				refresh(): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>>;
				loadMore(): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>>;
			}
	: {};

export type StoreWithAdd<TManager extends ManagerTypeShape> = 'add' extends keyof TManager['methods']
	? {
			add(
				args: { readonly input: InputOf<TManager, 'add'> },
				options?: StoreWriteOptions
			): Promise<SyncResult<OutputOf<TManager, 'add'>, SyncError>>;
			add(
				args: readonly { readonly input: InputOf<TManager, 'add'> }[],
				options?: StoreWriteOptions
			): Promise<SyncResult<unknown, SyncError>>;
		}
	: {};

type MutateArgs<TManager extends ManagerTypeShape> = [QueryOf<TManager, 'mutate'>] extends [never]
	? { readonly input: InputOf<TManager, 'mutate'> }
	: { readonly query: QueryOf<TManager, 'mutate'>; readonly input: InputOf<TManager, 'mutate'> };

export type StoreWithMutate<TManager extends ManagerTypeShape> = 'mutate' extends keyof TManager['methods']
	? {
			mutate(
				args: MutateArgs<TManager>,
				options?: StoreWriteOptions
			): Promise<SyncResult<OutputOf<TManager, 'mutate'>, SyncError>>;
			mutate(
				args: readonly MutateArgs<TManager>[],
				options?: StoreWriteOptions
			): Promise<SyncResult<unknown, SyncError>>;
		}
	: {};

type DeleteArgs<TManager extends ManagerTypeShape> = [QueryOf<TManager, 'delete'>] extends [never]
	? {}
	: { readonly query: QueryOf<TManager, 'delete'> };

export type StoreWithDelete<TManager extends ManagerTypeShape> = 'delete' extends keyof TManager['methods']
	? [QueryOf<TManager, 'delete'>] extends [never]
		? {
				delete(options?: StoreWriteOptions): Promise<SyncResult<OutputOf<TManager, 'delete'>, SyncError>>;
			}
		: {
				delete(
					args: DeleteArgs<TManager>,
					options?: StoreWriteOptions
				): Promise<SyncResult<OutputOf<TManager, 'delete'>, SyncError>>;
				delete(
					args: readonly DeleteArgs<TManager>[],
					options?: StoreWriteOptions
				): Promise<SyncResult<unknown, SyncError>>;
			}
	: {};

export interface StoreWriteOptions {
	readonly signal?: AbortSignal;
}

export type ClientStore<TManager extends ManagerTypeShape> = StoreBase<TManager> &
	StoreWithGet<TManager> &
	StoreWithList<TManager> &
	StoreWithAdd<TManager> &
	StoreWithMutate<TManager> &
	StoreWithDelete<TManager>;

export type StoreActionMethodName =
	| 'restore'
	| 'hydrate'
	| 'connect'
	| 'repair'
	| 'get'
	| 'refresh'
	| 'loadMore'
	| 'add'
	| 'mutate'
	| 'delete';

export type StoreActionMethods<TManager extends ManagerTypeShape> = Pick<
	ClientStore<TManager>,
	Extract<StoreActionMethodName, keyof ClientStore<TManager>>
>;

export function createStore<TManager extends ManagerTypeShape>(
	config: StoreConfig<TManager>,
	...reconcile: NeedsReconcile<TManager> extends true
		? [(builder: ReconcileBuilder<TManager>) => ReconcileConfig]
		: [(builder: ReconcileBuilder<TManager>) => ReconcileConfig] | []
): ClientStore<TManager> {
	const reconcileConfig = reconcile[0]?.({
		defaults(configValue) {
			return configValue ?? {};
		}
	});
	return new ClientStoreCore(config, reconcileConfig).api();
}

class ClientStoreCore<TManager extends ManagerTypeShape> {
	private readonly config: StoreConfig<TManager>;
	private readonly runtime: SyncRuntime;
	private readonly globalCache = getSyncCacheOptions();
	private readonly reconcile?: ReconcileConfig;
	private readonly state = createOptimisticState();
	private readonly pendingCommands = new Map<string, InternalPendingCommand>();
	private readonly repairScheduler: RepairScheduler = createRepairScheduler();
	private readonly subscribers = new Set<StoreSubscriber<TManager>>();
	private readonly dataSubscribers = new Set<StoreEventCallback<StoreSnapshot<TManager>['data']>>();
	private readonly itemSubscribers = new Set<StoreEventCallback<StoreSnapshot<TManager>['items']>>();
	private readonly listMetaSubscribers = new Set<StoreEventCallback<StoreSnapshot<TManager>['listMeta']>>();
	private readonly pageSubscribers = new Set<StoreEventCallback<StoreSnapshot<TManager>['pages']>>();
	private readonly signalSubscribers = new Map<string, Set<StoreSignalCallback>>();
	private readonly pendingSubscribers = new Set<StoreEventCallback<StoreSnapshot<TManager>['pending']>>();
	private readonly hydratingSubscribers = new Set<StoreEventCallback<StoreSnapshot<TManager>['hydrating']>>();
	private readonly refreshingSubscribers = new Set<StoreEventCallback<StoreSnapshot<TManager>['refreshing']>>();
	private readonly disposedSubscribers = new Set<StoreEventCallback<StoreSnapshot<TManager>['disposed']>>();
	private readonly errorSubscribers = new Set<StoreEventCallback<StoreSnapshot<TManager>['error']>>();
	private readonly abort = new AbortController();
	private readonly inflightReads = new Map<string, Promise<SyncResult<unknown, SyncError>>>();
	private readonly inflightHydrates = new Map<string, Promise<SyncResult<void, SyncError>>>();
	private readonly queuedForcedHydrates = new Map<string, ForcedHydrationQueue>();
	private readonly successfulHydrates = new Map<string, SyncResult<void, SyncError>>();
	private transportUnsubscribe: (() => void) | undefined;
	private connectPromise: Promise<SyncResult<void, SyncError>> | undefined;
	private visibleItemsCache: readonly CollectionItem<TManager>[] | undefined;
	private readonly listItemsCache = new Map<string, readonly CollectionItem<TManager>[]>();
	private cacheSaveTimeout: ReturnType<typeof setTimeout> | undefined;
	private cacheSaveMaxWaitTimeout: ReturnType<typeof setTimeout> | undefined;
	private cacheSaveInFlight = false;
	private cacheSaveRequested = false;
	private cachedScopeParamsText: string | undefined;
	private cachedScopeCacheKey: string | undefined;
	private cachedPendingStateCleared = false;
	private activeHydrationCount = 0;
	private hydrating = false;
	private refreshing = false;
	private disposed = false;
	private repairRequested = false;
	private lastError: SyncError | null = null;
	private activeQuery: unknown;
	private readonly hasDefaultListQuery: boolean;

	constructor(config: StoreConfig<TManager>, reconcile?: ReconcileConfig) {
		this.config = config;
		this.runtime = getSyncRuntime();
		this.reconcile = reconcile;
		this.hasDefaultListQuery = typeof config.query === 'function';
		this.activeQuery = config.query?.();
		this.bootstrapCacheRead();
	}

	api(): ClientStore<TManager> {
		const api: Record<string, unknown> = {
			on: {
				data: (callback: StoreEventCallback<StoreSnapshot<TManager>['data']>) =>
					this.subscribeEvent(this.dataSubscribers, callback),
				items: (callback: StoreEventCallback<StoreSnapshot<TManager>['items']>) =>
					this.subscribeEvent(this.itemSubscribers, callback),
				listMeta: (callback: StoreEventCallback<StoreSnapshot<TManager>['listMeta']>) =>
					this.subscribeEvent(this.listMetaSubscribers, callback),
				pages: (callback: StoreEventCallback<StoreSnapshot<TManager>['pages']>) =>
					this.subscribeEvent(this.pageSubscribers, callback),
				signal: <TPayload>(type: string, callback: StoreSignalCallback<TPayload>) =>
					this.subscribeSignal(type, callback as StoreSignalCallback),
				pending: (callback: StoreEventCallback<StoreSnapshot<TManager>['pending']>) =>
					this.subscribeEvent(this.pendingSubscribers, callback),
				hydrating: (callback: StoreEventCallback<StoreSnapshot<TManager>['hydrating']>) =>
					this.subscribeEvent(this.hydratingSubscribers, callback),
				refreshing: (callback: StoreEventCallback<StoreSnapshot<TManager>['refreshing']>) =>
					this.subscribeEvent(this.refreshingSubscribers, callback),
				disposed: (callback: StoreEventCallback<StoreSnapshot<TManager>['disposed']>) =>
					this.subscribeEvent(this.disposedSubscribers, callback),
				error: (callback: StoreEventCallback<StoreSnapshot<TManager>['error']>) =>
					this.subscribeEvent(this.errorSubscribers, callback)
			},
			snapshot: () => this.snapshot(),
			pending: () => this.pendingValue(),
			error: () => this.lastError,
			isHydrating: () => this.hydrating,
			isRefreshing: () => this.refreshing,
			isPending: () => this.pendingCommands.size > 0,
			restore: () => this.restore(),
			hydrate: () => this.hydrate(),
			connect: () => this.connect(),
			repair: () => this.repair(),
			subscribe: (callback: StoreSubscriber<TManager>) => this.subscribe(callback),
			unsubscribe: (callback: StoreSubscriber<TManager>) => this.unsubscribe(callback),
			dispose: () => this.dispose()
		};

		api.data = () => this.state.data;
		api.list = (query: unknown) => this.list(query);
		api.items = () => this.visibleItems();
		api.listMeta = () => this.state.listMeta as CollectionMeta<TManager> | undefined;
		api.pages = () => this.state.pages;
		api.get = (args?: { readonly query?: unknown }) => this.get(args?.query);
		api.refresh = () => this.refresh();
		api.loadMore = () => this.loadMore();
		api.add = (args: unknown, options?: StoreWriteOptions) => this.write('add', args, options);
		api.mutate = (args: unknown, options?: StoreWriteOptions) => this.write('mutate', args, options);
		api.delete = (argsOrOptions?: unknown, options?: StoreWriteOptions) => {
			if (options === undefined && isStoreWriteOptions(argsOrOptions)) {
				return this.write('delete', undefined, argsOrOptions);
			}
			return this.write('delete', argsOrOptions, options);
		};

		return api as ClientStore<TManager>;
	}

	snapshot(): StoreSnapshot<TManager> {
		return {
			data: this.state.data as OutputOf<TManager, 'get'> | undefined,
			items: this.visibleItems(),
			listMeta: this.state.listMeta as CollectionMeta<TManager> | undefined,
			pages: this.state.pages,
			pending: this.pendingValue(),
			hydrating: this.hydrating,
			refreshing: this.refreshing,
			disposed: this.disposed,
			error: this.lastError
		};
	}

	async restore(): Promise<SyncResult<void, SyncError>> {
		try {
			const cached = await this.loadCachedState();
			await this.clearCachedPendingStateOnce();
			if (!cached) {
				return ok(undefined);
			}
			if (this.pendingCommands.size > 0) {
				return ok(undefined);
			}

			this.applyCachedState(cached);
			this.emit('data', 'items', 'listMeta', 'pages');
			return ok(undefined);
		} catch (cause) {
			return err(this.recordError(toStoreSyncError(cause)));
		}
	}

	hydrate(): Promise<SyncResult<void, SyncError>> {
		return this.runHydrate(false);
	}

	private runHydrate(force: boolean): Promise<SyncResult<void, SyncError>> {
		if (this.disposed) {
			return Promise.resolve(err('disposed', 'Store is disposed.'));
		}

		const key = this.cacheKey();
		if (force) {
			return this.queueForcedHydrate(key);
		}
		const queuedForced = this.queuedForcedHydrates.get(key);
		if (queuedForced) {
			return queuedForced.task;
		}
		const existing = this.inflightHydrates.get(key);
		if (existing) {
			return existing;
		}
		const successful = this.successfulHydrates.get(key);
		if (successful) {
			return Promise.resolve(successful);
		}

		return this.startHydrate(key);
	}

	private queueForcedHydrate(key: string): Promise<SyncResult<void, SyncError>> {
		this.successfulHydrates.delete(key);
		const queued = this.queuedForcedHydrates.get(key);
		if (queued) {
			if (queued.state.readStarted) {
				queued.state.requestedGeneration = queued.state.runningGeneration + 1;
			}
			return queued.task;
		}

		const olderHydrate = this.inflightHydrates.get(key);
		const state: ForcedHydrationState = {
			requestedGeneration: 1,
			runningGeneration: 0,
			readStarted: false
		};
		this.beginHydration();
		const task = this.runForcedHydrates(key, state, olderHydrate).finally(() => {
			if (this.queuedForcedHydrates.get(key)?.state === state) {
				this.queuedForcedHydrates.delete(key);
			}
			this.endHydration();
		});
		this.queuedForcedHydrates.set(key, { state, task });
		return task;
	}

	private async runForcedHydrates(
		key: string,
		state: ForcedHydrationState,
		olderHydrate?: Promise<SyncResult<void, SyncError>>
	): Promise<SyncResult<void, SyncError>> {
		if (olderHydrate) {
			await olderHydrate;
		}
		if (this.disposed) {
			return err('disposed', 'Store is disposed.');
		}

		while (true) {
			state.runningGeneration += 1;
			state.readStarted = false;
			this.successfulHydrates.delete(key);
			const result = await this.startHydrate(key, () => {
				state.readStarted = true;
			});
			if (this.disposed || state.runningGeneration === state.requestedGeneration) {
				if (this.queuedForcedHydrates.get(key)?.state === state) {
					this.queuedForcedHydrates.delete(key);
				}
				return result;
			}
		}
	}

	private startHydrate(key: string, onAuthoritativeReadStart?: () => void): Promise<SyncResult<void, SyncError>> {
		this.clearError();
		const task = this.hydrateOnce(onAuthoritativeReadStart)
			.catch((cause) => err(toStoreSyncError(cause)))
			.then((result) => {
				if (this.disposed) {
					return err('disposed', 'Store is disposed.');
				}
				if (result.isErr()) {
					if (this.lastError !== result.error) {
						this.recordError(result.error);
					}
					return result;
				}
				this.successfulHydrates.set(key, result);
				return result;
			})
			.finally(() => {
				if (this.inflightHydrates.get(key) === task) {
					this.inflightHydrates.delete(key);
				}
			});
		this.inflightHydrates.set(key, task);
		return task;
	}

	async connect(): Promise<SyncResult<void, SyncError>> {
		try {
			return await this.ensureRealtime();
		} catch (cause) {
			return err(this.recordError(toStoreSyncError(cause)));
		}
	}

	subscribe(callback: StoreSubscriber<TManager>): () => void {
		this.subscribers.add(callback);
		callback(this.snapshot());
		return () => this.unsubscribe(callback);
	}

	unsubscribe(callback: StoreSubscriber<TManager>): void {
		this.subscribers.delete(callback);
	}

	dispose(): void {
		if (this.disposed) {
			return;
		}
		this.flushScheduledCachedRuntimeStateSave();
		this.disposed = true;
		this.activeHydrationCount = 0;
		this.hydrating = false;
		this.refreshing = false;
		this.inflightHydrates.clear();
		this.queuedForcedHydrates.clear();
		this.successfulHydrates.clear();
		this.abort.abort();
		this.transportUnsubscribe?.();
		this.repairScheduler.dispose();
		this.pendingCommands.clear();
		this.emit('disposed', 'pending', 'hydrating', 'refreshing');
		this.subscribers.clear();
		this.clearEventSubscribers();
	}

	private async get(query?: unknown): Promise<SyncResult<OutputOf<TManager, 'get'>, SyncError>> {
		return this.singleflightRead<OutputOf<TManager, 'get'>>(
			`${this.cacheKey()}:get:${stableStringify(query)}`,
			async () => {
				const result = await this.request<OutputOf<TManager, 'get'>>('get', 'GET', undefined, query);
				if (result.isErr()) {
					return result;
				}
				setBaseData(this.state, result.value.value, this.reconcileIdentity(), this.pendingCommands.values());
				if (result.value.envelope) {
					this.state.cursor = result.value.envelope.cursor;
				}
				this.emit('data');
				this.scheduleCachedRuntimeStateSave();
				return ok(result.value.value);
			}
		);
	}

	private async refresh(query?: unknown): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>> {
		const nextQuery = query ?? this.config.query?.();
		this.activeQuery = nextQuery;
		if (hasQueryFamily(this.state, nextQuery)) {
			selectQueryFamily(this.state, nextQuery, this.reconcileIdentity(), this.pendingCommands.values());
			this.emit('items', 'listMeta', 'pages');
		}
		return this.requestPage(nextQuery, nextQuery, true);
	}

	private async requestPage(
		viewQuery: unknown,
		requestQuery: unknown,
		activate: boolean
	): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>> {
		if (activate) {
			this.activeQuery = viewQuery;
		}
		return this.singleflightRead<OutputOf<TManager, 'list'>>(
			`${this.scopeCacheKey()}:page:${stableStringify(requestQuery)}`,
			async () => {
				const result = await this.request<OutputOf<TManager, 'list'>>('list', 'GET', undefined, requestQuery);
				if (result.isErr()) {
					return result;
				}
				if (activate && stableStringify(this.activeQuery) !== stableStringify(viewQuery)) {
					return ok(result.value.value);
				}
				applyPageOutput(
					this.state,
					result.value.value,
					requestQuery,
					this.reconcileIdentity(),
					this.pendingCommands.values(),
					activate
				);
				if (result.value.envelope) {
					this.state.cursor = result.value.envelope.cursor;
				}
				this.emit('items', 'listMeta', 'pages');
				this.scheduleCachedRuntimeStateSave();
				return ok(result.value.value);
			}
		);
	}

	private async loadMore(query?: unknown): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>> {
		const viewQuery = query ?? this.activeQuery ?? this.config.query?.();
		const activate = query === undefined;
		if (activate) {
			this.activeQuery = viewQuery;
			selectQueryFamily(this.state, viewQuery, this.reconcileIdentity(), this.pendingCommands.values());
		}
		const pages = activate ? this.state.pages : pagesForQueryFamily(this.state, viewQuery);
		const page = pages[pages.length - 1];
		const requestQuery = mergeCursor(viewQuery, page?.cursorOut);
		return this.requestPage(viewQuery, requestQuery, activate);
	}

	private bootstrapCacheRead(): void {
		// Construction must stay network-free; cache bootstrap gives instant UI without triggering server RU.
		this.restore();
	}

	private async hydrateOnce(onAuthoritativeReadStart?: () => void): Promise<SyncResult<void, SyncError>> {
		this.beginHydration();
		try {
			const restoreResult = await this.restore();
			if (restoreResult.isErr()) {
				return restoreResult;
			}
			if (onAuthoritativeReadStart) {
				let olderReads = this.inflightScopeReads();
				while (olderReads.length > 0) {
					await Promise.all(olderReads);
					if (this.disposed) {
						return err('disposed', 'Store is disposed.');
					}
					olderReads = this.inflightScopeReads();
				}
				onAuthoritativeReadStart();
			}
			const result =
				this.hasDefaultListQuery || this.activeQuery !== undefined
					? await this.refresh(this.activeQuery)
					: await this.get();
			if (result.isErr()) {
				return result;
			}
			return await this.ensureRealtime();
		} finally {
			this.endHydration();
		}
	}

	private async singleflightRead<TValue>(
		key: string,
		run: () => Promise<SyncResult<TValue, SyncError>>
	): Promise<SyncResult<TValue, SyncError>> {
		const existing = this.inflightReads.get(key);
		if (existing) {
			return existing as Promise<SyncResult<TValue, SyncError>>;
		}

		const task = run()
			.catch((cause) => err(this.recordError(toStoreSyncError(cause))))
			.finally(() => {
				if (this.inflightReads.get(key) === task) {
					this.inflightReads.delete(key);
				}
			});
		this.inflightReads.set(key, task as Promise<SyncResult<unknown, SyncError>>);
		return task;
	}

	private inflightScopeReads(): Promise<SyncResult<unknown, SyncError>>[] {
		const reads: Promise<SyncResult<unknown, SyncError>>[] = [];
		const scopePrefix = `${this.scopeCacheKey()}:`;
		for (const [key, read] of this.inflightReads) {
			if (key.startsWith(scopePrefix)) {
				reads.push(read);
			}
		}
		return reads;
	}

	private async repair(): Promise<SyncResult<void, SyncError>> {
		try {
			if (this.disposed) {
				return err('disposed', 'Store is disposed.');
			}

			const plan = this.repairRequested
				? {
						kind: 'scope' as const,
						stalePageCount: this.state.pages.length
					}
				: this.repairScheduler.plan(this.state.pages);
			if (plan.kind === 'none') {
				return noRepairNeeded();
			}

			return this.repairScheduler.request(async () => {
				try {
					this.refreshing = true;
					this.emit('refreshing');
					const result =
						this.state.pages.length > 0 || this.activeQuery !== undefined || this.hasDefaultListQuery
							? await this.refresh(this.activeQuery)
							: await this.get();
					this.refreshing = false;
					this.emit('refreshing');
					if (result.isErr()) {
						this.lastError = result.error;
						this.emit('error');
						return err(result.error);
					}
					this.repairRequested = false;
					return ok(undefined);
				} catch (cause) {
					this.refreshing = false;
					this.emit('refreshing');
					return err(this.recordError(toStoreSyncError(cause)));
				}
			});
		} catch (cause) {
			return err(this.recordError(toStoreSyncError(cause)));
		}
	}

	private async write(
		method: 'add' | 'mutate' | 'delete',
		args: unknown,
		options?: StoreWriteOptions
	): Promise<SyncResult<unknown, SyncError>> {
		try {
			if (this.disposed) {
				return err('disposed', 'Store is disposed.');
			}

			this.clearError();
			const mutationId = this.runtime.createId('mutation');
			const command = createPendingCommand(method, args, mutationId, this.reconcileIdentity());
			this.pendingCommands.set(mutationId, command);
			this.applyPendingWrite(method, command);
			this.emit(...this.writeOptimisticEvents(method));

			const result = await this.request<unknown>(method, 'POST', args, undefined, mutationId, options?.signal);
			if (result.isErr()) {
				this.pendingCommands.delete(mutationId);
				return err(await this.rollback(result.error));
			}

			this.pendingCommands.set(mutationId, ackPendingCommand(command));

			const isBatch = isBatchOutput(result.value.value);
			let rebuiltVisible = isBatch;
			if (isBatch) {
				// Batched writes still finalize through envelopes; rebuild from base plus pending instead of restoring a full snapshot.
				rebuildVisible(this.state, this.reconcileIdentity(), this.pendingCommands.values());
			}

			const envelope = result.value.envelope
				? this.writeResponseEnvelope(result.value.envelope, mutationId)
				: undefined;
			if (envelope) {
				this.applyStateEnvelope(envelope);
			} else {
				this.pendingCommands.delete(mutationId);
				rebuildVisible(this.state, this.reconcileIdentity(), this.pendingCommands.values());
				rebuiltVisible = true;
			}
			const events = envelope
				? this.eventsForEnvelope(envelope)
				: this.writeSettledEvents(method, rebuiltVisible);
			this.emit(...events);
			this.scheduleCachedRuntimeStateSave();
			return ok(result.value.value);
		} catch (cause) {
			return err(this.recordError(toStoreSyncError(cause)));
		}
	}

	private async request<TValue>(
		method: string,
		httpMethod: 'GET' | 'POST',
		body?: unknown,
		query?: unknown,
		mutationId?: string,
		signal?: AbortSignal
	): Promise<SyncResult<SyncHttpResult<TValue> & { readonly isOk: true }, SyncError>> {
		const params = this.config.getParams();
		const url = this.methodUrl(this.config.getUrl(params), method, query);
		const requestAbort = combineAbortSignals(this.abort.signal, signal);
		try {
			const response = await this.runtime.fetch(url, {
				method: httpMethod,
				body: body === undefined ? undefined : JSON.stringify(body),
				headers: {
					'content-type': 'application/json',
					'x-client-id': this.runtime.clientId,
					...(mutationId ? { 'x-mutation-id': mutationId } : {})
				},
				signal: requestAbort.signal
			});
			const payload = await response.json();
			if (!isSyncHttpResult<TValue>(payload)) {
				return err('validation', 'Sync response payload was malformed.', { details: payload });
			}
			if (payload.isError) {
				return err(normalizeSyncError(payload.error));
			}
			return ok(payload);
		} catch (cause) {
			if (requestAbort.signal.aborted) {
				return err('aborted', 'Sync request was aborted.');
			}
			return err('internal', cause instanceof Error ? cause.message : String(cause), { cause });
		} finally {
			requestAbort.dispose();
		}
	}

	private methodUrl(baseUrl: string, method: string, query?: unknown): string {
		const isAbsolute = /^https?:\/\//.test(baseUrl);
		const url = new URL(`${baseUrl.replace(/\/$/, '')}/${method}`, 'http://sync.local');
		if (query !== undefined) {
			url.searchParams.set('query', JSON.stringify(query));
		}
		return isAbsolute ? url.href : url.pathname + url.search;
	}

	private async ensureRealtime(): Promise<SyncResult<void, SyncError>> {
		if (this.disposed) {
			return err('disposed', 'Store is disposed.');
		}
		if (this.transportUnsubscribe) {
			return ok(undefined);
		}
		if (this.connectPromise) {
			return this.connectPromise;
		}
		this.connectPromise = this.connectRealtime().finally(() => {
			this.connectPromise = undefined;
		});
		return this.connectPromise;
	}

	private async connectRealtime(): Promise<SyncResult<void, SyncError>> {
		const params = this.config.getParams();
		const baseUrl = this.config.getUrl(params).replace(/\/$/, '');
		const controller = this.abort;
		const result = await this.runtime.transport.subscribe({
			url: this.realtimeUrl(`${baseUrl}/events`),
			connectUrl: this.realtimeUrl(`${baseUrl}/connect`),
			managerKey: this.config.key,
			scope: this.scopeCacheKey(),
			signal: controller.signal,
			getCursor: () => this.state.cursor,
			onReconnect: () => {
				// Reconnect is a correctness event, not just a transport event: every logical subscription must catch up.
				this.runHydrate(true);
			},
			onEnvelope: (envelope) => {
				if (envelope.changes.length > 0) {
					this.applyEnvelope(envelope);
					this.scheduleCachedRuntimeStateSave();
					this.emit(...this.eventsForEnvelope(envelope));
				}
				this.emitSignals(envelope);
			}
		});
		if (this.disposed) {
			if (result.isOk()) {
				result.value();
			}
			return err('disposed', 'Store is disposed.');
		}
		if (result.isOk()) {
			this.transportUnsubscribe = result.value;
			return ok(undefined);
		}
		this.lastError = result.error;
		this.emit('error');
		return result;
	}

	private realtimeUrl(eventsUrl: string): string {
		if (!this.state.cursor) {
			return eventsUrl;
		}
		const isAbsolute = /^https?:\/\//.test(eventsUrl);
		const url = new URL(eventsUrl, 'http://sync.local');
		url.searchParams.set('after', this.state.cursor);
		return isAbsolute ? url.href : url.pathname + url.search;
	}

	private applyEnvelope(envelope: SyncEnvelope): void {
		applyOptimisticEnvelope(
			this.state,
			this.localPendingEnvelope(envelope),
			this.reconcileIdentity(),
			this.pendingCommands,
			this.activeQuery
		);
		const plan = this.repairScheduler.plan(this.state.pages, envelope.reset);
		if (plan.kind !== 'none') {
			this.repairRequested = true;
			this.scheduleRepair();
		}
	}

	private applyStateEnvelope(envelope: SyncEnvelope): void {
		if (envelope.changes.length > 0) {
			this.applyEnvelope(envelope);
			return;
		}
		if (envelope.sourceMutationId) {
			this.pendingCommands.delete(envelope.sourceMutationId);
		}
	}

	private applyPendingWrite(method: 'add' | 'mutate' | 'delete', command: InternalPendingCommand): void {
		if (method === 'mutate' && command.targetIds.length > 0) {
			updateVisibleItems(
				this.state,
				new Set(command.targetIds),
				this.reconcileIdentity(),
				this.pendingCommands.values()
			);
			return;
		}

		rebuildVisible(this.state, this.reconcileIdentity(), this.pendingCommands.values());
	}

	private localPendingEnvelope(envelope: SyncEnvelope): SyncEnvelope {
		if (
			!envelope.sourceMutationId ||
			envelope.sourceClientId === undefined ||
			envelope.sourceClientId === this.runtime.clientId
		) {
			return envelope;
		}
		// Mutation id is the finality key, but sourceClientId prevents another client with a colliding custom id from clearing local pending.
		return {
			...envelope,
			sourceMutationId: undefined
		};
	}

	private writeResponseEnvelope(envelope: SyncEnvelope, mutationId: string): SyncEnvelope {
		const sourceMutationId = envelope.sourceMutationId ?? mutationId;
		return {
			...envelope,
			sourceMutationId,
			sourceClientId: sourceMutationId === mutationId ? this.runtime.clientId : envelope.sourceClientId
		};
	}

	private reconcileIdentity() {
		return reconcileFromConfig(this.reconcile, () => this.config.getParams());
	}

	private captureState(): CachedOptimisticState {
		return captureOptimisticState(this.state);
	}

	private async rollback(originalError: SyncError): Promise<SyncError> {
		rebuildVisible(this.state, this.reconcileIdentity(), this.pendingCommands.values());
		const recoveredError = this.withRecovery(originalError, 'memory');
		this.lastError = recoveredError;
		this.emit('data', 'items', 'pages', 'pending', 'error');
		this.scheduleCachedRuntimeStateSave();
		return recoveredError;
	}

	private scheduleRepair(): void {
		this.repair();
	}

	private applyCachedState(snapshot: CachedOptimisticState): void {
		restoreOptimisticState(this.state, snapshot, this.reconcileIdentity());
	}

	private async loadCachedState(): Promise<CachedOptimisticState | undefined> {
		const item = await this.cache().get<CachedOptimisticState>(this.cacheKey());
		if (!item) {
			return undefined;
		}
		if (item.expiry > 0 && item.expiry < this.runtime.now()) {
			await this.cache().del(this.cacheKey());
			return undefined;
		}
		return item.value;
	}

	private async clearCachedPendingStateOnce(): Promise<void> {
		if (this.cachedPendingStateCleared) {
			return;
		}
		await this.cache().del(this.pendingCacheKey());
		this.cachedPendingStateCleared = true;
	}

	private async saveCachedRuntimeState(): Promise<void> {
		await this.saveCachedState();
	}

	private scheduleCachedRuntimeStateSave(): void {
		if (this.disposed) {
			return;
		}
		this.cacheSaveRequested = true;
		if (this.cacheSaveInFlight) {
			return;
		}
		this.armCacheSaveDelayTimer();
		if (!this.cacheSaveMaxWaitTimeout) {
			this.armCacheSaveMaxWaitTimer();
		}
	}

	private armCacheSaveDelayTimer(): void {
		if (this.cacheSaveTimeout) {
			clearTimeout(this.cacheSaveTimeout);
		}
		this.cacheSaveTimeout = setTimeout(() => {
			this.runScheduledCachedRuntimeStateSave().catch((cause) => this.recordError(toStoreSyncError(cause)));
		}, CACHE_SAVE_DELAY_MS);
	}

	private armCacheSaveMaxWaitTimer(): void {
		this.cacheSaveMaxWaitTimeout = setTimeout(() => {
			this.runScheduledCachedRuntimeStateSave().catch((cause) => this.recordError(toStoreSyncError(cause)));
		}, CACHE_SAVE_MAX_WAIT_MS);
	}

	private async runScheduledCachedRuntimeStateSave(): Promise<void> {
		this.clearCacheSaveTimers();
		if (this.disposed || this.cacheSaveInFlight || !this.cacheSaveRequested) {
			return;
		}
		this.cacheSaveRequested = false;
		this.cacheSaveInFlight = true;
		try {
			await this.saveCachedRuntimeState();
		} finally {
			this.cacheSaveInFlight = false;
			if (this.cacheSaveRequested && !this.disposed) {
				this.scheduleCachedRuntimeStateSave();
			}
		}
	}

	private flushScheduledCachedRuntimeStateSave(): void {
		this.cacheSaveRequested = true;
		this.clearCacheSaveTimers();
		this.runScheduledCachedRuntimeStateSave().catch((cause) => this.recordError(toStoreSyncError(cause)));
	}

	private clearCacheSaveTimers(): void {
		if (this.cacheSaveTimeout) {
			clearTimeout(this.cacheSaveTimeout);
			this.cacheSaveTimeout = undefined;
		}
		if (this.cacheSaveMaxWaitTimeout) {
			clearTimeout(this.cacheSaveMaxWaitTimeout);
			this.cacheSaveMaxWaitTimeout = undefined;
		}
	}

	private async saveCachedState(): Promise<void> {
		const ttlMs = this.cacheTtlMs();
		await this.cache().set(this.cacheKey(), {
			value: this.captureState(),
			expiry: this.runtime.now() + ttlMs
		});
	}

	private cacheKey(): string {
		return `${this.scopeCacheKey()}:view`;
	}

	private pendingCacheKey(): string {
		return `${this.scopeCacheKey()}:pending`;
	}

	private scopeCacheKey(): string {
		const paramsText = stableStringify(this.config.getParams());
		if (this.cachedScopeParamsText === paramsText && this.cachedScopeCacheKey) {
			return this.cachedScopeCacheKey;
		}
		this.cachedScopeParamsText = paramsText;
		this.cachedScopeCacheKey = `live-resource:${this.config.key}:${paramsText}`;
		return this.cachedScopeCacheKey;
	}

	private withRecovery(syncErrorValue: SyncError, source: 'memory' | 'idb'): SyncError {
		return syncError(syncErrorValue.code, syncErrorValue.message, {
			details: syncErrorValue.details,
			cause: syncErrorValue.cause,
			recovery: {
				restored: true,
				source
			}
		});
	}

	private recordError(errorValue: SyncError): SyncError {
		this.lastError = errorValue;
		this.emit('error');
		return errorValue;
	}

	private clearError(): void {
		if (!this.lastError) {
			return;
		}
		this.lastError = null;
		this.emit('error');
	}

	private beginHydration(): void {
		this.activeHydrationCount += 1;
		if (this.hydrating) {
			return;
		}
		this.hydrating = true;
		this.emit('hydrating');
	}

	private endHydration(): void {
		if (this.disposed) {
			return;
		}
		this.activeHydrationCount -= 1;
		if (this.activeHydrationCount > 0) {
			return;
		}
		this.hydrating = false;
		this.emit('hydrating');
	}

	private writeOptimisticEvents(method: 'add' | 'mutate' | 'delete'): readonly StoreEventKey[] {
		if (method === 'add') {
			return ['items', 'pending'];
		}
		return ['data', 'items', 'pending'];
	}

	private writeSettledEvents(method: 'add' | 'mutate' | 'delete', rebuiltVisible: boolean): readonly StoreEventKey[] {
		if (!rebuiltVisible) {
			return ['pending'];
		}
		return this.writeOptimisticEvents(method);
	}

	private eventsForEnvelope(envelope: SyncEnvelope): readonly StoreEventKey[] {
		const events: StoreEventKey[] = [];
		for (const syncChange of envelope.changes) {
			if (syncChange.type === 'pageLoaded') {
				pushEvent(events, 'items');
				pushEvent(events, 'listMeta');
				pushEvent(events, 'pages');
				continue;
			}
			if (syncChange.type === 'itemAdded') {
				pushEvent(events, 'items');
				pushEvent(events, 'pages');
				continue;
			}
			if (syncChange.type === 'itemUpdated') {
				pushEvent(events, 'data');
				pushEvent(events, 'items');
				continue;
			}
			if (syncChange.type === 'itemDeleted') {
				pushEvent(events, 'data');
				pushEvent(events, 'items');
				continue;
			}
			pushEvent(events, 'data');
			pushEvent(events, 'items');
			pushEvent(events, 'pages');
			pushEvent(events, 'pending');
		}
		if (envelope.sourceMutationId) {
			pushEvent(events, 'pending');
		}
		return events;
	}

	private emit(...events: readonly StoreEventKey[]): void {
		if (events.length === 0) {
			return;
		}
		if (events.includes('items')) {
			this.visibleItemsCache = undefined;
			this.listItemsCache.clear();
		}
		const snapshot = this.subscribers.size > 0 ? this.snapshot() : undefined;
		for (const subscriber of this.subscribers) {
			subscriber(snapshot ?? this.snapshot());
		}
		for (const event of events) {
			this.emitEvent(event);
		}
	}

	private emitEvent(event: StoreEventKey): void {
		if (event === 'data') {
			this.emitTo(this.dataSubscribers, this.state.data as StoreSnapshot<TManager>['data']);
			return;
		}
		if (event === 'items') {
			this.emitTo(this.itemSubscribers, this.visibleItems());
			return;
		}
		if (event === 'listMeta') {
			this.emitTo(this.listMetaSubscribers, this.state.listMeta as StoreSnapshot<TManager>['listMeta']);
			return;
		}
		if (event === 'pages') {
			this.emitTo(this.pageSubscribers, this.state.pages);
			return;
		}
		if (event === 'pending') {
			this.emitTo(this.pendingSubscribers, this.pendingValue());
			return;
		}
		if (event === 'hydrating') {
			this.emitTo(this.hydratingSubscribers, this.hydrating);
			return;
		}
		if (event === 'refreshing') {
			this.emitTo(this.refreshingSubscribers, this.refreshing);
			return;
		}
		if (event === 'disposed') {
			this.emitTo(this.disposedSubscribers, this.disposed);
			return;
		}
		this.emitTo(this.errorSubscribers, this.lastError);
	}

	private emitTo<TValue>(subscribers: Set<StoreEventCallback<TValue>>, value: TValue): void {
		for (const subscriber of subscribers) {
			subscriber(value);
		}
	}

	private subscribeEvent<TValue>(
		subscribers: Set<StoreEventCallback<TValue>>,
		callback: StoreEventCallback<TValue>
	): () => void {
		subscribers.add(callback);
		return () => {
			subscribers.delete(callback);
		};
	}

	private subscribeSignal<TPayload>(type: string, callback: StoreSignalCallback<TPayload>): () => void {
		const subscribers = this.signalSubscribers.get(type) ?? new Set<StoreSignalCallback>();
		subscribers.add(callback as StoreSignalCallback);
		this.signalSubscribers.set(type, subscribers);
		return () => {
			subscribers.delete(callback as StoreSignalCallback);
			if (subscribers.size === 0) {
				this.signalSubscribers.delete(type);
			}
		};
	}

	private emitSignals(envelope: SyncEnvelope): void {
		for (const nextSignal of envelope.signals ?? []) {
			this.emitSignal(nextSignal);
		}
	}

	private emitSignal(nextSignal: SyncSignal): void {
		const subscribers = this.signalSubscribers.get(nextSignal.type);
		if (!subscribers) {
			return;
		}
		for (const subscriber of subscribers) {
			subscriber(nextSignal.payload);
		}
	}

	private clearEventSubscribers(): void {
		this.dataSubscribers.clear();
		this.itemSubscribers.clear();
		this.listMetaSubscribers.clear();
		this.pageSubscribers.clear();
		this.signalSubscribers.clear();
		this.pendingSubscribers.clear();
		this.hydratingSubscribers.clear();
		this.refreshingSubscribers.clear();
		this.disposedSubscribers.clear();
		this.errorSubscribers.clear();
	}

	private pendingValue(): readonly PendingCommand[] {
		return [...this.pendingCommands.values()].map(pendingSnapshot);
	}

	private visibleItems(): readonly CollectionItem<TManager>[] {
		if (!this.visibleItemsCache) {
			this.visibleItemsCache = [...this.state.items.values()] as CollectionItem<TManager>[];
		}
		return this.visibleItemsCache;
	}

	private list(query: unknown): StoreListHandle<TManager> {
		return {
			items: () => this.itemsForList(query),
			meta: () => this.metaForList(query),
			pages: () => this.pagesForList(query),
			refresh: () => this.refreshList(query),
			loadMore: () => this.loadMore(query)
		};
	}

	private itemsForList(query: unknown): readonly CollectionItem<TManager>[] {
		const key = stableStringify(query);
		const cached = this.listItemsCache.get(key);
		if (cached) {
			return cached;
		}
		const items = itemsForQueryFamily(
			this.state,
			query,
			this.reconcileIdentity(),
			this.pendingCommands.values()
		) as CollectionItem<TManager>[];
		this.listItemsCache.set(key, items);
		return items;
	}

	private metaForList(query: unknown): CollectionMeta<TManager> | undefined {
		return metaForQueryFamily(this.state, query) as CollectionMeta<TManager> | undefined;
	}

	private pagesForList(query: unknown): readonly PageState[] {
		return pagesForQueryFamily(this.state, query);
	}

	private async refreshList(query: unknown): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>> {
		return this.requestPage(query, query, false);
	}

	private cache() {
		return this.globalCache.adapter;
	}

	private cacheTtlMs(): number {
		return this.config.cache?.ttlMs ?? this.globalCache.ttlMs ?? 14 * 24 * 60 * 60 * 1000;
	}
}

function pushEvent(events: StoreEventKey[], event: StoreEventKey): void {
	if (!events.includes(event)) {
		events.push(event);
	}
}

function toStoreSyncError(cause: unknown): SyncError {
	if (isSyncError(cause)) {
		return cause;
	}
	return syncError('internal', cause instanceof Error ? cause.message : String(cause));
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isStoreWriteOptions(value: unknown): value is StoreWriteOptions {
	return isRecord(value) && 'signal' in value && (value.signal === undefined || value.signal instanceof AbortSignal);
}

function mergeCursor(query: unknown, cursor: string | undefined): unknown {
	if (!cursor) {
		return query;
	}
	if (!isRecord(query)) {
		return {
			cursor
		};
	}
	return {
		...query,
		cursor
	};
}

function isBatchOutput(value: unknown): value is { readonly items: readonly unknown[] } {
	return isRecord(value) && Array.isArray(value.items);
}

function combineAbortSignals(storeSignal: AbortSignal, callerSignal: AbortSignal | undefined) {
	if (!callerSignal) {
		return {
			signal: storeSignal,
			dispose() {}
		};
	}

	const controller = new AbortController();
	const abort = () => controller.abort();
	if (storeSignal.aborted || callerSignal.aborted) {
		controller.abort();
	} else {
		storeSignal.addEventListener('abort', abort, { once: true });
		callerSignal.addEventListener('abort', abort, { once: true });
	}

	return {
		signal: controller.signal,
		dispose() {
			storeSignal.removeEventListener('abort', abort);
			callerSignal.removeEventListener('abort', abort);
		}
	};
}
