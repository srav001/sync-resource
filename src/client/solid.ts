import { createSignal, onCleanup, type Accessor } from 'solid-js';

import type {
	ClientStore,
	CollectionMeta,
	CollectionItem,
	InputOf,
	ManagerTypeShape,
	NeedsReconcile,
	OutputOf,
	QueryOf,
	ReconcileBuilder,
	ReconcileConfig,
	StoreConfig,
	StoreListHandle,
	StoreSnapshot,
	SyncError,
	SyncResult
} from './core.ts';
import { createStoreRuntime } from './store.ts';

type WriteOptions = {
	readonly signal?: AbortSignal;
};

type SolidStoreWithGet<TManager extends ManagerTypeShape> = 'get' extends keyof TManager['methods']
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

type SolidStoreWithList<TManager extends ManagerTypeShape> = 'list' extends keyof TManager['methods']
	? [QueryOf<TManager, 'list'>] extends [never]
		? {
				list(): SolidStoreListHandle<TManager>;
				items(): readonly CollectionItem<TManager>[];
				listMeta(): CollectionMeta<TManager> | undefined;
				pages(): StoreSnapshot<TManager>['pages'];
				refresh(): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>>;
				loadMore(): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>>;
			}
		: {
				list(query: QueryOf<TManager, 'list'>): SolidStoreListHandle<TManager>;
				items(): readonly CollectionItem<TManager>[];
				listMeta(): CollectionMeta<TManager> | undefined;
				pages(): StoreSnapshot<TManager>['pages'];
				refresh(): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>>;
				loadMore(): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>>;
			}
	: {};

export interface SolidStoreListHandle<TManager extends ManagerTypeShape> {
	items(): readonly CollectionItem<TManager>[];
	meta(): CollectionMeta<TManager> | undefined;
	pages(): StoreSnapshot<TManager>['pages'];
	refresh(): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>>;
	loadMore(): Promise<SyncResult<OutputOf<TManager, 'list'>, SyncError>>;
}

interface SolidAdapterMethods<TValue = unknown> {
	readonly [key: string]: TValue;
}

type SolidStoreWithAdd<TManager extends ManagerTypeShape> = 'add' extends keyof TManager['methods']
	? {
			add(
				args: { readonly input: InputOf<TManager, 'add'> },
				options?: WriteOptions
			): Promise<SyncResult<OutputOf<TManager, 'add'>, SyncError>>;
			add(
				args: readonly { readonly input: InputOf<TManager, 'add'> }[],
				options?: WriteOptions
			): Promise<SyncResult<unknown, SyncError>>;
		}
	: {};

type MutateArgs<TManager extends ManagerTypeShape> = [QueryOf<TManager, 'mutate'>] extends [never]
	? { readonly input: InputOf<TManager, 'mutate'> }
	: { readonly query: QueryOf<TManager, 'mutate'>; readonly input: InputOf<TManager, 'mutate'> };

type SolidStoreWithMutate<TManager extends ManagerTypeShape> = 'mutate' extends keyof TManager['methods']
	? {
			mutate(
				args: MutateArgs<TManager>,
				options?: WriteOptions
			): Promise<SyncResult<OutputOf<TManager, 'mutate'>, SyncError>>;
			mutate(
				args: readonly MutateArgs<TManager>[],
				options?: WriteOptions
			): Promise<SyncResult<unknown, SyncError>>;
		}
	: {};

type DeleteArgs<TManager extends ManagerTypeShape> = [QueryOf<TManager, 'delete'>] extends [never]
	? {}
	: { readonly query: QueryOf<TManager, 'delete'> };

type SolidStoreWithDelete<TManager extends ManagerTypeShape> = 'delete' extends keyof TManager['methods']
	? [QueryOf<TManager, 'delete'>] extends [never]
		? {
				delete(options?: WriteOptions): Promise<SyncResult<OutputOf<TManager, 'delete'>, SyncError>>;
			}
		: {
				delete(
					args: DeleteArgs<TManager>,
					options?: WriteOptions
				): Promise<SyncResult<OutputOf<TManager, 'delete'>, SyncError>>;
				delete(
					args: readonly DeleteArgs<TManager>[],
					options?: WriteOptions
				): Promise<SyncResult<unknown, SyncError>>;
			}
	: {};

export type SolidClientStore<TManager extends ManagerTypeShape> = {
	readonly core: ClientStore<TManager>;
	snapshot: Accessor<StoreSnapshot<TManager>>;
	pending(): StoreSnapshot<TManager>['pending'];
	error(): SyncError | null;
	isHydrating(): boolean;
	isRefreshing(): boolean;
	isPending(): boolean;
	restore(): ReturnType<ClientStore<TManager>['restore']>;
	hydrate(): ReturnType<ClientStore<TManager>['hydrate']>;
	connect(): ReturnType<ClientStore<TManager>['connect']>;
	repair(): ReturnType<ClientStore<TManager>['repair']>;
	dispose(): void;
} & SolidStoreWithGet<TManager> &
	SolidStoreWithList<TManager> &
	SolidStoreWithAdd<TManager> &
	SolidStoreWithMutate<TManager> &
	SolidStoreWithDelete<TManager>;

export function createSolidStore<TManager extends ManagerTypeShape>(
	config: StoreConfig<TManager>,
	...reconcile: NeedsReconcile<TManager> extends true
		? [(builder: ReconcileBuilder<TManager>) => ReconcileConfig]
		: [(builder: ReconcileBuilder<TManager>) => ReconcileConfig] | []
): SolidClientStore<TManager> {
	const core = createStoreRuntime(config, ...reconcile);
	const initialSnapshot = core.snapshot();
	const [snapshotValue, setSnapshotValue] = createSignal(initialSnapshot, { equals: false });
	const [data, setData] = createSignal(initialSnapshot.data);
	const [items, setItems] = createSignal(initialSnapshot.items);
	const [listMeta, setListMeta] = createSignal(initialSnapshot.listMeta);
	const [pages, setPages] = createSignal(initialSnapshot.pages);
	const [pending, setPending] = createSignal(initialSnapshot.pending);
	const [error, setError] = createSignal(initialSnapshot.error);
	const [hydrating, setHydrating] = createSignal(initialSnapshot.hydrating);
	const [refreshing, setRefreshing] = createSignal(initialSnapshot.refreshing);
	const [listHandleVersion, setListHandleVersion] = createSignal(0);
	let snapshotUnsubscribe: (() => void) | undefined;
	const unsubscribers = [
		core.on.data((nextData) => setData(() => nextData)),
		core.on.items((nextItems) => {
			setItems(() => nextItems);
			bumpListHandleVersion();
		}),
		core.on.listMeta((nextListMeta) => {
			setListMeta(() => nextListMeta);
			bumpListHandleVersion();
		}),
		core.on.pages((nextPages) => {
			setPages(() => nextPages);
			bumpListHandleVersion();
		}),
		core.on.pending((nextPending) => setPending(() => nextPending)),
		core.on.error((nextError) => setError(() => nextError)),
		core.on.hydrating(setHydrating),
		core.on.refreshing(setRefreshing)
	];
	let disposed = false;

	function dispose(): void {
		if (disposed) {
			return;
		}
		disposed = true;
		core.dispose();
		snapshotUnsubscribe?.();
		snapshotUnsubscribe = undefined;
		for (const unsubscribe of unsubscribers) {
			unsubscribe();
		}
		const nextSnapshot = core.snapshot();
		setSnapshotValue(() => nextSnapshot);
		setData(() => nextSnapshot.data);
		setItems(() => nextSnapshot.items);
		setListMeta(() => nextSnapshot.listMeta);
		setPages(() => nextSnapshot.pages);
		setPending(() => nextSnapshot.pending);
		setError(() => nextSnapshot.error);
		setHydrating(nextSnapshot.hydrating);
		setRefreshing(nextSnapshot.refreshing);
	}

	onCleanup(dispose);

	const api: SolidAdapterMethods = {
		core,
		snapshot,
		pending,
		error,
		isHydrating: hydrating,
		isRefreshing: refreshing,
		isPending: () => pending().length > 0,
		restore: () => core.restore(),
		hydrate: () => core.hydrate(),
		connect: () => core.connect(),
		repair: () => core.repair(),
		dispose,
		data,
		list: <TQuery>(query: TQuery) => solidListHandle(core.list(query)),
		items,
		listMeta,
		pages,
		get: <TQuery>(args?: { readonly query?: TQuery }) => core.get(args),
		refresh: () => core.refresh(),
		loadMore: () => core.loadMore(),
		add: <TArgs>(args: TArgs, options?: WriteOptions) => core.add(args, options),
		mutate: <TArgs>(args: TArgs, options?: WriteOptions) => core.mutate(args, options),
		delete: <TArgs>(args: TArgs, options?: WriteOptions) => core.delete(args, options)
	};

	return api as SolidClientStore<TManager>;

	function snapshot(): StoreSnapshot<TManager> {
		if (!snapshotUnsubscribe && !disposed) {
			snapshotUnsubscribe = core.subscribe((nextSnapshot) => setSnapshotValue(() => nextSnapshot));
		}
		return snapshotValue();
	}

	function bumpListHandleVersion(): void {
		setListHandleVersion((version) => version + 1);
	}

	function solidListHandle(handle: StoreListHandle<TManager>): SolidStoreListHandle<TManager> {
		return {
			items() {
				listHandleVersion();
				return handle.items();
			},
			meta() {
				listHandleVersion();
				return handle.meta();
			},
			pages() {
				listHandleVersion();
				return handle.pages();
			},
			refresh() {
				return handle.refresh();
			},
			loadMore() {
				return handle.loadMore();
			}
		};
	}
}
