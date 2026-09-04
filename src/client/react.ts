import { useSyncExternalStore } from 'react';

import type {
	ClientStore,
	CollectionMeta,
	CollectionItem,
	ManagerTypeShape,
	NeedsReconcile,
	QueryOf,
	ReconcileBuilder,
	ReconcileConfig,
	StoreActionMethods,
	StoreConfig,
	StoreListHandle,
	StoreSnapshot
} from './core.ts';
import { createExternalStoreSource, type ExternalStoreSource } from './reactSource.ts';
import { createStoreRuntime } from './store.ts';

type ReactStoreWithGet<TManager extends ManagerTypeShape> = 'get' extends keyof TManager['methods']
	? {
			useData(): StoreSnapshot<TManager>['data'];
		}
	: {};

type ReactStoreWithList<TManager extends ManagerTypeShape> = 'list' extends keyof TManager['methods']
	? [QueryOf<TManager, 'list'>] extends [never]
		? {
				useItems(): StoreSnapshot<TManager>['items'];
				useListMeta(): StoreSnapshot<TManager>['listMeta'];
				usePages(): StoreSnapshot<TManager>['pages'];
				useList(): ReactStoreListSnapshot<TManager>;
			}
		: {
				useItems(): StoreSnapshot<TManager>['items'];
				useListMeta(): StoreSnapshot<TManager>['listMeta'];
				usePages(): StoreSnapshot<TManager>['pages'];
				useList(query: QueryOf<TManager, 'list'>): ReactStoreListSnapshot<TManager>;
			}
	: {};

export interface ReactStoreListSnapshot<TManager extends ManagerTypeShape> {
	readonly items: readonly CollectionItem<TManager>[];
	readonly meta: CollectionMeta<TManager> | undefined;
	readonly pages: StoreSnapshot<TManager>['pages'];
	refresh(): ReturnType<StoreListHandle<TManager>['refresh']>;
	loadMore(): ReturnType<StoreListHandle<TManager>['loadMore']>;
}

interface ReactAdapterMethods<TValue = unknown> {
	readonly [key: string]: TValue;
}

export type ReactClientStore<TManager extends ManagerTypeShape> = {
	readonly core: ClientStore<TManager>;
	useSnapshot(): StoreSnapshot<TManager>;
	usePending(): StoreSnapshot<TManager>['pending'];
	useError(): StoreSnapshot<TManager>['error'];
	useHydrating(): boolean;
	useRefreshing(): boolean;
	useDisposed(): boolean;
	useIsPending(): boolean;
	dispose(): void;
} & StoreActionMethods<TManager> &
	ReactStoreWithGet<TManager> &
	ReactStoreWithList<TManager>;

export function createReactStore<TManager extends ManagerTypeShape>(
	config: StoreConfig<TManager>,
	...reconcile: NeedsReconcile<TManager> extends true
		? [(builder: ReconcileBuilder<TManager>) => ReconcileConfig]
		: [(builder: ReconcileBuilder<TManager>) => ReconcileConfig] | []
): ReactClientStore<TManager> {
	const core = createStoreRuntime(config, ...reconcile);
	const snapshotSource = createExternalStoreSource(
		() => core.snapshot(),
		(callback) => core.subscribe(callback)
	);
	const dataSource = createExternalStoreSource(
		() => core.snapshot().data,
		(callback) => core.on.data(callback)
	);
	const itemsSource = createExternalStoreSource(
		() => core.snapshot().items,
		(callback) => core.on.items(callback)
	);
	const listMetaSource = createExternalStoreSource(
		() => core.snapshot().listMeta,
		(callback) => core.on.listMeta(callback)
	);
	const pagesSource = createExternalStoreSource(
		() => core.snapshot().pages,
		(callback) => core.on.pages(callback)
	);
	const pendingSource = createExternalStoreSource(
		() => core.snapshot().pending,
		(callback) => core.on.pending(callback)
	);
	const errorSource = createExternalStoreSource(
		() => core.error(),
		(callback) => core.on.error(callback)
	);
	const hydratingSource = createExternalStoreSource(
		() => core.isHydrating(),
		(callback) => core.on.hydrating(callback)
	);
	const refreshingSource = createExternalStoreSource(
		() => core.isRefreshing(),
		(callback) => core.on.refreshing(callback)
	);
	const disposedSource = createExternalStoreSource(
		() => core.snapshot().disposed,
		(callback) => core.on.disposed(callback)
	);

	function useSource<TValue>(source: ExternalStoreSource<TValue>): TValue {
		return useSyncExternalStore(source.subscribe, source.getSnapshot, source.getSnapshot);
	}

	function useList<TQuery>(query?: TQuery): ReactStoreListSnapshot<TManager> {
		useSource(itemsSource);
		useSource(listMetaSource);
		useSource(pagesSource);
		const handle = core.list(query);
		return {
			items: handle.items(),
			meta: handle.meta(),
			pages: handle.pages(),
			refresh: () => handle.refresh(),
			loadMore: () => handle.loadMore()
		};
	}

	const api: ReactAdapterMethods = {
		core,
		useSnapshot: () => useSource(snapshotSource),
		usePending: () => useSource(pendingSource),
		useError: () => useSource(errorSource),
		useHydrating: () => useSource(hydratingSource),
		useRefreshing: () => useSource(refreshingSource),
		useDisposed: () => useSource(disposedSource),
		useIsPending: () => useSource(pendingSource).length > 0,
		dispose: () => core.dispose(),
		restore: () => core.restore(),
		hydrate: () => core.hydrate(),
		connect: () => core.connect(),
		repair: () => core.repair(),
		useData: () => useSource(dataSource),
		useItems: () => useSource(itemsSource),
		useListMeta: () => useSource(listMetaSource),
		usePages: () => useSource(pagesSource),
		useList,
		get: <TQuery>(args?: { readonly query?: TQuery }) => core.get(args),
		refresh: () => core.refresh(),
		loadMore: () => core.loadMore(),
		add: <TArgs>(args: TArgs, options?: { readonly signal?: AbortSignal }) => core.add(args, options),
		mutate: <TArgs>(args: TArgs, options?: { readonly signal?: AbortSignal }) => core.mutate(args, options),
		delete: <TArgs>(args: TArgs, options?: { readonly signal?: AbortSignal }) => core.delete(args, options)
	};

	return api as ReactClientStore<TManager>;
}
