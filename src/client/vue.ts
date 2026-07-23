import { computed, getCurrentScope, onScopeDispose, shallowRef, type ComputedRef, type ShallowRef } from 'vue';

import { bindStoreEvents, createStore } from './core.ts';
import type {
	ClientStore,
	CollectionMeta,
	CollectionItem,
	ManagerTypeShape,
	NeedsReconcile,
	OutputOf,
	QueryOf,
	ReconcileBuilder,
	ReconcileConfig,
	StoreActionMethods,
	StoreConfig,
	StoreListHandle,
	StoreSnapshot
} from './core.ts';

type VueStoreWithGet<TManager extends ManagerTypeShape> = 'get' extends keyof TManager['methods']
	? {
			readonly data: Readonly<ShallowRef<OutputOf<TManager, 'get'> | undefined>>;
		}
	: {};

type VueStoreWithList<TManager extends ManagerTypeShape> = 'list' extends keyof TManager['methods']
	? [QueryOf<TManager, 'list'>] extends [never]
		? {
				list(): VueStoreListHandle<TManager>;
				readonly items: Readonly<ShallowRef<readonly CollectionItem<TManager>[]>>;
				readonly listMeta: Readonly<ShallowRef<CollectionMeta<TManager> | undefined>>;
				readonly pages: Readonly<ShallowRef<StoreSnapshot<TManager>['pages']>>;
			}
		: {
				list(query: QueryOf<TManager, 'list'>): VueStoreListHandle<TManager>;
				readonly items: Readonly<ShallowRef<readonly CollectionItem<TManager>[]>>;
				readonly listMeta: Readonly<ShallowRef<CollectionMeta<TManager> | undefined>>;
				readonly pages: Readonly<ShallowRef<StoreSnapshot<TManager>['pages']>>;
			}
	: {};

export interface VueStoreListHandle<TManager extends ManagerTypeShape> {
	readonly items: ComputedRef<readonly CollectionItem<TManager>[]>;
	readonly meta: ComputedRef<CollectionMeta<TManager> | undefined>;
	readonly pages: ComputedRef<StoreSnapshot<TManager>['pages']>;
	refresh(): ReturnType<StoreListHandle<TManager>['refresh']>;
	loadMore(): ReturnType<StoreListHandle<TManager>['loadMore']>;
}

export type VueClientStore<TManager extends ManagerTypeShape> = {
	readonly core: ClientStore<TManager>;
	readonly snapshot: ComputedRef<StoreSnapshot<TManager>>;
	readonly pending: Readonly<ShallowRef<StoreSnapshot<TManager>['pending']>>;
	readonly error: Readonly<ShallowRef<StoreSnapshot<TManager>['error']>>;
	readonly isHydrating: Readonly<ShallowRef<boolean>>;
	readonly isRefreshing: Readonly<ShallowRef<boolean>>;
	readonly isPending: ComputedRef<boolean>;
	readonly disposed: Readonly<ShallowRef<boolean>>;
	dispose(): void;
} & StoreActionMethods<TManager> &
	VueStoreWithGet<TManager> &
	VueStoreWithList<TManager>;

export function createVueStore<TManager extends ManagerTypeShape>(
	config: StoreConfig<TManager>,
	...reconcile: NeedsReconcile<TManager> extends true
		? [(builder: ReconcileBuilder<TManager>) => ReconcileConfig]
		: [(builder: ReconcileBuilder<TManager>) => ReconcileConfig] | []
): VueClientStore<TManager> {
	const core = createStore(config, ...reconcile);
	const coreMethods = core as unknown as Record<string, (...args: readonly unknown[]) => unknown>;
	const initialSnapshot = core.snapshot();
	const data = shallowRef<StoreSnapshot<TManager>['data']>(initialSnapshot.data);
	const items = shallowRef<StoreSnapshot<TManager>['items']>(initialSnapshot.items);
	const listMeta = shallowRef<StoreSnapshot<TManager>['listMeta']>(initialSnapshot.listMeta);
	const pages = shallowRef<StoreSnapshot<TManager>['pages']>(initialSnapshot.pages);
	const pending = shallowRef<StoreSnapshot<TManager>['pending']>(initialSnapshot.pending);
	const error = shallowRef<StoreSnapshot<TManager>['error']>(initialSnapshot.error);
	const hydrating = shallowRef(initialSnapshot.hydrating);
	const refreshing = shallowRef(initialSnapshot.refreshing);
	const disposedValue = shallowRef(initialSnapshot.disposed);
	const listHandleVersion = shallowRef(0);
	const unsubscribe = bindStoreEvents<TManager>(core, {
		data: (value) => {
			data.value = value;
		},
		items: (value) => {
			items.value = value;
			bumpListHandleVersion();
		},
		listMeta: (value) => {
			listMeta.value = value;
			bumpListHandleVersion();
		},
		pages: (value) => {
			pages.value = value;
			bumpListHandleVersion();
		},
		pending: (value) => {
			pending.value = value;
		},
		hydrating: (value) => {
			hydrating.value = value;
		},
		refreshing: (value) => {
			refreshing.value = value;
		},
		error: (value) => {
			error.value = value;
		},
		disposed: (value) => {
			disposedValue.value = value;
		}
	});
	const snapshot = computed<StoreSnapshot<TManager>>(() => ({
		data: data.value,
		items: items.value,
		listMeta: listMeta.value,
		pages: pages.value,
		pending: pending.value,
		hydrating: hydrating.value,
		refreshing: refreshing.value,
		disposed: disposedValue.value,
		error: error.value
	}));
	const isPending = computed(() => pending.value.length > 0);
	let disposed = false;

	function dispose(): void {
		if (disposed) {
			return;
		}
		disposed = true;
		core.dispose();
		unsubscribe();
		const nextSnapshot = core.snapshot();
		data.value = nextSnapshot.data;
		items.value = nextSnapshot.items;
		listMeta.value = nextSnapshot.listMeta;
		pages.value = nextSnapshot.pages;
		pending.value = nextSnapshot.pending;
		error.value = nextSnapshot.error;
		hydrating.value = nextSnapshot.hydrating;
		refreshing.value = nextSnapshot.refreshing;
		disposedValue.value = nextSnapshot.disposed;
	}

	if (getCurrentScope()) {
		onScopeDispose(dispose);
	}

	const api: Record<string, unknown> = {
		core,
		snapshot,
		pending,
		error,
		isHydrating: hydrating,
		isRefreshing: refreshing,
		isPending,
		disposed: disposedValue,
		dispose,
		restore: () => core.restore(),
		hydrate: () => core.hydrate(),
		connect: () => core.connect(),
		repair: () => core.repair(),
		data,
		list: (query: unknown) => vueListHandle(coreMethods.list?.(query) as StoreListHandle<TManager>),
		items,
		listMeta,
		pages,
		get: (args?: unknown) => coreMethods.get?.(args),
		refresh: () => coreMethods.refresh?.(),
		loadMore: () => coreMethods.loadMore?.(),
		add: (args: unknown, options?: unknown) => coreMethods.add?.(args, options),
		mutate: (args: unknown, options?: unknown) => coreMethods.mutate?.(args, options),
		delete: (args: unknown, options?: unknown) => coreMethods.delete?.(args, options)
	};

	return api as VueClientStore<TManager>;

	function bumpListHandleVersion(): void {
		listHandleVersion.value += 1;
	}

	function vueListHandle(handle: StoreListHandle<TManager>): VueStoreListHandle<TManager> {
		return {
			items: computed(() => trackedListValue(listHandleVersion.value, handle.items())),
			meta: computed(() => trackedListValue(listHandleVersion.value, handle.meta())),
			pages: computed(() => trackedListValue(listHandleVersion.value, handle.pages())),
			refresh: () => handle.refresh(),
			loadMore: () => handle.loadMore()
		};
	}
}

function trackedListValue<TValue>(_version: number, value: TValue): TValue {
	return value;
}
