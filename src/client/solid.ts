import { batch, createSignal, onCleanup } from 'solid-js';

import type { ValueRecord } from '../shared/guards.ts';
import type {
	ClientStore,
	ManagerTypeShape,
	NeedsReconcile,
	ReconcileBuilder,
	ReconcileConfig,
	StoreConfig,
	StoreListHandle,
	StoreSnapshot,
	StoreWriteOptions
} from './core.ts';
import { createBatchedStoreRuntime } from './store.ts';

export type SolidClientStore<TManager extends ManagerTypeShape> = Omit<ClientStore<TManager>, 'on' | 'subscribe'> & {
	readonly core: ClientStore<TManager>;
};

export function createSolidStore<TManager extends ManagerTypeShape>(
	config: StoreConfig<TManager>,
	...reconcile: NeedsReconcile<TManager> extends true
		? [(builder: ReconcileBuilder<TManager>) => ReconcileConfig]
		: [(builder: ReconcileBuilder<TManager>) => ReconcileConfig] | []
): SolidClientStore<TManager> {
	const core = createBatchedStoreRuntime(batch, config, ...reconcile);
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
		batch(() => {
			setSnapshotValue(() => nextSnapshot);
			setData(() => nextSnapshot.data);
			setItems(() => nextSnapshot.items);
			setListMeta(() => nextSnapshot.listMeta);
			setPages(() => nextSnapshot.pages);
			setPending(() => nextSnapshot.pending);
			setError(() => nextSnapshot.error);
			setHydrating(nextSnapshot.hydrating);
			setRefreshing(nextSnapshot.refreshing);
		});
	}

	onCleanup(dispose);

	const api: ValueRecord = {
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
		add: <TArgs>(args: TArgs, options?: StoreWriteOptions) => core.add(args, options),
		mutate: <TArgs>(args: TArgs, options?: StoreWriteOptions) => core.mutate(args, options),
		delete: <TArgs>(args: TArgs, options?: StoreWriteOptions) => core.delete(args, options)
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

	function solidListHandle(handle: StoreListHandle<TManager>): StoreListHandle<TManager> {
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
