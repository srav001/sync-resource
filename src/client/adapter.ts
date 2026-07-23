import type { StoreEventHandlers, StoreSnapshot } from './store.ts';
import type { ManagerTypeShape } from './types.ts';

export interface StoreAdapterEventSinks<TManager extends ManagerTypeShape> {
	data(value: StoreSnapshot<TManager>['data']): void;
	items(value: StoreSnapshot<TManager>['items']): void;
	listMeta(value: StoreSnapshot<TManager>['listMeta']): void;
	pages(value: StoreSnapshot<TManager>['pages']): void;
	pending(value: StoreSnapshot<TManager>['pending']): void;
	hydrating(value: StoreSnapshot<TManager>['hydrating']): void;
	refreshing(value: StoreSnapshot<TManager>['refreshing']): void;
	error(value: StoreSnapshot<TManager>['error']): void;
	disposed?(value: StoreSnapshot<TManager>['disposed']): void;
}

export interface StoreAdapterEventSource<TManager extends ManagerTypeShape> {
	readonly on: StoreEventHandlers<TManager>;
}

export function bindStoreEvents<TManager extends ManagerTypeShape>(
	store: StoreAdapterEventSource<TManager>,
	sinks: StoreAdapterEventSinks<TManager>
): () => void {
	const unsubscribers = [
		store.on.data((value) => sinks.data(value)),
		store.on.items((value) => sinks.items(value)),
		store.on.listMeta((value) => sinks.listMeta(value)),
		store.on.pages((value) => sinks.pages(value)),
		store.on.pending((value) => sinks.pending(value)),
		store.on.hydrating((value) => sinks.hydrating(value)),
		store.on.refreshing((value) => sinks.refreshing(value)),
		store.on.error((value) => sinks.error(value))
	];

	if (sinks.disposed) {
		unsubscribers.push(store.on.disposed((value) => sinks.disposed?.(value)));
	}

	let active = true;
	return () => {
		if (!active) {
			return;
		}
		active = false;
		for (const unsubscribe of unsubscribers) {
			unsubscribe();
		}
	};
}
