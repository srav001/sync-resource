import { describe, expect, expectTypeOf, it } from 'vite-plus/test';

import {
	bindStoreEvents,
	type ManagerTypeShape,
	type MethodType,
	type ReconcileBuilder,
	type ReconcileConfig,
	type StoreAdapterEventSource,
	type StoreEventHandlers,
	type StoreSnapshot
} from '../src/client/core.ts';
import type { ReactClientStore } from '../src/client/react.ts';
import { createExternalStoreSource } from '../src/client/reactSource.ts';
import { createSolidStore, type SolidClientStore } from '../src/client/solid.ts';
import type { VueClientStore } from '../src/client/vue.ts';

interface Note {
	readonly id: string;
	readonly title: string;
}

interface NotesPage {
	readonly items: readonly Note[];
	readonly meta?: {
		readonly totalCount: number;
	};
}

interface NotesManager extends ManagerTypeShape {
	readonly key: 'notes';
	readonly params: {
		readonly workspaceId: string;
	};
	readonly methods: {
		readonly list: MethodType<{ readonly limit: number }, never, NotesPage>;
		readonly add: MethodType<never, Note, Note>;
		readonly mutate: MethodType<{ readonly id: string }, { readonly title: string }, Note>;
		readonly delete: MethodType<{ readonly id: string }, never, { readonly id: string; readonly deleted: true }>;
	};
}

interface GetOnlyManager extends ManagerTypeShape {
	readonly key: 'profile';
	readonly params: { readonly userId: string };
	readonly methods: {
		readonly get: MethodType<never, never, { readonly name: string }>;
	};
}

interface NoQueryListManager extends ManagerTypeShape {
	readonly key: 'recent-notes';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<never, never, NotesPage>;
	};
}

interface UnidentifiedListManager extends ManagerTypeShape {
	readonly key: 'labels';
	readonly params: { readonly workspaceId: string };
	readonly methods: {
		readonly list: MethodType<never, never, { readonly items: readonly { readonly label: string }[] }>;
	};
}

type HasKey<TValue, TKey extends PropertyKey> = TKey extends keyof TValue ? true : false;

describe('framework adapters', () => {
	it('keeps React snapshots stable and reconnects its lazy source', () => {
		let value = { count: 1 };
		const storeSubscribers = new Set<(nextValue: { readonly count: number }) => void>();
		const source = createExternalStoreSource(
			() => value,
			(callback) => {
				storeSubscribers.add(callback);
				return () => storeSubscribers.delete(callback);
			}
		);
		let notifications = 0;
		const unsubscribe = source.subscribe(() => {
			notifications += 1;
		});

		emitStoreValue({ count: 2 });
		expect(source.getSnapshot()).toEqual({ count: 2 });
		expect(source.getSnapshot()).toBe(source.getSnapshot());
		expect(notifications).toBe(1);
		unsubscribe();
		expect(storeSubscribers).toHaveLength(0);

		value = { count: 3 };
		const unsubscribeAgain = source.subscribe(() => {
			notifications += 1;
		});
		expect(source.getSnapshot()).toEqual({ count: 3 });
		unsubscribeAgain();

		function emitStoreValue(nextValue: { readonly count: number }): void {
			value = nextValue;
			for (const callback of storeSubscribers) {
				callback(nextValue);
			}
		}
	});

	it('binds every fixed store event and removes subscriptions idempotently', () => {
		const events = createEventSource();
		const itemValues: StoreSnapshot<NotesManager>['items'][] = [];
		const disposedValues: boolean[] = [];
		const unsubscribe = bindStoreEvents(events.source, {
			data: () => {},
			items: (value) => itemValues.push(value),
			listMeta: () => {},
			pages: () => {},
			pending: () => {},
			hydrating: () => {},
			refreshing: () => {},
			error: () => {},
			disposed: (value) => disposedValues.push(value)
		});

		events.emitItems([{ id: 'note-1', title: 'One' }]);
		events.emitDisposed(true);
		expect(itemValues).toEqual([[{ id: 'note-1', title: 'One' }]]);
		expect(disposedValues).toEqual([true]);
		unsubscribe();
		unsubscribe();
		events.emitItems([{ id: 'note-2', title: 'Two' }]);
		expect(itemValues).toHaveLength(1);
	});

	it('retains manager-derived types across all adapters', () => {
		expectTypeOf<SolidClientStore<NotesManager>['items']>().returns.toEqualTypeOf<readonly Note[]>();
		expectTypeOf<ReactClientStore<NotesManager>['useItems']>().returns.toEqualTypeOf<readonly Note[]>();
		expectTypeOf<VueClientStore<NotesManager>['items']['value']>().toEqualTypeOf<readonly Note[]>();
		expectTypeOf<SolidClientStore<NotesManager>['mutate']>().toBeCallableWith({
			query: { id: 'note-1' },
			input: { title: 'Updated' }
		});
		expectTypeOf<SolidClientStore<GetOnlyManager>['data']>().returns.toEqualTypeOf<
			{ readonly name: string } | undefined
		>();
		expectTypeOf<ReactClientStore<GetOnlyManager>['useData']>().returns.toEqualTypeOf<
			{ readonly name: string } | undefined
		>();
		expectTypeOf<VueClientStore<GetOnlyManager>['data']['value']>().toEqualTypeOf<
			{ readonly name: string } | undefined
		>();
		expectTypeOf<HasKey<SolidClientStore<GetOnlyManager>, 'items'>>().toEqualTypeOf<false>();
		expectTypeOf<SolidClientStore<NoQueryListManager>['list']>().toBeCallableWith();
		expectTypeOf<ReactClientStore<NoQueryListManager>['useList']>().toBeCallableWith();
		expectTypeOf<VueClientStore<NoQueryListManager>['list']>().toBeCallableWith();
		expectTypeOf<Parameters<typeof createSolidStore<UnidentifiedListManager>>[1]>().toEqualTypeOf<
			(builder: ReconcileBuilder<UnidentifiedListManager>) => ReconcileConfig
		>();
	});
});

function createEventSource(): {
	readonly source: StoreAdapterEventSource<NotesManager>;
	emitItems(value: StoreSnapshot<NotesManager>['items']): void;
	emitDisposed(value: boolean): void;
} {
	const data = new Set<(value: StoreSnapshot<NotesManager>['data']) => void>();
	const items = new Set<(value: StoreSnapshot<NotesManager>['items']) => void>();
	const listMeta = new Set<(value: StoreSnapshot<NotesManager>['listMeta']) => void>();
	const pages = new Set<(value: StoreSnapshot<NotesManager>['pages']) => void>();
	const pending = new Set<(value: StoreSnapshot<NotesManager>['pending']) => void>();
	const hydrating = new Set<(value: boolean) => void>();
	const refreshing = new Set<(value: boolean) => void>();
	const disposed = new Set<(value: boolean) => void>();
	const error = new Set<(value: StoreSnapshot<NotesManager>['error']) => void>();
	const on: StoreEventHandlers<NotesManager> = {
		data: subscribeTo(data),
		items: subscribeTo(items),
		listMeta: subscribeTo(listMeta),
		pages: subscribeTo(pages),
		signal: () => () => {},
		pending: subscribeTo(pending),
		hydrating: subscribeTo(hydrating),
		refreshing: subscribeTo(refreshing),
		disposed: subscribeTo(disposed),
		error: subscribeTo(error)
	};

	return {
		source: { on },
		emitItems(value) {
			for (const callback of items) {
				callback(value);
			}
		},
		emitDisposed(value) {
			for (const callback of disposed) {
				callback(value);
			}
		}
	};
}

function subscribeTo<TValue>(
	callbacks: Set<(value: TValue) => void>
): (callback: (value: TValue) => void) => () => void {
	return (callback) => {
		callbacks.add(callback);
		return () => callbacks.delete(callback);
	};
}
