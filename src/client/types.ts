import type { SyncEnvelope, SyncError } from '../shared/index.ts';
import type { SyncResult } from '../shared/result.ts';

export interface MethodType<TQuery, TInput, TOutput> {
	readonly query: TQuery;
	readonly input: TInput;
	readonly output: TOutput;
}

export interface ManagerTypeShape {
	readonly key: string;
	readonly params: object;
	readonly methods: Partial<
		Record<'get' | 'list' | 'add' | 'mutate' | 'delete', MethodType<unknown, unknown, unknown>>
	>;
}

export interface PageState {
	readonly cursorIn?: string;
	readonly cursorOut?: string;
	readonly ids: readonly string[];
	readonly complete: boolean;
	readonly stale: boolean;
	readonly source: 'network' | 'realtime';
}

export interface CacheItem<TValue> {
	readonly value: TValue;
	readonly expiry: number;
}

export interface CacheAdapter {
	get<TValue>(key: string): Promise<CacheItem<TValue> | undefined>;
	set<TValue>(key: string, value: CacheItem<TValue>): Promise<void>;
	del(key: string): Promise<void>;
	keys?(): Promise<readonly string[]>;
}

export interface SyncCacheConfiguration {
	readonly adapter: CacheAdapter;
	readonly ttlMs?: number;
}

export interface RuntimeTransport {
	subscribe(options: {
		readonly connectUrl: string;
		readonly managerKey: string;
		readonly scope: string;
		readonly signal: AbortSignal;
		getCursor?(this: void): string | undefined;
		onEnvelope(this: void, envelope: SyncEnvelope): void;
		onReconnect?(this: void): void;
		onRegistered?(this: void): void;
	}): Promise<SyncResult<() => void, SyncError>>;
	holdRegistrations?(): () => void;
}

export interface SyncRuntime {
	readonly fetch: typeof fetch;
	readonly cache: CacheAdapter;
	readonly transport: RuntimeTransport;
	readonly clientId: string;
	now(this: void): number;
	createId(this: void, prefix: string): string;
	dispose(): void;
}

export interface StoreCacheOptions {
	readonly ttlMs?: number;
}

type ParamsOf<TManager extends ManagerTypeShape> = TManager['params'];
type MethodsOf<TManager extends ManagerTypeShape> = TManager['methods'];
type MethodOf<TManager extends ManagerTypeShape, TKey extends string> = TKey extends keyof MethodsOf<TManager>
	? NonNullable<MethodsOf<TManager>[TKey]>
	: never;
export type QueryOf<TManager extends ManagerTypeShape, TKey extends string> =
	MethodOf<TManager, TKey> extends MethodType<infer TQuery, unknown, unknown> ? TQuery : never;
export type InputOf<TManager extends ManagerTypeShape, TKey extends string> =
	MethodOf<TManager, TKey> extends MethodType<unknown, infer TInput, unknown> ? TInput : never;
export type OutputOf<TManager extends ManagerTypeShape, TKey extends string> =
	MethodOf<TManager, TKey> extends MethodType<unknown, unknown, infer TOutput> ? TOutput : never;

type PageOutputItem<TOutput> = TOutput extends { readonly items: readonly (infer TItem)[] } ? TItem : never;
type PageOutputMeta<TOutput> = TOutput extends { readonly meta?: infer TMeta } ? TMeta : never;
export type CollectionItem<TManager extends ManagerTypeShape> = 'list' extends keyof MethodsOf<TManager>
	? PageOutputItem<OutputOf<TManager, 'list'>>
	: never;
export type CollectionMeta<TManager extends ManagerTypeShape> = 'list' extends keyof MethodsOf<TManager>
	? PageOutputMeta<OutputOf<TManager, 'list'>>
	: never;

export interface ReconcileItemContext<TParams = unknown> {
	readonly params: TParams;
}

export interface ReconcileTargetContext<TParams = unknown, TTargetQuery = unknown, TTargetInput = unknown> {
	readonly params: TParams;
	readonly query: TTargetQuery;
	readonly input: TTargetInput;
}

export interface ReconcileQueryContext<TParams = unknown, TPageQuery = unknown> {
	readonly params: TParams;
	readonly query: TPageQuery | undefined;
}

export interface ReconcileConfig<
	TItem = unknown,
	TParams = unknown,
	TTargetQuery = unknown,
	TTargetInput = unknown,
	TPageQuery = unknown
> {
	itemId?(item: TItem, context: ReconcileItemContext<TParams>): string;
	targetId?(context: ReconcileTargetContext<TParams, TTargetQuery, TTargetInput>): string;
	matchesQuery?(item: TItem, context: ReconcileQueryContext<TParams, TPageQuery>): boolean;
	compare?(this: void, left: TItem, right: TItem): number;
	readonly addPlacement?: 'sorted';
}

export interface ReconcileBuilder<TManager extends ManagerTypeShape> {
	defaults(
		config?: ReconcileConfig<
			CollectionItem<TManager>,
			ParamsOf<TManager>,
			QueryOf<TManager, 'mutate'>,
			InputOf<TManager, 'mutate'> | undefined,
			QueryOf<TManager, 'list'>
		>
	): ReconcileConfig;
}

export type NeedsReconcile<TManager extends ManagerTypeShape> = 'list' extends keyof MethodsOf<TManager>
	? CollectionItem<TManager> extends { readonly id: string }
		? false
		: true
	: false;

interface StoreConfigBase<TManager extends ManagerTypeShape> {
	readonly key: TManager['key'];
	readonly params: ParamsOf<TManager>;
	getUrl(params: ParamsOf<TManager>): string;
	readonly cache?: StoreCacheOptions;
}

export type StoreConfig<TManager extends ManagerTypeShape> = StoreConfigBase<TManager> &
	('list' extends keyof MethodsOf<TManager>
		? [QueryOf<TManager, 'list'>] extends [never]
			? {
					query(): undefined;
				}
			: {
					query(): QueryOf<TManager, 'list'>;
				}
		: {
				query?(): never;
			});
