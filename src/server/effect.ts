import { Cause, Context, Effect, Exit, Predicate, Scope, Tracer } from 'effect';
import { HttpRouter, HttpServerRequest, HttpServerResponse } from 'effect/unstable/http';

import { err, ok, type SyncResult } from '../shared/result.js';
import { isSyncError, normalizeSyncError, type SyncError } from './errors.js';
import { executionForCommit } from './executionState.js';
import { manager as plainManager, type CreateManagerOptions, type Manager as PlainManager } from './manager.js';
import {
	resource as plainResource,
	methodKinds,
	type MethodKind,
	type Resource as PlainResource,
	type ResourceMethodMap
} from './resource.js';
import { httpSharedSyncStream } from './streamMultiplexer.js';
import type {
	AnySchema,
	AuthorizeResult,
	CostMetric,
	InferSchemaOutput,
	ManagerHttpHandlers,
	ManagerMutationRecord,
	ManagerOutboxRead,
	ManagerTelemetryContext,
	MethodType,
	OperationExecution,
	OperationOptions,
	ResourceCommitContext,
	ResourceCommitResult,
	ResourceHandlerContext,
	ResourceOk,
	ResourceOptions,
	ResourceSignalOk,
	ResourceStateOk,
	ResourceTelemetryContext,
	ScopeValue,
	SyncBatchOutput,
	SyncEnvelope
} from './types.js';

const EffectEnvironmentTypeId: unique symbol = Symbol('@sync-resource/effect/environment');
const PlainResourceTypeId: unique symbol = Symbol.for('@sync-resource/effect/plain-resource');
const unboundedSignal = new AbortController().signal;
const managerHttpHandlerNames = ['get', 'list', 'add', 'mutate', 'delete', 'connect', 'events'] as const;

export type EffectOperationOptions = Omit<OperationOptions, 'signal' | 'environment'>;
export type EffectResourceSelf = Partial<
	Record<MethodKind, (...args: readonly unknown[]) => Effect.Effect<unknown, SyncError, never>>
>;

type ResourceCallMap = Partial<
	Record<MethodKind, (...args: readonly unknown[]) => Promise<SyncResult<unknown, SyncError>>>
>;
type EffectRuntimeMethod = <TArgs>(
	args: TArgs,
	options?: EffectOperationOptions
) => Effect.Effect<unknown, SyncError, unknown>;
type EffectRuntimeMethodMap = Partial<Record<MethodKind, EffectRuntimeMethod>>;
type RuntimeBoundManager = {
	readonly key: string;
	readonly params: object;
	readonly scope: string;
} & Partial<
	Record<
		MethodKind,
		((options?: EffectOperationOptions) => Effect.Effect<unknown, SyncError, unknown>) | EffectRuntimeMethod
	>
>;

interface AdaptedResourceDefinition {
	readonly kind: MethodKind;
	handler<TArgs>(
		args: TArgs & { readonly ctx: ResourceHandlerContext; readonly self: ResourceCallMap }
	): Promise<SyncResult<unknown, SyncError>>;
	batchHandler?<TArgs>(
		args: TArgs & { readonly ctx: ResourceHandlerContext }
	): Promise<SyncResult<unknown, SyncError>>;
}

export interface EffectResourceCommit<R> {
	commit(context: ResourceCommitContext): Effect.Effect<ResourceCommitResult | void, SyncError, R>;
}
export type EffectResourceHandlerOk<T, R> =
	| (Omit<ResourceStateOk<T>, 'sourceCommit'> & { readonly sourceCommit?: EffectResourceCommit<R> })
	| ResourceSignalOk;

type HandlerValue<T, R> = T | EffectResourceHandlerOk<T, R>;
type Handler<TArgs, TOutput, R> = (
	args: TArgs & { readonly ctx: ResourceHandlerContext; readonly self: EffectResourceSelf }
) => Effect.Effect<HandlerValue<TOutput, R>, SyncError, R>;
type BatchHandler<TParams extends object, TItem, TOutput, R> = (args: {
	readonly params: TParams;
	readonly items: readonly TItem[];
	readonly ctx: ResourceHandlerContext;
}) => Effect.Effect<EffectSyncBatchOutput<EffectResourceHandlerOk<TOutput, R>, R>, SyncError, R>;

type EffectSyncBatchOutput<TOutput, R> = Omit<SyncBatchOutput<TOutput>, 'sourceCommit'> & {
	readonly sourceCommit?: EffectResourceCommit<R>;
};

declare const EffectMethodTypeId: unique symbol;
interface EffectMethod<Type, R> {
	readonly [EffectMethodTypeId]?: { readonly type: Type; readonly requirements: R };
}

type SchemaField<Key extends 'query' | 'input', T> = [T] extends [never]
	? { readonly [K in Key]?: never }
	: { readonly [K in Key]: AnySchema<T> };
type MethodArgs<TParams extends object, TQuery, TInput> = { readonly params: TParams } & ([TQuery] extends [never]
	? object
	: { readonly query: TQuery }) &
	([TInput] extends [never] ? object : { readonly input: TInput });
type BatchItem<TQuery, TInput> = ([TQuery] extends [never] ? object : { readonly query: TQuery }) &
	([TInput] extends [never] ? object : { readonly input: TInput });
type OutputSchema<TKind extends MethodKind, TOutput> = TKind extends 'get' | 'list'
	? { readonly output: AnySchema<TOutput> }
	: { readonly output?: AnySchema<TOutput> };

export type EffectMethodDefinition<
	TKind extends MethodKind,
	TParams extends object,
	TQuery,
	TInput,
	TOutput,
	R
> = EffectMethod<MethodType<TQuery, TInput, TOutput>, R> & { readonly kind: TKind } & EffectMethodOptions<
		TKind,
		TParams,
		TQuery,
		TInput,
		TOutput,
		R
	>;

type EffectMethodOptions<TKind extends MethodKind, TParams extends object, TQuery, TInput, TOutput, R> = SchemaField<
	'query',
	TQuery
> &
	SchemaField<'input', TInput> &
	OutputSchema<TKind, TOutput> & {
		readonly handler: Handler<MethodArgs<TParams, TQuery, TInput>, NoInfer<TOutput>, R>;
		readonly batchHandler?: TKind extends 'add' | 'mutate' | 'delete'
			? BatchHandler<TParams, BatchItem<TQuery, TInput>, NoInfer<TOutput>, R>
			: never;
	};

type EffectHandlerRequirements<THandler> = THandler extends (...args: never[]) => infer TResult
	? TResult extends Effect.Effect<unknown, SyncError, infer R>
		? R
		: never
	: never;
type ValidEffectHandler<THandler, TOutput> = THandler extends (...args: never[]) => infer TResult
	? TResult extends Effect.Effect<infer TValue, SyncError, infer R>
		? TValue extends HandlerValue<TOutput, R>
			? unknown
			: never
		: never
	: never;
type EffectHandlerFunction<TParams extends object, TQuery, TInput, TOutput> = (
	args: MethodArgs<TParams, TQuery, TInput> & {
		readonly ctx: ResourceHandlerContext;
		readonly self: EffectResourceSelf;
	}
) => Effect.Effect<HandlerValue<TOutput, unknown>, SyncError, unknown>;
type EffectHandlerOptions<TKind extends MethodKind, TParams extends object, TQuery, TInput, TOutput, THandler> = Omit<
	EffectMethodOptions<TKind, TParams, TQuery, TInput, TOutput, unknown>,
	'handler'
> & {
	readonly handler: THandler;
} & ValidEffectHandler<THandler, TOutput>;

export type EffectResourceMethodDefinition<TParams extends object = object> =
	| EffectMethodDefinition<'get', TParams, unknown, never, unknown, unknown>
	| EffectMethodDefinition<'list', TParams, unknown, never, unknown, unknown>
	| EffectMethodDefinition<'add', TParams, never, unknown, unknown, unknown>
	| EffectMethodDefinition<'mutate', TParams, unknown, unknown, unknown, unknown>
	| EffectMethodDefinition<'delete', TParams, unknown, never, unknown, unknown>;
export type EffectResourceMethodMap = Partial<Record<MethodKind, { readonly kind: MethodKind }>>;

export interface EffectMethodBuilder<TParams extends object> {
	get<TOutput, THandler extends EffectHandlerFunction<TParams, never, never, TOutput>>(
		definition: EffectHandlerOptions<'get', TParams, never, never, TOutput, THandler>
	): EffectMethodDefinition<'get', TParams, never, never, TOutput, EffectHandlerRequirements<THandler>>;
	get<TQuery, TOutput, THandler extends EffectHandlerFunction<TParams, TQuery, never, TOutput>>(
		definition: EffectHandlerOptions<'get', TParams, TQuery, never, TOutput, THandler>
	): EffectMethodDefinition<'get', TParams, TQuery, never, TOutput, EffectHandlerRequirements<THandler>>;
	list<TOutput, THandler extends EffectHandlerFunction<TParams, never, never, TOutput>>(
		definition: EffectHandlerOptions<'list', TParams, never, never, TOutput, THandler>
	): EffectMethodDefinition<'list', TParams, never, never, TOutput, EffectHandlerRequirements<THandler>>;
	list<TQuery, TOutput, THandler extends EffectHandlerFunction<TParams, TQuery, never, TOutput>>(
		definition: EffectHandlerOptions<'list', TParams, TQuery, never, TOutput, THandler>
	): EffectMethodDefinition<'list', TParams, TQuery, never, TOutput, EffectHandlerRequirements<THandler>>;
	add<TInput, TOutput = void, R = never>(
		definition: EffectMethodOptions<'add', TParams, never, TInput, TOutput, R>
	): EffectMethodDefinition<'add', TParams, never, TInput, TOutput, R>;
	mutate<TInput, TOutput = void, R = never>(
		definition: EffectMethodOptions<'mutate', TParams, never, TInput, TOutput, R>
	): EffectMethodDefinition<'mutate', TParams, never, TInput, TOutput, R>;
	mutate<TQuery, TInput, TOutput = void, R = never>(
		definition: EffectMethodOptions<'mutate', TParams, TQuery, TInput, TOutput, R>
	): EffectMethodDefinition<'mutate', TParams, TQuery, TInput, TOutput, R>;
	delete<TOutput = void, R = never>(
		definition: EffectMethodOptions<'delete', TParams, never, never, TOutput, R>
	): EffectMethodDefinition<'delete', TParams, never, never, TOutput, R>;
	delete<TQuery, TOutput = void, R = never>(
		definition: EffectMethodOptions<'delete', TParams, TQuery, never, TOutput, R>
	): EffectMethodDefinition<'delete', TParams, TQuery, never, TOutput, R>;
}

type EffectMethodType<T> = T extends EffectMethod<infer Type, infer _Requirements> ? Type : never;

export interface EffectResourceWriteMethod<TArgs, TOutput, R> {
	(args: TArgs, options?: EffectOperationOptions): Effect.Effect<ResourceOk<TOutput>, SyncError, R>;
	(
		args: readonly TArgs[],
		options?: EffectOperationOptions
	): Effect.Effect<SyncBatchOutput<ResourceOk<TOutput>>, SyncError, R>;
}

type EffectResourceMethod<TParams extends object, T, R> = T extends { readonly kind: infer Kind }
	? EffectMethodType<T> extends MethodType<infer Query, infer Input, infer Output>
		? Kind extends 'add' | 'mutate' | 'delete'
			? EffectResourceWriteMethod<
					([Query] extends [never] ? object : { readonly query: Query }) &
						([Input] extends [never] ? object : { readonly input: Input }) & { readonly params: TParams },
					Output,
					R
				>
			: (
					args: { readonly params: TParams } & ([Query] extends [never] ? object : { readonly query: Query }),
					options?: EffectOperationOptions
				) => Effect.Effect<ResourceOk<Output>, SyncError, R>
		: never
	: never;

export type EffectResource<TParams extends object, TMethods extends EffectResourceMethodMap> = {
	readonly [K in keyof TMethods]: EffectResourceMethod<TParams, TMethods[K], ResourceRequirements<TMethods>>;
} & {
	readonly definition: { readonly params: AnySchema<TParams>; readonly methods: TMethods };
	readonly type: {
		readonly params: TParams;
		readonly methods: { readonly [K in keyof TMethods]: EffectMethodType<TMethods[K]> };
	};
	readonly [PlainResourceTypeId]: PlainResource<TParams, ResourceMethodMap>;
};

interface EffectResourceLike {
	readonly definition: { readonly methods: EffectResourceMethodMap };
	readonly type: { readonly params: object; readonly methods: unknown };
	readonly [PlainResourceTypeId]: unknown;
}

export interface EffectResourceOptions extends Omit<ResourceOptions, 'telemetry' | 'handleError'> {
	readonly telemetry?: {
		start?(this: void, context: ResourceTelemetryContext): Effect.Effect<void, SyncError>;
		success?(
			this: void,
			context: ResourceTelemetryContext,
			metrics: readonly CostMetric[]
		): Effect.Effect<void, SyncError>;
		error?(this: void, context: ResourceTelemetryContext, error: SyncError): Effect.Effect<void, SyncError>;
	};
	handleError?(this: void, error: SyncError, context: ResourceTelemetryContext): Effect.Effect<void, SyncError>;
}

const effectMethodBuilder = {
	get: <TDefinition extends {}>(definition: TDefinition) => ({ kind: 'get', ...definition }),
	list: <TDefinition extends {}>(definition: TDefinition) => ({ kind: 'list', ...definition }),
	add: <TDefinition extends {}>(definition: TDefinition) => ({ kind: 'add', ...definition }),
	mutate: <TDefinition extends {}>(definition: TDefinition) => ({ kind: 'mutate', ...definition }),
	delete: <TDefinition extends {}>(definition: TDefinition) => ({ kind: 'delete', ...definition })
} as EffectMethodBuilder<object>;

export function resource<TParamsSchema extends AnySchema<object>, const TMethods extends EffectResourceMethodMap>(
	params: TParamsSchema,
	factory: (method: EffectMethodBuilder<InferSchemaOutput<TParamsSchema>>) => TMethods,
	options?: EffectResourceOptions
): EffectResource<InferSchemaOutput<TParamsSchema>, TMethods> {
	const methods = factory(effectMethodBuilder as EffectMethodBuilder<InferSchemaOutput<TParamsSchema>>);
	const plain = plainResource(params, () => adaptResourceDefinitions(methods), adaptResourceOptions(options));
	const calls: EffectRuntimeMethodMap = {};
	for (const kind of methodKinds(methods)) {
		const call = plain[kind];
		if (Predicate.isFunction(call)) {
			const runtimeCall = call as <TArgs>(
				args: TArgs,
				options?: OperationOptions
			) => Promise<SyncResult<unknown, SyncError>>;
			calls[kind] = <TArgs>(args: TArgs, options?: EffectOperationOptions) =>
				liftKernel((signal, context) => runtimeCall(args, operationOptions(options, signal, context)));
		}
	}
	return {
		...calls,
		definition: { params, methods },
		type: plain.type,
		[PlainResourceTypeId]: plain
	} as EffectResource<InferSchemaOutput<TParamsSchema>, TMethods>;
}

type ResourceParams<T> = T extends { readonly type: { readonly params: infer P extends object } } ? P : never;
type ResourceDefinitions<T> = T extends { readonly definition: { readonly methods: infer M } } ? M : never;
type MethodRequirements<T> = T extends { readonly handler: infer THandler }
	? THandler extends (...args: never[]) => infer TResult
		? TResult extends Effect.Effect<unknown, SyncError, infer R>
			? R
			: never
		: never
	: never;
type ResourceRequirements<TMethods> = MethodRequirements<TMethods[keyof TMethods]>;

export interface EffectManagerWriteMethod<TArgs, TOutput, R> {
	(args: TArgs, options?: EffectOperationOptions): Effect.Effect<TOutput, SyncError, R>;
	(args: readonly TArgs[], options?: EffectOperationOptions): Effect.Effect<SyncBatchOutput<TOutput>, SyncError, R>;
}

type EffectManagerMethod<T, R> =
	EffectMethodType<T> extends MethodType<infer Query, infer Input, infer Output>
		? [Query] extends [never]
			? [Input] extends [never]
				? (options?: EffectOperationOptions) => Effect.Effect<Output, SyncError, R>
				: EffectManagerWriteMethod<{ readonly input: Input }, Output, R>
			: [Input] extends [never]
				? (
						args: { readonly query: Query },
						options?: EffectOperationOptions
					) => Effect.Effect<Output, SyncError, R>
				: EffectManagerWriteMethod<{ readonly query: Query; readonly input: Input }, Output, R>
		: never;

export type EffectBoundManager<TKey extends string, TResource> = {
	readonly [K in keyof ResourceDefinitions<TResource>]: EffectManagerMethod<
		ResourceDefinitions<TResource>[K],
		ResourceRequirements<ResourceDefinitions<TResource>>
	>;
} & { readonly key: TKey; readonly params: ResourceParams<TResource>; readonly scope: string };

export interface EffectManager<TKey extends string, TResource, TContext = undefined> {
	readonly key: TKey;
	readonly type: {
		readonly key: TKey;
		readonly params: ResourceParams<TResource>;
		readonly methods: TResource extends { readonly type: { readonly methods: infer M } } ? M : never;
	};
	readonly http: ManagerHttpHandlers<TContext, ResourceParams<TResource>>;
	readonly _effectRequirements?: ResourceRequirements<ResourceDefinitions<TResource>>;
	bind(params: ResourceParams<TResource>, context?: TContext): EffectBoundManager<TKey, TResource>;
}

export interface EffectManagerOutbox<R> {
	append(this: void, envelope: SyncEnvelope): Effect.Effect<void, SyncError, R>;
	readAfter(this: void, scope: string, cursor: string, limit: number): Effect.Effect<ManagerOutboxRead, SyncError, R>;
}
export interface EffectManagerPersistence<R> extends EffectManagerOutbox<R> {
	readMutation(
		this: void,
		scope: string,
		mutationId: string
	): Effect.Effect<ManagerMutationRecord | undefined, SyncError, R>;
	recordMutation(this: void, record: ManagerMutationRecord): Effect.Effect<void, SyncError, R>;
}
export interface EffectManagerRealtimeBus<R> {
	publish(this: void, envelope: SyncEnvelope): Effect.Effect<void, SyncError, R>;
	subscribe(
		this: void,
		scope: string,
		onEnvelope: (envelope: SyncEnvelope) => void
	): Effect.Effect<void, SyncError, R | Scope.Scope>;
}

export interface CreateEffectManagerOptions<TKey extends string, TResource, TContext, R> {
	readonly key: TKey;
	readonly resource: TResource;
	readonly authorize: (
		context: TContext | undefined,
		params: ResourceParams<TResource>
	) => Effect.Effect<AuthorizeResult, SyncError, R>;
	scope(params: ResourceParams<TResource>): ScopeValue;
	readonly maxPayloadBytes?: number;
	readonly replayLimit?: number;
	readonly outbox?: EffectManagerOutbox<R>;
	readonly persistence?: EffectManagerPersistence<R>;
	readonly realtimeBus?: EffectManagerRealtimeBus<R>;
	readonly telemetry?: {
		start?(this: void, context: ManagerTelemetryContext): Effect.Effect<void, SyncError, R>;
		success?(
			this: void,
			context: ManagerTelemetryContext,
			metrics: readonly CostMetric[]
		): Effect.Effect<void, SyncError, R>;
		error?(this: void, context: ManagerTelemetryContext, error: SyncError): Effect.Effect<void, SyncError, R>;
	};
	readonly stream?: {
		readonly heartbeatMs?: number;
		readonly idleTtlMs?: number;
		readonly maxConnectionsPerIp?: number;
		readonly maxEventBytes?: number;
		onScopeIdle?(this: void, scope: string): Effect.Effect<void, SyncError, R>;
	};
	handleError?(this: void, error: SyncError, context: ManagerTelemetryContext): Effect.Effect<void, SyncError, R>;
}

export function manager<TKey extends string, TResource extends EffectResourceLike, TContext = undefined, R = never>(
	options: CreateEffectManagerOptions<TKey, TResource, TContext, R>
): Effect.Effect<EffectManager<TKey, TResource, TContext>, never, R> {
	return Effect.contextWith((context: Context.Context<R>) =>
		Effect.sync(() => {
			// Startup scopes and parent spans must not leak into later manager calls.
			const base: Context.Context<R> = Context.omit(Scope.Scope, Tracer.ParentSpan)(context) as never;
			const plain: PlainManager<
				TKey,
				PlainResource<object, ResourceMethodMap>,
				TContext,
				ResourceParams<TResource>
			> = plainManager(adaptManagerOptions(options, base) as never);
			return wrapManager(plain, options.resource, base);
		})
	);
}

export interface EffectManagerHttp<TContext, TParams, R = never> {
	readonly http: ManagerHttpHandlers<TContext, TParams>;
	readonly _effectRequirements?: R;
}
export interface EffectManagerRoutesOptions<TContext, TParams, TError = never, R = never, ManagerR = never> {
	readonly manager: EffectManagerHttp<TContext, TParams, ManagerR>;
	readonly path: `/${string}`;
	readonly params: (request: HttpServerRequest.HttpServerRequest) => Effect.Effect<TParams, TError, R>;
	readonly context?: (request: HttpServerRequest.HttpServerRequest) => Effect.Effect<TContext, TError, R>;
	readonly events?: boolean;
}

export function httpSharedSyncStreamRoute(path: `/${string}`): HttpRouter.Route<never, never> {
	return HttpRouter.route('POST', path, (request) => webHandler(request, httpSharedSyncStream));
}

export function managerHttpRoutes<TContext, TParams, TError = never, R = never, ManagerR = never>(
	options: EffectManagerRoutesOptions<TContext, TParams, TError, R, ManagerR>
): readonly HttpRouter.Route<TError, R | ManagerR>[] {
	const routes: HttpRouter.Route<TError, R | ManagerR>[] = [];
	addManagerRoute(routes, options, 'GET', 'get');
	addManagerRoute(routes, options, 'GET', 'list');
	addManagerRoute(routes, options, 'POST', 'add');
	addManagerRoute(routes, options, 'POST', 'mutate');
	addManagerRoute(routes, options, 'POST', 'delete');
	addManagerRoute(routes, options, 'POST', 'connect');
	if (options.events) {
		addManagerRoute(routes, options, 'GET', 'events');
	}
	return routes;
}

function addManagerRoute<TContext, TParams, TError, R, ManagerR>(
	routes: HttpRouter.Route<TError, R | ManagerR>[],
	options: EffectManagerRoutesOptions<TContext, TParams, TError, R, ManagerR>,
	method: 'GET' | 'POST',
	name: keyof ManagerHttpHandlers<TContext, TParams>
): void {
	const handler = options.manager.http[name];
	if (!handler) {
		return;
	}
	routes.push(
		HttpRouter.route(method, `${options.path}/${name}` as `/${string}`, (request) =>
			Effect.gen(function* () {
				const params = yield* options.params(request);
				const context = options.context ? yield* options.context(request) : undefined;
				const effectContext = yield* Effect.context<R | ManagerR>();
				return yield* webHandler(request, (webRequest) =>
					handler({
						request: webRequest,
						params,
						context,
						options: { environment: effectEnvironment(effectContext) }
					})
				);
			})
		)
	);
}

function adaptResourceDefinitions(methods: EffectResourceMethodMap): ResourceMethodMap {
	const adapted: ResourceMethodMap = {};
	for (const kind of methodKinds(methods)) {
		const definition = methods[kind] as EffectResourceMethodDefinition;
		const { batchHandler, ...plainDefinition } = definition;
		const adaptedDefinition: AdaptedResourceDefinition = {
			...plainDefinition,
			handler: <TArgs>(
				args: TArgs & {
					readonly ctx: ResourceHandlerContext;
					readonly self: ResourceCallMap;
				}
			) =>
				lowerResourceHandler(
					Effect.suspend(() => definition.handler({ ...args, self: wrapSelf(args.self, methods) } as never)),
					args.ctx
				)
		};
		if (batchHandler) {
			adaptedDefinition.batchHandler = (args: { readonly ctx: ResourceHandlerContext }) =>
				lowerResourceHandler(
					Effect.suspend(() => batchHandler(args as never)),
					args.ctx
				);
		}
		adapted[kind] = adaptedDefinition;
	}
	return adapted;
}

function adaptResourceOptions(options: EffectResourceOptions | undefined): ResourceOptions | undefined {
	if (!options) {
		return undefined;
	}
	const context = Context.empty();
	const telemetry = options.telemetry;
	const start = telemetry?.start;
	const success = telemetry?.success;
	const error = telemetry?.error;
	const handleError = options.handleError;
	const run = <A>(make: () => Effect.Effect<A, SyncError>, execution: OperationExecution) =>
		runHook(Effect.suspend(make), context, execution.signal);
	const runFinal = <A>(make: () => Effect.Effect<A, SyncError>) => runHook(Effect.suspend(make), context);
	return {
		name: options.name,
		telemetry: telemetry
			? {
					start: start ? (value) => run(() => start(value), value) : undefined,
					success: success ? (value, metrics) => run(() => success(value, metrics), value) : undefined,
					error: error ? (value, errorValue) => runFinal(() => error(value, errorValue)) : undefined
				}
			: undefined,
		handleError: handleError ? (errorValue, value) => runFinal(() => handleError(errorValue, value)) : undefined
	};
}

function adaptManagerOptions<TKey extends string, TResource extends EffectResourceLike, TContext, R>(
	options: CreateEffectManagerOptions<TKey, TResource, TContext, R>,
	base: Context.Context<R>
): CreateManagerOptions<TKey, ResourceParams<TResource>, PlainResource<object, ResourceMethodMap>, TContext> {
	const outbox = options.outbox;
	const persistence = options.persistence;
	const realtimeBus = options.realtimeBus;
	const telemetry = options.telemetry;
	const telemetryStart = telemetry?.start;
	const telemetrySuccess = telemetry?.success;
	const telemetryError = telemetry?.error;
	const stream = options.stream;
	const onScopeIdle = stream?.onScopeIdle;
	const handleError = options.handleError;
	const run = <A>(make: () => Effect.Effect<A, SyncError, R>, execution?: OperationExecution) =>
		runHook(Effect.suspend(make), contextFromEnvironment(execution?.environment, base), execution?.signal);
	const runFinal = <A>(make: () => Effect.Effect<A, SyncError, R>, execution?: OperationExecution) =>
		runHook(Effect.suspend(make), contextFromEnvironment(execution?.environment, base));
	return {
		...options,
		resource: options.resource[PlainResourceTypeId] as PlainResource<object, ResourceMethodMap>,
		authorize: (context, params, execution) => run(() => options.authorize(context, params), execution),
		outbox: outbox
			? {
					append: (envelope, execution) => runFinal(() => outbox.append(envelope), execution),
					readAfter: (scope, cursor, limit, execution) =>
						run(() => outbox.readAfter(scope, cursor, limit), execution)
				}
			: undefined,
		persistence: persistence
			? {
					append: (envelope, execution) => runFinal(() => persistence.append(envelope), execution),
					readAfter: (scope, cursor, limit, execution) =>
						run(() => persistence.readAfter(scope, cursor, limit), execution),
					readMutation: (scope, mutationId, execution) =>
						run(() => persistence.readMutation(scope, mutationId), execution),
					recordMutation: (record, execution) => runFinal(() => persistence.recordMutation(record), execution)
				}
			: undefined,
		realtimeBus: realtimeBus
			? {
					publish: (envelope) => run(() => realtimeBus.publish(envelope)),
					subscribe: (scope, onEnvelope, execution) =>
						subscribeScoped(
							Effect.suspend(() => realtimeBus.subscribe(scope, onEnvelope)),
							base,
							execution?.signal
						)
				}
			: undefined,
		telemetry: telemetry
			? {
					start: telemetryStart ? (value) => run(() => telemetryStart(value), value) : undefined,
					success: telemetrySuccess
						? (value, metrics) => run(() => telemetrySuccess(value, metrics), value)
						: undefined,
					error: telemetryError
						? (value, errorValue) => runFinal(() => telemetryError(value, errorValue), value)
						: undefined
				}
			: undefined,
		stream: stream
			? {
					...stream,
					onScopeIdle: onScopeIdle
						? (scope) => {
								run(() => onScopeIdle(scope))
									.catch((cause: unknown) => {
										if (!handleError) {
											return;
										}
										return run(() =>
											handleError(normalizeSyncError(cause), {
												manager: options.key,
												method: 'stream.onScopeIdle',
												scope,
												signal: unboundedSignal,
												environment: effectEnvironment(base)
											})
										);
									})
									.catch(() => undefined);
							}
						: undefined
				}
			: undefined,
		handleError: handleError
			? (errorValue, value) => runFinal(() => handleError(errorValue, value), value)
			: undefined
	};
}

function wrapManager<TKey extends string, TResource extends EffectResourceLike, TContext, R>(
	plain: PlainManager<TKey, PlainResource<object, ResourceMethodMap>, TContext, ResourceParams<TResource>>,
	resourceValue: TResource,
	base: Context.Context<R>
): EffectManager<TKey, TResource, TContext> {
	const http: ManagerHttpHandlers<TContext, ResourceParams<TResource>> = {};
	for (const name of managerHttpHandlerNames) {
		const handler = plain.http[name];
		if (!handler) {
			continue;
		}
		http[name] = (args) => {
			const caller = contextFromEnvironment(args.options?.environment, Context.empty());
			return handler({
				...args,
				options: {
					...args.options,
					environment: effectEnvironment(Context.merge(base, caller))
				}
			});
		};
	}
	return {
		key: plain.key,
		type: plain.type as EffectManager<TKey, TResource, TContext>['type'],
		http,
		bind(params, context) {
			const bound = plain.bind(params, context);
			const wrapped: RuntimeBoundManager = { key: bound.key, params: bound.params, scope: bound.scope };
			for (const kind of methodKinds(resourceValue.definition.methods)) {
				const call = bound[kind];
				if (!Predicate.isFunction(call)) {
					continue;
				}
				const runtimeCall = call as (...args: readonly unknown[]) => Promise<SyncResult<unknown, SyncError>>;
				const definition = resourceValue.definition.methods[kind] as { readonly query?: unknown };
				const optionOnly =
					(kind === 'get' || kind === 'list' || kind === 'delete') && definition.query === undefined;
				wrapped[kind] = optionOnly
					? (options?: EffectOperationOptions) =>
							liftKernel((signal, caller) =>
								runtimeCall(operationOptions(options, signal, Context.merge(base, caller)))
							)
					: <TArgs>(args: TArgs, options?: EffectOperationOptions) =>
							liftKernel((signal, caller) =>
								runtimeCall(args, operationOptions(options, signal, Context.merge(base, caller)))
							);
			}
			return wrapped as EffectBoundManager<TKey, TResource>;
		}
	};
}

function wrapSelf(self: ResourceCallMap, methods: EffectResourceMethodMap): EffectResourceSelf {
	const wrapped: EffectResourceSelf = {};
	for (const kind of methodKinds(methods)) {
		const call = self[kind];
		if (!call) {
			continue;
		}
		const definition = methods[kind] as { readonly query?: unknown };
		const optionOnly = (kind === 'get' || kind === 'list' || kind === 'delete') && definition.query === undefined;
		const runtimeCall = (...args: readonly unknown[]) =>
			liftKernel((signal, context) =>
				optionOnly
					? call(operationOptions(args[0] as EffectOperationOptions | undefined, signal, context))
					: call(args[0], operationOptions(args[1] as EffectOperationOptions | undefined, signal, context))
			);
		wrapped[kind] = runtimeCall as never;
	}
	return wrapped;
}

async function lowerResourceHandler(
	effect: Effect.Effect<unknown, SyncError, unknown>,
	ctx: ResourceHandlerContext
): Promise<SyncResult<unknown, SyncError>> {
	// Plain callers have no Effect Context; Effect callers pass theirs through the private environment.
	const context = contextFromEnvironment<unknown>(ctx.environment, Context.empty() as Context.Context<unknown>);
	const result = await lowerEffect(effect, context, ctx.signal);
	return result.isErr() ? result : ok(wrapSourceCommit(result.value, context, ctx.signal));
}

function wrapSourceCommit<TValue>(value: TValue, context: Context.Context<unknown>, signal: AbortSignal) {
	if (
		!Predicate.isObject(value) ||
		!Predicate.isObject(value.sourceCommit) ||
		!Predicate.isFunction(value.sourceCommit.commit)
	) {
		return value;
	}
	const commit = value.sourceCommit.commit as (
		ctx: ResourceCommitContext
	) => Effect.Effect<ResourceCommitResult | void, SyncError, unknown>;
	return {
		...value,
		sourceCommit: {
			commit: (ctx: ResourceCommitContext) => {
				const commitSignal = executionForCommit(ctx)?.signal ?? signal;
				return lowerEffect(
					Effect.suspend(() => commit(ctx)),
					context,
					commitSignal
				);
			}
		}
	};
}

async function lowerEffect<A>(
	effect: Effect.Effect<A, SyncError, unknown>,
	context: Context.Context<unknown>,
	signal: AbortSignal
): Promise<SyncResult<A, SyncError>> {
	const exit = await Effect.runPromiseExitWith(context)(effect, { signal });
	if (Exit.isSuccess(exit)) {
		return ok(exit.value);
	}
	const failure = syncErrorFromCause(exit.cause);
	if (failure) {
		return err(failure);
	}
	throw new EffectHandlerDefect(exit.cause);
}

function liftKernel<A>(
	run: (signal: AbortSignal, context: Context.Context<unknown>) => Promise<SyncResult<A, SyncError>>
): Effect.Effect<A, SyncError, unknown> {
	return Effect.contextWith((context: Context.Context<unknown>) =>
		Effect.callback((resume, signal) => {
			const pending = run(signal, context).then(
				(result) => resume(resultToEffect(result)),
				(cause) => resume(Effect.die(cause))
			);
			return Effect.promise(() =>
				pending.then(
					() => undefined,
					() => undefined
				)
			);
		})
	);
}
function resultToEffect<A>(result: SyncResult<A, SyncError>): Effect.Effect<A, SyncError> {
	if (result.isOk()) {
		return Effect.succeed(result.value);
	}
	return result.error.cause instanceof EffectHandlerDefect
		? Effect.failCause(result.error.cause.effectCause)
		: Effect.fail(result.error);
}

function syncErrorFromCause(cause: Cause.Cause<SyncError>): SyncError | undefined {
	if (cause.reasons.length !== 1) {
		return undefined;
	}
	const reason = cause.reasons[0];
	return reason && Cause.isFailReason(reason) && isSyncError(reason.error) ? reason.error : undefined;
}

async function runHook<A, R>(
	effect: Effect.Effect<A, SyncError, R>,
	context: Context.Context<R>,
	signal?: AbortSignal
): Promise<A> {
	const exit = await Effect.runPromiseExitWith(context)(effect, signal ? { signal } : undefined);
	if (Exit.isSuccess(exit)) {
		return exit.value;
	}
	const failure = syncErrorFromCause(exit.cause);
	if (failure) {
		throw failure;
	}
	throw new EffectHandlerDefect(exit.cause);
}

async function subscribeScoped<R>(
	effect: Effect.Effect<void, SyncError, R | Scope.Scope>,
	context: Context.Context<R>,
	signal?: AbortSignal
): Promise<() => Promise<void>> {
	return runHook(
		Effect.flatMap(Scope.make(), (scope) =>
			Effect.as(
				Effect.onError(Scope.provide(scope)(effect), () => Scope.close(scope, Exit.void)),
				() => runHook(Scope.close(scope, Exit.void), Context.empty())
			)
		),
		context,
		signal
	);
}

function operationOptions(
	options: EffectOperationOptions | undefined,
	signal: AbortSignal,
	context: Context.Context<unknown>
): OperationOptions {
	return {
		...options,
		signal,
		environment: effectEnvironment(context)
	};
}

interface EffectEnvironment<R> {
	readonly [EffectEnvironmentTypeId]: true;
	readonly context: Context.Context<R>;
}

function effectEnvironment<R>(context: Context.Context<R>): EffectEnvironment<R> {
	return { [EffectEnvironmentTypeId]: true, context };
}

function contextFromEnvironment<R, TEnvironment = unknown>(
	environment: TEnvironment,
	fallback: Context.Context<R>
): Context.Context<R> {
	// Only private branded envelopes can restore a caller Context.
	const value = environment as EffectEnvironment<R> | undefined;
	return value?.[EffectEnvironmentTypeId] === true ? value.context : fallback;
}

class EffectHandlerDefect extends Error {
	readonly effectCause: Cause.Cause<SyncError>;

	constructor(effectCause: Cause.Cause<SyncError>) {
		const squashed = Cause.squash(effectCause);
		super(squashed instanceof Error ? squashed.message : String(squashed));
		this.effectCause = effectCause;
	}
}

function webHandler(
	request: HttpServerRequest.HttpServerRequest,
	handler: (request: Request) => Promise<Response>
): Effect.Effect<HttpServerResponse.HttpServerResponse> {
	return Effect.gen(function* () {
		const controller = new AbortController();
		const webRequest = yield* Effect.orDie(HttpServerRequest.toWeb(request, { signal: controller.signal }));
		const response = yield* Effect.callback<Response>((resume, signal) => {
			const abort = () => controller.abort();
			signal.addEventListener('abort', abort, { once: true });
			const pending = Promise.resolve()
				.then(() => handler(webRequest))
				.then(
					(response) => resume(Effect.succeed(response)),
					(cause) => resume(Effect.die(cause))
				)
				.finally(() => signal.removeEventListener('abort', abort));
			return Effect.promise(() =>
				pending.then(
					() => undefined,
					() => undefined
				)
			);
		});
		return HttpServerResponse.fromWeb(response);
	});
}
