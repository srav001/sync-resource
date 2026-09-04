import { isRecord, type ValueRecord } from '../shared/guards.ts';
import { isSyncSignal } from '../shared/index.ts';
import { err, ok, type SyncResult } from '../shared/result.js';
import { normalizeSyncError, syncError, type SyncError } from './errors.js';
import { withCommitExecution } from './executionState.js';
import type {
	AnySchema,
	Awaitable,
	CostMetric,
	InferSchemaOutput,
	MethodType,
	OperationOptions,
	ResourceExecution,
	ResourceHandlerContext,
	ResourceHandlerOk,
	ResourceHandlerResult,
	ResourceOk,
	ResourceSignalOk,
	ResourceStateOk,
	ResourceOptions,
	ResourceTelemetryContext,
	SyncBatchOutput
} from './types.js';
import { parseSchema } from './validator.js';

export type MethodKind = 'get' | 'list' | 'add' | 'mutate' | 'delete';

export function methodKinds(methods: ResourceMethodMap): MethodKind[] {
	return Object.keys(methods) as MethodKind[];
}

type NoExtraKeys<TValue, TAllowed> = TValue & Record<Exclude<keyof TValue, keyof TAllowed>, never>;

type GetHandlerArgs<TParams extends object, TQuery, TSelf> = [TQuery] extends [never]
	? { readonly params: TParams; readonly ctx: ResourceHandlerContext; readonly self: TSelf }
	: { readonly params: TParams; readonly query: TQuery; readonly ctx: ResourceHandlerContext; readonly self: TSelf };
type ListHandlerArgs<TParams extends object, TQuery, TSelf> = [TQuery] extends [never]
	? { readonly params: TParams; readonly ctx: ResourceHandlerContext; readonly self: TSelf }
	: { readonly params: TParams; readonly query: TQuery; readonly ctx: ResourceHandlerContext; readonly self: TSelf };
type MutateHandlerArgs<TParams extends object, TQuery, TInput, TSelf> = [TQuery] extends [never]
	? { readonly params: TParams; readonly input: TInput; readonly ctx: ResourceHandlerContext; readonly self: TSelf }
	: {
			readonly params: TParams;
			readonly query: TQuery;
			readonly input: TInput;
			readonly ctx: ResourceHandlerContext;
			readonly self: TSelf;
		};
type DeleteHandlerArgs<TParams extends object, TQuery, TSelf> = [TQuery] extends [never]
	? { readonly params: TParams; readonly ctx: ResourceHandlerContext; readonly self: TSelf }
	: { readonly params: TParams; readonly query: TQuery; readonly ctx: ResourceHandlerContext; readonly self: TSelf };
type MutateBatchItem<TQuery, TInput> = [TQuery] extends [never]
	? { readonly input: TInput }
	: { readonly query: TQuery; readonly input: TInput };
type DeleteBatchItem<TQuery> = [TQuery] extends [never] ? {} : { readonly query: TQuery };

export interface GetMethodInput<TParams extends object, TQuery, TOutput, TSelf> {
	readonly output: AnySchema<TOutput>;
	readonly query?: AnySchema<TQuery>;
	readonly input?: never;
	handler(this: void, args: GetHandlerArgs<TParams, TQuery, TSelf>): ResourceHandlerResult<TOutput>;
}

export interface ListMethodInput<TParams extends object, TQuery, TOutput, TSelf> {
	readonly query?: AnySchema<TQuery>;
	readonly output: AnySchema<TOutput>;
	readonly input?: never;
	handler(this: void, args: ListHandlerArgs<TParams, TQuery, TSelf>): ResourceHandlerResult<TOutput>;
}

export interface AddMethodInput<TParams extends object, TInput, TOutput, TSelf> {
	readonly input: AnySchema<TInput>;
	readonly output?: AnySchema<TOutput>;
	readonly query?: never;
	handler(
		this: void,
		args: {
			readonly params: TParams;
			readonly input: TInput;
			readonly ctx: ResourceHandlerContext;
			readonly self: TSelf;
		}
	): ResourceHandlerResult<TOutput>;
	batchHandler?(
		this: void,
		args: {
			readonly params: TParams;
			readonly items: readonly { readonly input: TInput }[];
			readonly ctx: ResourceHandlerContext;
		}
	): BatchResourceHandlerResult<TOutput>;
}

export interface MutateMethodInput<TParams extends object, TQuery, TInput, TOutput, TSelf> {
	readonly query?: AnySchema<TQuery>;
	readonly input: AnySchema<TInput>;
	readonly output?: AnySchema<TOutput>;
	handler(this: void, args: MutateHandlerArgs<TParams, TQuery, TInput, TSelf>): ResourceHandlerResult<TOutput>;
	batchHandler?(
		this: void,
		args: {
			readonly params: TParams;
			readonly items: readonly MutateBatchItem<TQuery, TInput>[];
			readonly ctx: ResourceHandlerContext;
		}
	): BatchResourceHandlerResult<TOutput>;
}

export interface DeleteMethodInput<TParams extends object, TQuery, TOutput, TSelf> {
	readonly query?: AnySchema<TQuery>;
	readonly output?: AnySchema<TOutput>;
	readonly input?: never;
	handler(this: void, args: DeleteHandlerArgs<TParams, TQuery, TSelf>): ResourceHandlerResult<TOutput>;
	batchHandler?(
		this: void,
		args: {
			readonly params: TParams;
			readonly items: readonly DeleteBatchItem<TQuery>[];
			readonly ctx: ResourceHandlerContext;
		}
	): BatchResourceHandlerResult<TOutput>;
}

export interface GetMethodDefinition<TParams extends object, TQuery, TOutput> {
	readonly kind: 'get';
	readonly query?: AnySchema<TQuery>;
	readonly output: AnySchema<TOutput>;
	handler(this: void, args: GetHandlerArgs<TParams, TQuery, unknown>): ResourceHandlerResult<TOutput>;
}

export interface ListMethodDefinition<TParams extends object, TQuery, TOutput> {
	readonly kind: 'list';
	readonly query?: AnySchema<TQuery>;
	readonly output: AnySchema<TOutput>;
	handler(this: void, args: ListHandlerArgs<TParams, TQuery, unknown>): ResourceHandlerResult<TOutput>;
}

export interface AddMethodDefinition<TParams extends object, TInput, TOutput> {
	readonly kind: 'add';
	readonly input: AnySchema<TInput>;
	readonly output?: AnySchema<TOutput>;
	handler(
		this: void,
		args: {
			readonly params: TParams;
			readonly input: TInput;
			readonly ctx: ResourceHandlerContext;
			readonly self: unknown;
		}
	): ResourceHandlerResult<TOutput>;
	batchHandler?(
		this: void,
		args: {
			readonly params: TParams;
			readonly items: readonly { readonly input: TInput }[];
			readonly ctx: ResourceHandlerContext;
		}
	): BatchResourceHandlerResult<TOutput>;
}

export interface MutateMethodDefinition<TParams extends object, TQuery, TInput, TOutput> {
	readonly kind: 'mutate';
	readonly query?: AnySchema<TQuery>;
	readonly input: AnySchema<TInput>;
	readonly output?: AnySchema<TOutput>;
	handler(this: void, args: MutateHandlerArgs<TParams, TQuery, TInput, unknown>): ResourceHandlerResult<TOutput>;
	batchHandler?(
		this: void,
		args: {
			readonly params: TParams;
			readonly items: readonly MutateBatchItem<TQuery, TInput>[];
			readonly ctx: ResourceHandlerContext;
		}
	): BatchResourceHandlerResult<TOutput>;
}

export interface DeleteMethodDefinition<TParams extends object, TQuery, TOutput> {
	readonly kind: 'delete';
	readonly query?: AnySchema<TQuery>;
	readonly output?: AnySchema<TOutput>;
	handler(this: void, args: DeleteHandlerArgs<TParams, TQuery, unknown>): ResourceHandlerResult<TOutput>;
	batchHandler?(
		this: void,
		args: {
			readonly params: TParams;
			readonly items: readonly DeleteBatchItem<TQuery>[];
			readonly ctx: ResourceHandlerContext;
		}
	): BatchResourceHandlerResult<TOutput>;
}

export type AnyResourceMethodDefinition<TParams extends object = object> =
	| GetMethodDefinition<TParams, unknown, unknown>
	| ListMethodDefinition<TParams, unknown, unknown>
	| AddMethodDefinition<TParams, unknown, unknown>
	| MutateMethodDefinition<TParams, unknown, unknown, unknown>
	| DeleteMethodDefinition<TParams, unknown, unknown>;

export type ResourceMethodMap = Partial<Record<MethodKind, { readonly kind: MethodKind }>>;

export interface MethodBuilder<TParams extends object> {
	get<TOutput>(
		definition: GetMethodInput<TParams, never, TOutput, unknown>
	): GetMethodDefinition<TParams, never, TOutput>;
	get<TQuery, TOutput>(
		definition: GetMethodInput<TParams, TQuery, TOutput, unknown> & {
			readonly query: AnySchema<TQuery>;
		}
	): GetMethodDefinition<TParams, TQuery, TOutput>;
	list<TOutput>(
		definition: ListMethodInput<TParams, never, TOutput, unknown>
	): ListMethodDefinition<TParams, never, TOutput>;
	list<TQuery, TOutput>(
		definition: ListMethodInput<TParams, TQuery, TOutput, unknown>
	): ListMethodDefinition<TParams, TQuery, TOutput>;
	add<TInput, TOutput = void>(
		definition: AddMethodInput<TParams, TInput, TOutput, unknown>
	): AddMethodDefinition<TParams, TInput, TOutput>;
	mutate<TInput, TOutput = void>(
		definition: MutateMethodInput<TParams, never, TInput, TOutput, unknown>
	): MutateMethodDefinition<TParams, never, TInput, TOutput>;
	mutate<TQuery, TInput, TOutput = void>(
		definition: MutateMethodInput<TParams, TQuery, TInput, TOutput, unknown>
	): MutateMethodDefinition<TParams, TQuery, TInput, TOutput>;
	delete<TOutput = void>(
		definition: DeleteMethodInput<TParams, never, TOutput, unknown>
	): DeleteMethodDefinition<TParams, never, TOutput>;
	delete<TQuery, TOutput = void>(
		definition: DeleteMethodInput<TParams, TQuery, TOutput, unknown>
	): DeleteMethodDefinition<TParams, TQuery, TOutput>;
}

export type ResourceMethodType<TMethod> =
	TMethod extends GetMethodDefinition<infer _TParams extends object, infer TQuery, infer TOutput>
		? MethodType<TQuery, never, TOutput>
		: TMethod extends ListMethodDefinition<infer _TParams extends object, infer TQuery, infer TOutput>
			? MethodType<TQuery, never, TOutput>
			: TMethod extends AddMethodDefinition<infer _TParams extends object, infer TInput, infer TOutput>
				? MethodType<never, TInput, TOutput>
				: TMethod extends MutateMethodDefinition<
							infer _TParams extends object,
							infer TQuery,
							infer TInput,
							infer TOutput
					  >
					? MethodType<TQuery, TInput, TOutput>
					: TMethod extends DeleteMethodDefinition<infer _TParams extends object, infer TQuery, infer TOutput>
						? MethodType<TQuery, never, TOutput>
						: never;

export type ResourceMethodTypes<TMethods extends ResourceMethodMap> = {
	readonly [Key in keyof TMethods]: ResourceMethodType<TMethods[Key]>;
};

export type GetResourceArgs<TParams extends object, TQuery> = [TQuery] extends [never]
	? { readonly params: TParams }
	: { readonly params: TParams; readonly query: TQuery };

export type ListResourceArgs<TParams extends object, TQuery> = [TQuery] extends [never]
	? { readonly params: TParams }
	: { readonly params: TParams; readonly query: TQuery };

export type AddResourceArgs<TParams extends object, TInput> = {
	readonly params: TParams;
	readonly input: TInput;
};

export type MutateResourceArgs<TParams extends object, TQuery, TInput> = [TQuery] extends [never]
	? { readonly params: TParams; readonly input: TInput }
	: { readonly params: TParams; readonly query: TQuery; readonly input: TInput };

export type DeleteResourceArgs<TParams extends object, TQuery> = [TQuery] extends [never]
	? { readonly params: TParams }
	: { readonly params: TParams; readonly query: TQuery };

export interface ResourceWriteMethod<TArgs, TOutput> {
	(args: TArgs, options?: OperationOptions): Promise<SyncResult<ResourceOk<TOutput>, SyncError>>;
	(
		args: readonly TArgs[],
		options?: OperationOptions
	): Promise<SyncResult<SyncBatchOutput<ResourceOk<TOutput>>, SyncError>>;
}

export type ResourceMethod<TParams extends object, TMethod> =
	TMethod extends GetMethodDefinition<infer _TParams extends object, infer TQuery, infer TOutput>
		? (
				args: GetResourceArgs<TParams, TQuery>,
				options?: OperationOptions
			) => Promise<SyncResult<ResourceOk<TOutput>, SyncError>>
		: TMethod extends ListMethodDefinition<infer _TParams extends object, infer TQuery, infer TOutput>
			? (
					args: ListResourceArgs<TParams, TQuery>,
					options?: OperationOptions
				) => Promise<SyncResult<ResourceOk<TOutput>, SyncError>>
			: TMethod extends AddMethodDefinition<infer _TParams extends object, infer TInput, infer TOutput>
				? ResourceWriteMethod<AddResourceArgs<TParams, TInput>, TOutput>
				: TMethod extends MutateMethodDefinition<
							infer _TParams extends object,
							infer TQuery,
							infer TInput,
							infer TOutput
					  >
					? ResourceWriteMethod<MutateResourceArgs<TParams, TQuery, TInput>, TOutput>
					: TMethod extends DeleteMethodDefinition<infer _TParams extends object, infer TQuery, infer TOutput>
						? ResourceWriteMethod<DeleteResourceArgs<TParams, TQuery>, TOutput>
						: never;

export type ResourceMethods<TParams extends object, TMethods extends ResourceMethodMap> = {
	readonly [Key in keyof TMethods]: ResourceMethod<TParams, TMethods[Key]>;
};

export interface ResourceDefinition<TParams extends object, TMethods extends ResourceMethodMap> {
	readonly params: AnySchema<TParams>;
	readonly methods: TMethods;
	readonly options?: ResourceOptions;
}

export type Resource<TParams extends object, TMethods extends ResourceMethodMap> = ResourceMethods<
	TParams,
	TMethods
> & {
	readonly definition: ResourceDefinition<TParams, TMethods>;
	readonly type: {
		readonly params: TParams;
		readonly methods: ResourceMethodTypes<TMethods>;
	};
};

type RuntimeDefinition =
	| GetMethodDefinition<object, unknown, unknown>
	| ListMethodDefinition<object, unknown, unknown>
	| AddMethodDefinition<object, unknown, unknown>
	| MutateMethodDefinition<object, unknown, unknown, unknown>
	| DeleteMethodDefinition<object, unknown, unknown>;

export type BatchResourceHandlerResult<TOutput> = Awaitable<
	SyncResult<SyncBatchOutput<ResourceHandlerOk<TOutput>>, SyncError>
>;

interface ExecutionState {
	readonly exec: ResourceExecution;
	readonly cleanup: () => void;
	getAbortError(): SyncError;
}

interface InternalOperationOptions extends OperationOptions {
	readonly deferSourceCommit?: boolean;
}

type RuntimeDefinitionWithBatchHandler =
	| (AddMethodDefinition<object, unknown, unknown> & {
			batchHandler(
				this: void,
				args: {
					readonly params: object;
					readonly items: readonly { readonly input: unknown }[];
					readonly ctx: ResourceHandlerContext;
				}
			): BatchResourceHandlerResult<unknown>;
	  })
	| (MutateMethodDefinition<object, unknown, unknown, unknown> & {
			batchHandler(
				this: void,
				args: {
					readonly params: object;
					readonly items: readonly { readonly query?: unknown; readonly input: unknown }[];
					readonly ctx: ResourceHandlerContext;
				}
			): BatchResourceHandlerResult<unknown>;
	  })
	| (DeleteMethodDefinition<object, unknown, unknown> & {
			batchHandler(
				this: void,
				args: {
					readonly params: object;
					readonly items: readonly { readonly query?: unknown }[];
					readonly ctx: ResourceHandlerContext;
				}
			): BatchResourceHandlerResult<unknown>;
	  });

interface ParsedBatchArgs {
	readonly params: object;
	readonly items: readonly ValueRecord[];
}

type RuntimeDefinitions = Partial<Record<MethodKind, RuntimeDefinition>>;
type RuntimeResourceCalls = Partial<Record<MethodKind, RuntimeResourceMethod>>;
type RuntimeResourceMethod = <TArgs>(
	args: TArgs,
	options?: OperationOptions
) => Promise<SyncResult<ResourceHandlerOk<unknown> | SyncBatchOutput<ResourceHandlerOk<unknown>>, SyncError>>;

const methodBuilder = {
	get<TParams extends object, TQuery, TOutput>(
		definition: GetMethodInput<TParams, TQuery, TOutput, unknown>
	): GetMethodDefinition<TParams, TQuery, TOutput> {
		return {
			kind: 'get',
			query: definition.query,
			output: definition.output,
			handler: definition.handler
		};
	},

	list<TParams extends object, TQuery, TOutput>(
		definition: ListMethodInput<TParams, TQuery, TOutput, unknown>
	): ListMethodDefinition<TParams, TQuery, TOutput> {
		return {
			kind: 'list',
			query: definition.query,
			output: definition.output,
			handler: definition.handler
		};
	},

	add<TParams extends object, TInput, TOutput>(
		definition: AddMethodInput<TParams, TInput, TOutput, unknown>
	): AddMethodDefinition<TParams, TInput, TOutput> {
		return {
			kind: 'add',
			input: definition.input,
			output: definition.output,
			handler: definition.handler,
			batchHandler: definition.batchHandler
		};
	},

	mutate<TParams extends object, TQuery, TInput, TOutput>(
		definition: MutateMethodInput<TParams, TQuery, TInput, TOutput, unknown>
	): MutateMethodDefinition<TParams, TQuery, TInput, TOutput> {
		return {
			kind: 'mutate',
			query: definition.query,
			input: definition.input,
			output: definition.output,
			handler: definition.handler,
			batchHandler: definition.batchHandler
		};
	},

	delete<TParams extends object, TQuery, TOutput>(
		definition: DeleteMethodInput<TParams, TQuery, TOutput, unknown>
	): DeleteMethodDefinition<TParams, TQuery, TOutput> {
		return {
			kind: 'delete',
			query: definition.query,
			output: definition.output,
			handler: definition.handler,
			batchHandler: definition.batchHandler
		};
	}
};

export function resource<TParamsSchema extends AnySchema<object>, const TMethods extends ResourceMethodMap>(
	params: TParamsSchema,
	factory: (method: MethodBuilder<InferSchemaOutput<TParamsSchema>>) => NoExtraKeys<TMethods, ResourceMethodMap>,
	options?: ResourceOptions
): Resource<InferSchemaOutput<TParamsSchema>, TMethods> {
	const methods = factory(methodBuilder as MethodBuilder<InferSchemaOutput<TParamsSchema>>);
	const runtimeMethods = methods as RuntimeDefinitions;
	const calls: RuntimeResourceCalls = {};

	const getDefinition = runtimeMethods.get;
	if (getDefinition) {
		calls.get = <TArgs>(args: TArgs, callOptions?: OperationOptions) =>
			executeSingle(params, runtimeMethods, getDefinition, args, callOptions, options);
	}

	const listDefinition = runtimeMethods.list;
	if (listDefinition) {
		calls.list = <TArgs>(args: TArgs, callOptions?: OperationOptions) =>
			executeSingle(params, runtimeMethods, listDefinition, args, callOptions, options);
	}

	const addDefinition = runtimeMethods.add;
	if (addDefinition) {
		calls.add = <TArgs>(args: TArgs, callOptions?: OperationOptions) =>
			executeWrite(params, runtimeMethods, addDefinition, args, callOptions, options);
	}

	const mutateDefinition = runtimeMethods.mutate;
	if (mutateDefinition) {
		calls.mutate = <TArgs>(args: TArgs, callOptions?: OperationOptions) =>
			executeWrite(params, runtimeMethods, mutateDefinition, args, callOptions, options);
	}

	const deleteDefinition = runtimeMethods.delete;
	if (deleteDefinition) {
		calls.delete = <TArgs>(args: TArgs, callOptions?: OperationOptions) =>
			executeWrite(params, runtimeMethods, deleteDefinition, args, callOptions, options);
	}

	return {
		...calls,
		definition: {
			params,
			methods,
			options
		},
		type: {
			params: undefined as never,
			methods: undefined as never
		}
	} as never;
}

async function executeWrite<TArgs>(
	paramsSchema: AnySchema<object>,
	methods: RuntimeDefinitions,
	definition: RuntimeDefinition,
	args: TArgs,
	options: OperationOptions | undefined,
	resourceOptions: ResourceOptions | undefined
): Promise<SyncResult<ResourceHandlerOk<unknown> | SyncBatchOutput<ResourceHandlerOk<unknown>>, SyncError>> {
	if (!Array.isArray(args)) {
		return executeSingle(paramsSchema, methods, definition, args, options, resourceOptions);
	}

	if (hasBatchHandler(definition)) {
		return executeBatch(paramsSchema, definition, args, options, resourceOptions);
	}

	const items = [];
	let okCount = 0;
	let errorCount = 0;
	const baseMutationId = options?.mutationId;

	for (let index = 0; index < args.length; index += 1) {
		const result = await executeSingle(
			paramsSchema,
			methods,
			definition,
			args[index],
			{
				...options,
				mutationId: baseMutationId ? `${baseMutationId}:${index}` : undefined
			},
			resourceOptions
		);

		if (result.isOk()) {
			okCount += 1;
			items.push({
				index,
				status: 'ok' as const,
				value: result.value
			});
		} else {
			errorCount += 1;
			items.push({
				index,
				status: 'error' as const,
				error: result.error
			});
		}
	}

	return ok({
		items,
		execution: {
			mode: 'loop',
			atomic: false,
			okCount,
			errorCount
		}
	});
}

async function executeBatch(
	paramsSchema: AnySchema<object>,
	definition: RuntimeDefinitionWithBatchHandler,
	args: readonly unknown[],
	options: OperationOptions | undefined,
	resourceOptions: ResourceOptions | undefined
): Promise<SyncResult<SyncBatchOutput<ResourceHandlerOk<unknown>>, SyncError>> {
	const execution = createExecution(options);
	const context = {
		resource: resourceOptions?.name ?? 'resource',
		method: definition.kind,
		mutationId: options?.mutationId,
		signal: execution.exec.signal,
		environment: execution.exec.environment
	};
	try {
		await callHook(() => resourceOptions?.telemetry?.start?.(context));

		const parsed = parseBatchArgs(paramsSchema, definition, args);
		if (parsed.isErr()) {
			await reportResourceError(resourceOptions, context, parsed.error);
			return parsed;
		}

		if (execution.exec.signal.aborted) {
			const abortError = execution.getAbortError();
			await reportResourceError(resourceOptions, context, abortError);
			return err(abortError);
		}

		const handlerResult = await callBatchHandler(
			definition,
			parsed.value.params,
			parsed.value.items,
			execution.exec
		);
		if (handlerResult.isErr()) {
			await reportResourceError(resourceOptions, context, handlerResult.error);
			return handlerResult;
		}

		const outputResult = validateBatchOutput(definition.output, handlerResult.value);
		if (outputResult.isErr()) {
			await reportResourceError(resourceOptions, context, outputResult.error);
			return outputResult;
		}

		const committedBatch = await commitBatchSourceIfNeeded(outputResult.value, options, execution.exec);
		if (committedBatch.isErr()) {
			await reportResourceError(resourceOptions, context, committedBatch.error);
			return committedBatch;
		}

		await callHook(() => resourceOptions?.telemetry?.success?.(context, committedBatch.value.metrics ?? []));
		return ok(committedBatch.value);
	} catch (cause) {
		const errorValue = execution.exec.signal.aborted ? execution.getAbortError() : normalizeSyncError(cause);
		await reportResourceError(resourceOptions, context, errorValue);
		return err(errorValue);
	} finally {
		execution.cleanup();
	}
}

function hasBatchHandler(definition: RuntimeDefinition): definition is RuntimeDefinitionWithBatchHandler {
	return 'batchHandler' in definition && definition.batchHandler !== undefined;
}

function parseBatchArgs(
	paramsSchema: AnySchema<object>,
	definition: RuntimeDefinitionWithBatchHandler,
	args: readonly unknown[]
): SyncResult<ParsedBatchArgs, SyncError> {
	const items: ValueRecord[] = [];
	let params: object | undefined;
	let paramsFingerprint: string | undefined;

	for (const item of args) {
		const parsedArgs = parseArgs(item);
		if (parsedArgs.isErr()) {
			return parsedArgs;
		}

		const paramsResult = parseSchema(paramsSchema, parsedArgs.value.params, 'params');
		if (paramsResult.isErr()) {
			return paramsResult;
		}

		const nextFingerprint = JSON.stringify(paramsResult.value);
		if (paramsFingerprint === undefined) {
			params = paramsResult.value;
			paramsFingerprint = nextFingerprint;
		} else if (paramsFingerprint !== nextFingerprint) {
			return err('bad_request', 'Batched sync method args must use the same params.');
		}

		const itemResult = parseBatchItem(definition, parsedArgs.value);
		if (itemResult.isErr()) {
			return itemResult;
		}
		items.push(itemResult.value);
	}

	if (!params) {
		return err('bad_request', 'Batched sync method args cannot be empty.');
	}

	return ok({
		params,
		items
	});
}

function parseBatchItem(
	definition: RuntimeDefinitionWithBatchHandler,
	args: ValueRecord
): SyncResult<ValueRecord, SyncError> {
	switch (definition.kind) {
		case 'add': {
			const inputResult = parseSchema(definition.input, args.input, 'input');
			if (inputResult.isErr()) {
				return inputResult;
			}
			return ok({
				input: inputResult.value
			});
		}
		case 'mutate': {
			const inputResult = parseSchema(definition.input, args.input, 'input');
			if (inputResult.isErr()) {
				return inputResult;
			}
			if (!definition.query) {
				return ok({
					input: inputResult.value
				});
			}
			const queryResult = parseSchema(definition.query, args.query, 'query');
			if (queryResult.isErr()) {
				return queryResult;
			}
			return ok({
				query: queryResult.value,
				input: inputResult.value
			});
		}
		case 'delete': {
			if (!definition.query) {
				return ok({});
			}
			const queryResult = parseSchema(definition.query, args.query, 'query');
			if (queryResult.isErr()) {
				return queryResult;
			}
			return ok({
				query: queryResult.value
			});
		}
	}
}

function callBatchHandler<TParams extends object>(
	definition: RuntimeDefinitionWithBatchHandler,
	params: TParams,
	items: readonly ValueRecord[],
	exec: ResourceExecution
): BatchResourceHandlerResult<unknown> {
	const context = createHandlerContext(exec);
	switch (definition.kind) {
		case 'add':
			return definition.batchHandler({
				params,
				items: items as readonly { readonly input: unknown }[],
				ctx: context
			});
		case 'mutate':
			return definition.batchHandler({
				params,
				items: items as readonly { readonly query?: unknown; readonly input: unknown }[],
				ctx: context
			});
		case 'delete':
			return definition.batchHandler({
				params,
				items: items as readonly { readonly query?: unknown }[],
				ctx: context
			});
	}
}

function validateBatchOutput(
	outputSchema: AnySchema<unknown> | undefined,
	value: SyncBatchOutput<ResourceHandlerOk<unknown>>
): SyncResult<SyncBatchOutput<ResourceHandlerOk<unknown>>, SyncError> {
	const items = [];
	let hasStateOutput = false;
	let hasSignalOutput = false;
	for (const item of value.items) {
		if (item.status === 'error') {
			items.push(item);
			continue;
		}

		const valueResult = normalizeStructuredHandlerOutput(outputSchema, item.value);
		if (valueResult.isErr()) {
			return valueResult;
		}
		hasStateOutput = hasStateOutput || 'output' in valueResult.value;
		hasSignalOutput = hasSignalOutput || 'signals' in valueResult.value;
		if (hasStateOutput && hasSignalOutput) {
			return err('validation', 'Batch resource outputs cannot mix state output and signals.');
		}

		items.push({
			...item,
			value: valueResult.value
		});
	}

	return ok({
		...value,
		items
	});
}

async function commitBatchSourceIfNeeded(
	value: SyncBatchOutput<ResourceHandlerOk<unknown>>,
	options: OperationOptions | undefined,
	execution: ResourceExecution
): Promise<SyncResult<SyncBatchOutput<ResourceHandlerOk<unknown>>, SyncError>> {
	if (!value.sourceCommit || shouldDeferSourceCommit(options)) {
		return ok(value);
	}

	const result = await value.sourceCommit.commit(withCommitExecution({}, execution));
	if (result.isErr()) {
		return result;
	}

	return ok({
		...value,
		metrics: mergeMetrics(value.metrics, result.value?.metrics),
		sourceCommit: undefined
	});
}

async function executeSingle<TArgs>(
	paramsSchema: AnySchema<object>,
	methods: Partial<Record<MethodKind, RuntimeDefinition>>,
	definition: RuntimeDefinition,
	args: TArgs,
	options: OperationOptions | undefined,
	resourceOptions: ResourceOptions | undefined
): Promise<SyncResult<ResourceHandlerOk<unknown>, SyncError>> {
	const execution = createExecution(options);
	const context = {
		resource: resourceOptions?.name ?? 'resource',
		method: definition.kind,
		mutationId: options?.mutationId,
		signal: execution.exec.signal,
		environment: execution.exec.environment
	};
	try {
		await callHook(() => resourceOptions?.telemetry?.start?.(context));

		const parsedArgs = parseArgs(args);
		if (parsedArgs.isErr()) {
			await reportResourceError(resourceOptions, context, parsedArgs.error);
			return parsedArgs;
		}

		const paramsResult = parseSchema(paramsSchema, parsedArgs.value.params, 'params');
		if (paramsResult.isErr()) {
			await reportResourceError(resourceOptions, context, paramsResult.error);
			return paramsResult;
		}

		if (execution.exec.signal.aborted) {
			const abortError = execution.getAbortError();
			await reportResourceError(resourceOptions, context, abortError);
			return err(abortError);
		}

		const handlerResult = await callHandler(
			paramsSchema,
			methods,
			definition,
			paramsResult.value,
			parsedArgs.value,
			execution.exec,
			resourceOptions
		);
		if (handlerResult.isErr()) {
			await reportResourceError(resourceOptions, context, handlerResult.error);
			return handlerResult;
		}

		const envelopeResult = normalizeHandlerOutput(definition.output, handlerResult.value);
		if (envelopeResult.isErr()) {
			await reportResourceError(resourceOptions, context, envelopeResult.error);
			return envelopeResult;
		}

		const committedEnvelope = await commitSourceIfNeeded(envelopeResult.value, options, execution.exec);
		if (committedEnvelope.isErr()) {
			await reportResourceError(resourceOptions, context, committedEnvelope.error);
			return committedEnvelope;
		}

		await callHook(() => resourceOptions?.telemetry?.success?.(context, committedEnvelope.value.metrics ?? []));
		return ok(committedEnvelope.value);
	} catch (cause) {
		const errorValue = execution.exec.signal.aborted ? execution.getAbortError() : normalizeSyncError(cause);
		await reportResourceError(resourceOptions, context, errorValue);
		return err(errorValue);
	} finally {
		execution.cleanup();
	}
}

async function commitSourceIfNeeded(
	value: ResourceHandlerOk<unknown>,
	options: OperationOptions | undefined,
	execution: ResourceExecution
): Promise<SyncResult<ResourceHandlerOk<unknown>, SyncError>> {
	if (!value.sourceCommit || shouldDeferSourceCommit(options)) {
		return ok(value);
	}

	const result = await value.sourceCommit.commit(withCommitExecution({}, execution));
	if (result.isErr()) {
		return result;
	}

	return ok({
		...value,
		metrics: mergeMetrics(value.metrics, result.value?.metrics),
		sourceCommit: undefined
	});
}

function shouldDeferSourceCommit(options: InternalOperationOptions | undefined): boolean {
	return options?.deferSourceCommit === true;
}

function mergeMetrics(
	first: readonly CostMetric[] | undefined,
	second: readonly CostMetric[] | undefined
): readonly CostMetric[] | undefined {
	if (!first || first.length === 0) {
		return second;
	}
	if (!second || second.length === 0) {
		return first;
	}
	return [...first, ...second];
}

function normalizeHandlerOutput<TValue>(
	outputSchema: AnySchema<unknown> | undefined,
	value: TValue
): SyncResult<ResourceHandlerOk<unknown>, SyncError> {
	if (isRecord(value)) {
		const record = value;
		if ('signals' in record) {
			return normalizeSignalOutput(record);
		}
		if ('output' in record && hasStateResourceMetadata(record)) {
			return normalizeStructuredHandlerOutput(outputSchema, record);
		}
	}

	if (!outputSchema) {
		return err('validation', 'Sync method output schema is required for state outputs.');
	}

	const directOutputResult = parseSchema(outputSchema, value, 'output');
	if (directOutputResult.isOk()) {
		return ok({
			output: directOutputResult.value
		});
	}

	if (!isRecord(value) || !('output' in value)) {
		return directOutputResult;
	}

	return normalizeStructuredHandlerOutput(outputSchema, value);
}

function normalizeStructuredHandlerOutput(
	outputSchema: AnySchema<unknown> | undefined,
	value: ResourceHandlerOk<unknown> | ValueRecord
): SyncResult<ResourceHandlerOk<unknown>, SyncError> {
	if ('signals' in value) {
		return normalizeSignalOutput(value);
	}

	if (!('output' in value)) {
		return err('validation', 'Resource state metadata requires an output value.');
	}
	if (!outputSchema) {
		return err('validation', 'Sync method output schema is required for state outputs.');
	}

	const outputResult = parseSchema(outputSchema, value.output, 'output');
	if (outputResult.isErr()) {
		return outputResult;
	}

	const normalized: ResourceStateOk<unknown> = {
		...value,
		output: outputResult.value
	};
	return ok(normalized);
}

function normalizeSignalOutput(
	value: ResourceHandlerOk<unknown> | ValueRecord
): SyncResult<ResourceSignalOk, SyncError> {
	if (
		'output' in value ||
		'changes' in value ||
		'pageCursor' in value ||
		'syncCursor' in value ||
		'metrics' in value ||
		'sourceCommit' in value
	) {
		return err('validation', 'Resource signal outputs cannot include state output or metadata.');
	}
	if (!Array.isArray(value.signals) || !value.signals.every(isSyncSignal)) {
		return err('validation', 'Resource signal output must include valid signals.');
	}

	return ok({
		signals: value.signals
	});
}

function hasStateResourceMetadata(value: ValueRecord): boolean {
	return (
		'changes' in value ||
		'pageCursor' in value ||
		'syncCursor' in value ||
		'metrics' in value ||
		'sourceCommit' in value
	);
}

function parseArgs<TArgs>(args: TArgs): SyncResult<ValueRecord, SyncError> {
	if (!isRecord(args)) {
		return err('bad_request', 'Sync method args must be an object.');
	}

	return ok(args);
}

async function callHandler<TParams extends object>(
	paramsSchema: AnySchema<TParams>,
	methods: RuntimeDefinitions,
	definition: RuntimeDefinition,
	params: TParams,
	args: ValueRecord,
	exec: ResourceExecution,
	resourceOptions: ResourceOptions | undefined
): Promise<SyncResult<unknown, SyncError>> {
	const context = createHandlerContext(exec);
	const self = createResourceSelf(paramsSchema, methods, definition.kind, params, exec, resourceOptions);
	switch (definition.kind) {
		case 'get': {
			if (!definition.query) {
				return (definition as GetMethodDefinition<object, never, unknown>).handler({
					params,
					ctx: context,
					self
				});
			}
			const queryResult = parseSchema(definition.query, args.query, 'query');
			if (queryResult.isErr()) {
				return queryResult;
			}
			return definition.handler({ params, query: queryResult.value, ctx: context, self });
		}
		case 'list': {
			if (!definition.query) {
				return (definition as ListMethodDefinition<object, never, unknown>).handler({
					params,
					ctx: context,
					self
				});
			}
			const queryResult = parseSchema(definition.query, args.query, 'query');
			if (queryResult.isErr()) {
				return queryResult;
			}
			return definition.handler({ params, query: queryResult.value, ctx: context, self });
		}
		case 'add': {
			const inputResult = parseSchema(definition.input, args.input, 'input');
			if (inputResult.isErr()) {
				return inputResult;
			}
			return definition.handler({ params, input: inputResult.value, ctx: context, self });
		}
		case 'mutate': {
			const inputResult = parseSchema(definition.input, args.input, 'input');
			if (inputResult.isErr()) {
				return inputResult;
			}
			if (!definition.query) {
				return (definition as MutateMethodDefinition<object, never, unknown, unknown>).handler({
					params,
					input: inputResult.value,
					ctx: context,
					self
				});
			}
			const queryResult = parseSchema(definition.query, args.query, 'query');
			if (queryResult.isErr()) {
				return queryResult;
			}
			return definition.handler({
				params,
				query: queryResult.value,
				input: inputResult.value,
				ctx: context,
				self
			});
		}
		case 'delete': {
			if (!definition.query) {
				return (definition as DeleteMethodDefinition<object, never, unknown>).handler({
					params,
					ctx: context,
					self
				});
			}
			const queryResult = parseSchema(definition.query, args.query, 'query');
			if (queryResult.isErr()) {
				return queryResult;
			}
			return definition.handler({ params, query: queryResult.value, ctx: context, self });
		}
	}
}

function createResourceSelf<TParams extends object>(
	paramsSchema: AnySchema<TParams>,
	methods: RuntimeDefinitions,
	currentKind: MethodKind,
	params: TParams,
	exec: ResourceExecution,
	resourceOptions: ResourceOptions | undefined
): RuntimeResourceCalls {
	const self: RuntimeResourceCalls = {};
	for (const kind of methodKinds(methods)) {
		if (kind === currentKind) {
			continue;
		}
		const definition = methods[kind];
		if (!definition) {
			continue;
		}
		if (isWriteMethodKind(kind)) {
			if (kind === 'delete' && !methodHasQuery(definition)) {
				self[kind] = <TArgs>(argsOrOptions?: TArgs) =>
					executeWrite(
						paramsSchema,
						methods,
						definition,
						mergeSelfArgs(params, undefined),
						mergeSelfOptions(exec, argsOrOptions as OperationOptions | undefined),
						resourceOptions
					);
				continue;
			}
			self[kind] = <TArgs>(args: TArgs, options?: OperationOptions) =>
				executeWrite(
					paramsSchema,
					methods,
					definition,
					mergeSelfArgs(params, args),
					mergeSelfOptions(exec, options),
					resourceOptions
				);
			continue;
		}
		self[kind] = <TArgs>(argsOrOptions?: TArgs, options?: OperationOptions) => {
			const hasQuery = methodHasQuery(definition);
			const args = hasQuery ? argsOrOptions : undefined;
			const callOptions = hasQuery ? options : (argsOrOptions as OperationOptions | undefined);
			return executeSingle(
				paramsSchema,
				methods,
				definition,
				mergeSelfArgs(params, args),
				mergeSelfOptions(exec, callOptions),
				resourceOptions
			);
		};
	}
	return self;
}

function mergeSelfArgs<TParams extends object, TArgs>(params: TParams, args: TArgs) {
	if (args === undefined) {
		return { params };
	}
	if (Array.isArray(args)) {
		return args.map((item) => {
			if (!isRecord(item)) {
				return item;
			}
			return {
				...item,
				params
			};
		});
	}
	if (!isRecord(args)) {
		return args;
	}
	return {
		...args,
		params
	};
}

function mergeSelfOptions(exec: ResourceExecution, options: OperationOptions | undefined): OperationOptions {
	return {
		...options,
		signal: combineAbortSignals(exec.signal, options?.signal),
		mutationId: options?.mutationId ?? exec.mutationId,
		meta:
			exec.meta || options?.meta
				? {
						...exec.meta,
						...options?.meta
					}
				: undefined,
		actor: options?.actor ?? exec.actor,
		environment: options?.environment ?? exec.environment
	};
}

export function combineAbortSignals(parent: AbortSignal, child: AbortSignal | undefined): AbortSignal {
	if (!child || child === parent) {
		return parent;
	}
	return AbortSignal.any([parent, child]);
}

function methodHasQuery(definition: RuntimeDefinition): boolean {
	return 'query' in definition && definition.query !== undefined;
}

function isWriteMethodKind(kind: MethodKind): boolean {
	return kind === 'add' || kind === 'mutate' || kind === 'delete';
}

function createHandlerContext(exec: ResourceExecution): ResourceHandlerContext {
	return {
		...exec,
		ok,
		error: err,
		syncError
	};
}

function createExecution(options: OperationOptions | undefined): ExecutionState {
	const controller = new AbortController();
	let timeoutReached = false;
	let timeout: ReturnType<typeof setTimeout> | undefined;

	const abortFromCaller = (): void => {
		controller.abort(options?.signal?.reason);
	};

	if (options?.signal?.aborted) {
		abortFromCaller();
	} else if (options?.signal) {
		options.signal.addEventListener('abort', abortFromCaller, { once: true });
	}

	if (options?.timeoutMs !== undefined) {
		timeout = setTimeout(() => {
			timeoutReached = true;
			controller.abort();
		}, options.timeoutMs);
	}

	return {
		exec: {
			signal: controller.signal,
			deadline: options?.timeoutMs === undefined ? undefined : Date.now() + options.timeoutMs,
			mutationId: options?.mutationId,
			meta: options?.meta,
			actor: options?.actor,
			environment: options?.environment
		},
		cleanup() {
			if (timeout) {
				clearTimeout(timeout);
			}
			options?.signal?.removeEventListener('abort', abortFromCaller);
		},
		getAbortError() {
			if (timeoutReached) {
				return syncError('timeout', 'Sync operation timed out.');
			}
			return syncError('aborted', 'Sync operation was aborted.');
		}
	};
}

async function reportResourceError(
	options: ResourceOptions | undefined,
	context: ResourceTelemetryContext,
	errorValue: SyncError
): Promise<void> {
	await callHook(() => options?.telemetry?.error?.(context, errorValue));
	await callHook(() => options?.handleError?.(errorValue, context));
}

async function callHook<TValue>(run: () => TValue): Promise<SyncResult<void, SyncError>> {
	try {
		await run();
		return ok();
	} catch (cause) {
		return err(normalizeSyncError(cause));
	}
}
