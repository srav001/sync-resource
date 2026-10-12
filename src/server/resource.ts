import { isRecord, type ValueRecord } from '../shared/guards.ts';
import {
	normalizeSyncError,
	syncError,
	type AnySchema,
	type CostMetric,
	type InferSchemaOutput,
	type SyncError
} from '../shared/index.ts';
import { isSyncSignal } from '../shared/protocol.ts';
import { err, ok, type SyncResult } from '../shared/result.ts';
import { callHook } from './hook.ts';
import type {
	Awaitable,
	MethodType,
	OperationExecution,
	OperationOptions,
	ResourceCommit,
	ResourceCommitContext,
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
} from './types.ts';
import { parseSchema } from './validator.ts';

export type MethodKind = 'get' | 'list' | 'add' | 'mutate' | 'delete';

export function methodKinds(methods: ResourceMethodMap): MethodKind[] {
	return Object.keys(methods) as MethodKind[];
}

// Manager and self calls of a get, list or delete without a query schema take only the options argument.
export function takesOptionsOnly(
	kind: MethodKind,
	definition: { readonly kind: MethodKind; readonly query?: unknown } | undefined
): boolean {
	return (kind === 'get' || kind === 'list' || kind === 'delete') && definition?.query === undefined;
}

type NoExtraKeys<TValue, TAllowed> = TValue & Record<Exclude<keyof TValue, keyof TAllowed>, never>;

type QueryHandlerArgs<TParams extends object, TQuery, TSelf> = [TQuery] extends [never]
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
type MutateBatchItem<TQuery, TInput> = [TQuery] extends [never]
	? { readonly input: TInput }
	: { readonly query: TQuery; readonly input: TInput };
type DeleteBatchItem<TQuery> = [TQuery] extends [never] ? {} : { readonly query: TQuery };

export interface GetMethodInput<TParams extends object, TQuery, TOutput, TSelf> {
	readonly output: AnySchema<TOutput>;
	readonly query?: AnySchema<TQuery>;
	readonly input?: never;
	handler(this: void, args: QueryHandlerArgs<TParams, TQuery, TSelf>): ResourceHandlerResult<TOutput>;
}

export interface ListMethodInput<TParams extends object, TQuery, TOutput, TSelf> {
	readonly query?: AnySchema<TQuery>;
	readonly output: AnySchema<TOutput>;
	readonly input?: never;
	handler(this: void, args: QueryHandlerArgs<TParams, TQuery, TSelf>): ResourceHandlerResult<TOutput>;
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
	handler(this: void, args: QueryHandlerArgs<TParams, TQuery, TSelf>): ResourceHandlerResult<TOutput>;
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
	handler(this: void, args: QueryHandlerArgs<TParams, TQuery, unknown>): ResourceHandlerResult<TOutput>;
}

export interface ListMethodDefinition<TParams extends object, TQuery, TOutput> {
	readonly kind: 'list';
	readonly query?: AnySchema<TQuery>;
	readonly output: AnySchema<TOutput>;
	handler(this: void, args: QueryHandlerArgs<TParams, TQuery, unknown>): ResourceHandlerResult<TOutput>;
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
	handler(this: void, args: QueryHandlerArgs<TParams, TQuery, unknown>): ResourceHandlerResult<TOutput>;
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

type RuntimeDefinition = AnyResourceMethodDefinition;

export type BatchResourceHandlerResult<TOutput> = Awaitable<
	SyncResult<SyncBatchOutput<ResourceHandlerOk<TOutput>>, SyncError>
>;

interface ExecutionState {
	readonly exec: ResourceExecution;
	readonly cleanup: () => void;
	getAbortError(): SyncError;
}

export interface InternalOperationOptions extends OperationOptions {
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

	for (const kind of methodKinds(runtimeMethods)) {
		const definition = runtimeMethods[kind];
		if (definition) {
			calls[kind] = <TArgs>(args: TArgs, callOptions?: OperationOptions) =>
				executeMethod(params, runtimeMethods, definition, args, callOptions, options);
		}
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

async function executeMethod<TArgs>(
	paramsSchema: AnySchema<object>,
	methods: RuntimeDefinitions,
	definition: RuntimeDefinition,
	args: TArgs,
	options: OperationOptions | undefined,
	resourceOptions: ResourceOptions | undefined
): Promise<SyncResult<ResourceHandlerOk<unknown> | SyncBatchOutput<ResourceHandlerOk<unknown>>, SyncError>> {
	if (!Array.isArray(args) || !isWriteMethodKind(definition.kind)) {
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

// Every step reports its own failure through the telemetry and error hooks here, once.
async function runOperation<TOutput extends Committable>(
	definition: RuntimeDefinition,
	options: OperationOptions | undefined,
	resourceOptions: ResourceOptions | undefined,
	run: (execution: ExecutionState) => Promise<SyncResult<TOutput, SyncError>>
): Promise<SyncResult<TOutput, SyncError>> {
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
		const result = await run(execution);
		if (result.isErr()) {
			await reportResourceError(resourceOptions, context, result.error);
			return result;
		}
		await callHook(() => resourceOptions?.telemetry?.success?.(context, result.value.metrics ?? []));
		return result;
	} catch (cause) {
		const errorValue = execution.exec.signal.aborted ? execution.getAbortError() : normalizeSyncError(cause);
		await reportResourceError(resourceOptions, context, errorValue);
		return err(errorValue);
	} finally {
		execution.cleanup();
	}
}

function executeBatch(
	paramsSchema: AnySchema<object>,
	definition: RuntimeDefinitionWithBatchHandler,
	args: readonly unknown[],
	options: OperationOptions | undefined,
	resourceOptions: ResourceOptions | undefined
): Promise<SyncResult<SyncBatchOutput<ResourceHandlerOk<unknown>>, SyncError>> {
	return runOperation<SyncBatchOutput<ResourceHandlerOk<unknown>>>(
		definition,
		options,
		resourceOptions,
		async (execution) => {
			const parsed = parseBatchArgs(paramsSchema, definition, args);
			if (parsed.isErr()) {
				return parsed;
			}
			if (execution.exec.signal.aborted) {
				return err(execution.getAbortError());
			}
			const handlerResult = await definition.batchHandler({
				params: parsed.value.params,
				items: parsed.value.items as never,
				ctx: createHandlerContext(execution.exec)
			});
			if (handlerResult.isErr()) {
				return handlerResult;
			}
			const outputResult = validateBatchOutput(definition.output, handlerResult.value);
			if (outputResult.isErr()) {
				return outputResult;
			}
			return commitSourceIfNeeded(outputResult.value, options, execution.exec);
		}
	);
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

		const itemResult = parseMethodArgs(definition, parsedArgs.value);
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

// The input and query a handler receives, validated against the method's schemas.
function parseMethodArgs(definition: RuntimeDefinition, args: ValueRecord): SyncResult<ValueRecord, SyncError> {
	const parsed: { input?: unknown; query?: unknown } = {};
	if (definition.kind === 'add' || definition.kind === 'mutate') {
		const inputResult = parseSchema(definition.input, args.input, 'input');
		if (inputResult.isErr()) {
			return inputResult;
		}
		parsed.input = inputResult.value;
	}
	if (definition.kind !== 'add' && definition.query) {
		const queryResult = parseSchema(definition.query, args.query, 'query');
		if (queryResult.isErr()) {
			return queryResult;
		}
		parsed.query = queryResult.value;
	}
	return ok(parsed);
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

function executeSingle<TArgs>(
	paramsSchema: AnySchema<object>,
	methods: RuntimeDefinitions,
	definition: RuntimeDefinition,
	args: TArgs,
	options: OperationOptions | undefined,
	resourceOptions: ResourceOptions | undefined
): Promise<SyncResult<ResourceHandlerOk<unknown>, SyncError>> {
	return runOperation<ResourceHandlerOk<unknown>>(definition, options, resourceOptions, async (execution) => {
		const parsedArgs = parseArgs(args);
		if (parsedArgs.isErr()) {
			return parsedArgs;
		}

		const paramsResult = parseSchema(paramsSchema, parsedArgs.value.params, 'params');
		if (paramsResult.isErr()) {
			return paramsResult;
		}

		if (execution.exec.signal.aborted) {
			return err(execution.getAbortError());
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
			return handlerResult;
		}

		const envelopeResult = normalizeHandlerOutput(definition.output, handlerResult.value);
		if (envelopeResult.isErr()) {
			return envelopeResult;
		}

		return commitSourceIfNeeded(envelopeResult.value, options, execution.exec);
	});
}

interface Committable {
	readonly sourceCommit?: ResourceCommit;
	readonly metrics?: readonly CostMetric[];
}

export interface CommittedSource<TValue> {
	readonly value: TValue;
	readonly syncPersisted: boolean;
}

const executionByCommitContext = new WeakMap<ResourceCommitContext, OperationExecution>();

export function executionForCommit(context: ResourceCommitContext): OperationExecution | undefined {
	return executionByCommitContext.get(context);
}

// The write is committed here unless the manager defers it to commit together with its sync record.
export async function commitSource<TValue extends Committable>(
	value: TValue,
	context: ResourceCommitContext,
	execution: OperationExecution
): Promise<SyncResult<CommittedSource<TValue>, SyncError>> {
	if (!value.sourceCommit) {
		return ok({ value, syncPersisted: false });
	}

	executionByCommitContext.set(context, execution);
	// Plain commits observe cancellation through this property, which keeps the public record shape unchanged.
	Object.defineProperty(context, 'signal', { enumerable: false, value: execution.signal });
	const result = await value.sourceCommit.commit(context);
	if (result.isErr()) {
		return result;
	}

	return ok({
		value: { ...value, metrics: mergeMetrics(value.metrics, result.value?.metrics), sourceCommit: undefined },
		syncPersisted: result.value?.syncPersisted === true
	});
}

async function commitSourceIfNeeded<TValue extends Committable>(
	value: TValue,
	options: InternalOperationOptions | undefined,
	execution: ResourceExecution
): Promise<SyncResult<TValue, SyncError>> {
	if (options?.deferSourceCommit === true) {
		return ok(value);
	}
	const committed = await commitSource(value, {}, execution);
	return committed.isErr() ? committed : ok(committed.value.value);
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
	const parsed = parseMethodArgs(definition, args);
	if (parsed.isErr()) {
		return parsed;
	}
	return definition.handler({
		...parsed.value,
		params,
		ctx: createHandlerContext(exec),
		self: createResourceSelf(paramsSchema, methods, definition.kind, params, exec, resourceOptions)
	} as never);
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
		const definition = methods[kind];
		if (kind === currentKind || !definition) {
			continue;
		}
		const optionsOnly = takesOptionsOnly(kind, definition);
		self[kind] = <TArgs>(argsOrOptions?: TArgs, options?: OperationOptions) =>
			executeMethod(
				paramsSchema,
				methods,
				definition,
				mergeParams(params, optionsOnly ? undefined : argsOrOptions),
				mergeSelfOptions(exec, optionsOnly ? (argsOrOptions as OperationOptions | undefined) : options),
				resourceOptions
			);
	}
	return self;
}

export function mergeParams<TParams extends object, TArgs>(params: TParams, args: TArgs) {
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

export function isWriteMethodKind(kind: MethodKind): boolean {
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
