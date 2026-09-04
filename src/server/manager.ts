import { isCallable, isRecord, isString, isUnrefableTimer } from '../shared/guards.ts';
import { err, ok, type SyncResult } from '../shared/result.js';
import { encodeSseFrame, encodeSyncEnvelopeSseFrames, MAX_SSE_FRAME_BYTES } from '../shared/sse.ts';
import { stableStringify } from '../shared/stableJson.ts';
import { normalizeSyncError, syncError, syncErrorToHttpStatus, toSyncProtocolError, type SyncError } from './errors.js';
import { withCommitExecution } from './executionState.js';
import { combineAbortSignals, type MethodKind } from './resource.js';
import {
	publishSharedStreamEnvelope,
	registerSharedStreamManager,
	registerSharedStreamSubscription
} from './streamMultiplexer.js';
import type {
	AuthorizeHook,
	Awaitable,
	CostMetric,
	ManagerHttpHandlers,
	ManagerMutationRecord,
	ManagerOutbox,
	ManagerOutboxRead,
	ManagerRealtimeBus,
	ManagerSyncPersistence,
	ManagerTelemetry,
	ManagerTelemetryContext,
	MethodType,
	OperationMeta,
	OperationExecution,
	OperationOptions,
	ResourceCommitContext,
	ResourceHandlerOk,
	ResetManifest,
	ScopeValue,
	SyncBatchOutput,
	SyncChange,
	SyncEnvelope,
	SyncSignal,
	SyncHttpResult
} from './types.js';

const JSON_HEADERS = {
	'content-type': 'application/json; charset=utf-8',
	'cache-control': 'no-store'
};
// Body decoding never uses streaming state, so one decoder safely avoids per-request allocation.
const textDecoder = new TextDecoder();
const MAX_STREAM_BACKPRESSURE_QUEUE = 4096;
const MAX_MEMORY_MUTATION_RECORDS = 10_000;
const MEMORY_MUTATION_RECORD_TTL_MS = 60 * 60_000;
const unboundedExecution: OperationExecution = { signal: new AbortController().signal };

type ResourceParams<TResource> = TResource extends { readonly type: { readonly params: infer TParams extends object } }
	? TParams
	: never;
type ResourceMethodTypes<TResource> = TResource extends { readonly type: { readonly methods: infer TMethods } }
	? TMethods
	: never;

type ManagerMethods<TMethods> = {
	readonly [Key in keyof TMethods]: TMethods[Key] extends MethodType<infer TQuery, infer TInput, infer TOutput>
		? ManagerMethod<TQuery, TInput, TOutput>
		: never;
};

type ManagerMethod<TQuery, TInput, TOutput> = [TQuery] extends [never]
	? [TInput] extends [never]
		? (options?: OperationOptions) => Promise<SyncResult<TOutput, SyncError>>
		: ManagerWriteMethod<{ readonly input: TInput }, TOutput>
	: [TInput] extends [never]
		? (args: { readonly query: TQuery }, options?: OperationOptions) => Promise<SyncResult<TOutput, SyncError>>
		: ManagerWriteMethod<{ readonly query: TQuery; readonly input: TInput }, TOutput>;

export interface ManagerWriteMethod<TArgs, TOutput> {
	(args: TArgs, options?: OperationOptions): Promise<SyncResult<TOutput, SyncError>>;
	(args: readonly TArgs[], options?: OperationOptions): Promise<SyncResult<SyncBatchOutput<TOutput>, SyncError>>;
}

export type ManagerTypeFromResource<TKey extends string, TResource> = {
	readonly key: TKey;
	readonly params: ResourceParams<TResource>;
	readonly methods: ResourceMethodTypes<TResource>;
};

export type BoundManager<TKey extends string, TResource> = ManagerMethods<ResourceMethodTypes<TResource>> & {
	readonly key: TKey;
	readonly params: ResourceParams<TResource>;
	readonly scope: string;
};

export interface Manager<TKey extends string, TResource, TContext = undefined, TParams = ResourceParams<TResource>> {
	readonly key: TKey;
	readonly type: ManagerTypeFromResource<TKey, TResource>;
	readonly http: ManagerHttpHandlers<TContext, TParams>;
	bind(params: TParams, context?: TContext): BoundManager<TKey, TResource>;
}

export interface CreateManagerOptions<TKey extends string, TParams extends object, TResource, TContext> {
	readonly key: TKey;
	readonly resource: TResource;
	readonly authorize: AuthorizeHook<TContext | undefined, TParams>;
	scope(params: TParams): ScopeValue;
	readonly maxPayloadBytes?: number;
	readonly replayLimit?: number;
	readonly outbox?: ManagerOutbox;
	readonly persistence?: ManagerSyncPersistence;
	readonly realtimeBus?: ManagerRealtimeBus;
	readonly telemetry?: ManagerTelemetry;
	readonly stream?: {
		readonly heartbeatMs?: number;
		readonly idleTtlMs?: number;
		readonly maxConnectionsPerIp?: number;
		readonly maxEventBytes?: number;
		onScopeIdle?(scope: string): void;
	};
	handleError?(error: SyncError, context: ManagerTelemetryContext): Promise<void> | void;
}

interface RuntimeMethodDefinition {
	readonly kind: MethodKind;
	readonly query?: object;
}

interface RuntimeResource {
	readonly definition: {
		readonly methods: Partial<Record<MethodKind, RuntimeMethodDefinition>>;
	};
	readonly type: {
		readonly params: object;
		readonly methods: object;
	};
	readonly get?: RuntimeResourceMethod;
	readonly list?: RuntimeResourceMethod;
	readonly add?: RuntimeResourceMethod;
	readonly mutate?: RuntimeResourceMethod;
	readonly delete?: RuntimeResourceMethod;
}

interface ResourceLike {
	readonly definition: {
		readonly methods: Partial<Record<MethodKind, RuntimeMethodDefinition>>;
	};
	readonly type: {
		readonly params: object;
		readonly methods: object;
	};
}

type RuntimeResourceMethod = <TArgs>(
	args: TArgs,
	options?: OperationOptions
) => Promise<SyncResult<ResourceHandlerOk<unknown> | SyncBatchOutput<ResourceHandlerOk<unknown>>, SyncError>>;
type BoundRuntimeMethod = <TArgs>(args: TArgs, options?: OperationOptions) => Promise<SyncResult<unknown, SyncError>>;

interface ManagerExecutionSuccess {
	readonly output: unknown;
	readonly raw: ResourceHandlerOk<unknown> | SyncBatchOutput<ResourceHandlerOk<unknown>>;
	readonly envelope?: SyncEnvelope;
}

interface PublishOptions {
	readonly alreadyPersisted?: boolean;
	readonly transient?: boolean;
	readonly execution?: OperationExecution;
}

type PublishEnvelope = (envelope: SyncEnvelope, options?: PublishOptions) => Promise<void>;

interface InternalOperationOptions extends OperationOptions {
	readonly deferSourceCommit?: boolean;
}

interface DeferredSourceCommitResult {
	readonly raw: ResourceHandlerOk<unknown> | SyncBatchOutput<ResourceHandlerOk<unknown>>;
	readonly syncPersisted: boolean;
}

interface MutationIdentity {
	readonly id: string;
	readonly argsHash: string;
}

interface InFlightManagerMutation {
	readonly method: string;
	readonly argsHash: string;
	readonly promise: Promise<SyncResult<ManagerExecutionSuccess, SyncError>>;
}

interface BusSubscriptionEntry {
	readonly controller: AbortController;
	owners: number;
	readonly ready: Promise<(() => Awaitable<void>) | undefined>;
	closing?: Promise<void>;
}

interface RuntimeManagerOptions<TContext, TParams extends object> {
	readonly key: string;
	readonly authorize: AuthorizeHook<TContext | undefined, TParams>;
	scope(params: TParams): ScopeValue;
	readonly replayLimit?: number;
	readonly outbox?: ManagerOutbox;
	readonly persistence: ManagerSyncPersistence;
	readonly inFlightMutations: Map<string, InFlightManagerMutation>;
	readonly realtimeBus?: ManagerRealtimeBus;
	readonly telemetry?: ManagerTelemetry;
	readonly stream?: {
		readonly heartbeatMs?: number;
		readonly idleTtlMs?: number;
		readonly maxConnectionsPerIp?: number;
		readonly maxEventBytes?: number;
		onScopeIdle?(scope: string): void;
	};
	handleError?(error: SyncError, context: ManagerTelemetryContext): Promise<void> | void;
}

export function manager<TKey extends string, TResource extends ResourceLike, TContext = undefined>(
	options: CreateManagerOptions<TKey, ResourceParams<TResource>, TResource, TContext>
): Manager<TKey, TResource, TContext, ResourceParams<TResource>> {
	const runtimeResource: RuntimeResource = options.resource as never;
	const runtimeMethods = runtimeResource.definition.methods;
	const persistence = resolveManagerPersistence(options.persistence, options.outbox);
	const inFlightMutations = new Map<string, InFlightManagerMutation>();
	const cursorSeed = createCursorSeed();
	const managerOptions = {
		...options,
		persistence,
		inFlightMutations
	};
	let cursorIndex = 0;
	const subscribers = new Map<string, Set<(envelope: SyncEnvelope) => void>>();
	const busSubscriptions = new Map<string, BusSubscriptionEntry>();
	const sharedBusLeases = new Map<string, () => void>();

	registerSharedStreamManager({
		key: options.key,
		...options.stream,
		onScopeIdle(scope) {
			releaseSharedBusSubscription(scope);
			prunePersistenceScope(persistence, scope);
			options.stream?.onScopeIdle?.(scope);
		},
		handleError(errorValue, context) {
			return reportManagerError(managerOptions, { ...context, ...unboundedExecution }, errorValue);
		}
	});

	return {
		key: options.key,
		type: {
			key: options.key,
			params: undefined as never,
			methods: undefined as never
		},
		http: createHttpHandlers(managerOptions),
		bind(params: ResourceParams<TResource>, context?: TContext) {
			return createBoundManager(managerOptions, params, context);
		}
	};

	function nextCursor(): string {
		cursorIndex += 1;
		return `${options.key}:${cursorSeed}:${Date.now()}:${cursorIndex}`;
	}

	function createBoundManager(
		managerOptions: RuntimeManagerOptions<TContext, ResourceParams<TResource>>,
		params: ResourceParams<TResource>,
		context?: TContext
	): BoundManager<TKey, TResource> {
		const scope = encodeScope(options.key, options.scope(params));
		const methods: Partial<Record<MethodKind, BoundRuntimeMethod>> = {};

		if (runtimeMethods.get) {
			methods.get = <TArgs>(argsOrOptions?: TArgs, callOptions?: OperationOptions) => {
				const hasQuery = methodHasQuery(runtimeMethods.get);
				return executeManagerMethod(
					managerOptions,
					runtimeResource,
					'get',
					params,
					context,
					hasQuery ? argsOrOptions : undefined,
					hasQuery ? callOptions : (argsOrOptions as OperationOptions | undefined),
					nextCursor,
					recordAndPublish
				);
			};
		}

		if (runtimeMethods.list) {
			methods.list = <TArgs>(argsOrOptions?: TArgs, callOptions?: OperationOptions) => {
				const hasQuery = methodHasQuery(runtimeMethods.list);
				return executeManagerMethod(
					managerOptions,
					runtimeResource,
					'list',
					params,
					context,
					hasQuery ? argsOrOptions : undefined,
					hasQuery ? callOptions : (argsOrOptions as OperationOptions | undefined),
					nextCursor,
					recordAndPublish
				);
			};
		}

		if (runtimeMethods.add) {
			methods.add = <TArgs>(args: TArgs, callOptions?: OperationOptions) =>
				executeManagerMethod(
					managerOptions,
					runtimeResource,
					'add',
					params,
					context,
					args,
					callOptions,
					nextCursor,
					recordAndPublish
				);
		}

		if (runtimeMethods.mutate) {
			methods.mutate = <TArgs>(args: TArgs, callOptions?: OperationOptions) =>
				executeManagerMethod(
					managerOptions,
					runtimeResource,
					'mutate',
					params,
					context,
					args,
					callOptions,
					nextCursor,
					recordAndPublish
				);
		}

		if (runtimeMethods.delete) {
			methods.delete = <TArgs>(argsOrOptions?: TArgs, callOptions?: OperationOptions) => {
				const hasQuery = methodHasQuery(runtimeMethods.delete);
				return executeManagerMethod(
					managerOptions,
					runtimeResource,
					'delete',
					params,
					context,
					hasQuery ? argsOrOptions : undefined,
					hasQuery ? callOptions : (argsOrOptions as OperationOptions | undefined),
					nextCursor,
					recordAndPublish
				);
			};
		}

		return {
			...methods,
			key: options.key,
			params,
			scope
		} as BoundManager<TKey, TResource>;
	}

	function createHttpHandlers(
		managerOptions: RuntimeManagerOptions<TContext, ResourceParams<TResource>> & {
			readonly maxPayloadBytes?: number;
		}
	): ManagerHttpHandlers<TContext, ResourceParams<TResource>> {
		const handlers: ManagerHttpHandlers<TContext, ResourceParams<TResource>> = {};

		if (runtimeMethods.get) {
			handlers.get = (args) =>
				executeHttpMethod(managerOptions, runtimeResource, 'get', args, nextCursor, recordAndPublish);
		}

		if (runtimeMethods.list) {
			handlers.list = (args) =>
				executeHttpMethod(managerOptions, runtimeResource, 'list', args, nextCursor, recordAndPublish);
		}

		if (runtimeMethods.add) {
			handlers.add = (args) =>
				executeHttpMethod(managerOptions, runtimeResource, 'add', args, nextCursor, recordAndPublish);
		}

		if (runtimeMethods.mutate) {
			handlers.mutate = (args) =>
				executeHttpMethod(managerOptions, runtimeResource, 'mutate', args, nextCursor, recordAndPublish);
		}

		if (runtimeMethods.delete) {
			handlers.delete = (args) =>
				executeHttpMethod(managerOptions, runtimeResource, 'delete', args, nextCursor, recordAndPublish);
		}

		handlers.connect = async (args) => {
			const scope = encodeScope(managerOptions.key, managerOptions.scope(args.params));
			const execution = operationExecution(
				mergeOperationOptions(operationOptionsFromRequest(args.request), args.options)
			);
			const telemetryContext = {
				manager: managerOptions.key,
				method: 'connect',
				scope,
				...execution
			};
			const authResult = await authorize(managerOptions, args.context, args.params, telemetryContext);
			if (authResult.isErr()) {
				return jsonResponse(syncHttpError(authResult.error), syncErrorToHttpStatus(authResult.error));
			}

			const releaseBus = await acquireBusSubscription(managerOptions, scope, execution.signal);
			if (execution.signal.aborted) {
				releaseBus?.();
				const errorValue = syncError('aborted', 'Sync operation was aborted.');
				return jsonResponse(syncHttpError(errorValue), syncErrorToHttpStatus(errorValue));
			}
			const subscriptionResult = await registerSharedStreamSubscription({
				request: args.request,
				managerKey: managerOptions.key,
				scope,
				afterCursor: readAfterCursor(args.request),
				replayLimit: managerOptions.replayLimit ?? 1000,
				persistence,
				nextCursor
			});
			if (subscriptionResult.isErr()) {
				releaseBus?.();
				return jsonResponse(
					syncHttpError(subscriptionResult.error),
					syncErrorToHttpStatus(subscriptionResult.error)
				);
			}
			retainSharedBusSubscription(scope, releaseBus);

			return jsonResponse({
				isOk: true,
				isError: false,
				value: {
					subscribed: true,
					managerKey: managerOptions.key,
					scope
				}
			});
		};

		handlers.events = async (args) => {
			const scope = encodeScope(managerOptions.key, managerOptions.scope(args.params));
			const execution = operationExecution(
				mergeOperationOptions(operationOptionsFromRequest(args.request), args.options)
			);
			const telemetryContext = {
				manager: managerOptions.key,
				method: 'events',
				scope,
				...execution
			};
			const authResult = await authorize(managerOptions, args.context, args.params, telemetryContext);
			if (authResult.isErr()) {
				return jsonResponse(syncHttpError(authResult.error), syncErrorToHttpStatus(authResult.error));
			}

			let cancelStream = (): void => {};
			const stream = new ReadableStream<Uint8Array>(
				{
					async start(controller) {
						const afterCursor = readAfterCursor(args.request);
						const replayLimit = managerOptions.replayLimit ?? 1000;
						const maxEventBytes = managerOptions.stream?.maxEventBytes ?? 256 * 1024;
						const heartbeatMs = managerOptions.stream?.heartbeatMs ?? 60_000;
						const sentCursorLimit = Math.max(replayLimit * 2, 4096);
						const buffered: SyncEnvelope[] = [];
						const sentCursors = new Set<string>();
						const sentCursorOrder: string[] = [];
						let sentCursorStart = 0;
						let replaying = afterCursor !== undefined;
						let closed = false;
						let heartbeat: ReturnType<typeof setInterval> | undefined;
						let releaseBus: (() => void) | undefined;
						const streamController = new AbortController();
						const streamExecution = {
							...execution,
							signal: AbortSignal.any([execution.signal, streamController.signal])
						};
						let scopeSubscribers = subscribers.get(scope);
						if (!scopeSubscribers) {
							scopeSubscribers = new Set();
							subscribers.set(scope, scopeSubscribers);
						}
						const onAbort = () => close();

						function close(failure?: { readonly cause: unknown }): void {
							if (closed) {
								return;
							}
							closed = true;
							streamController.abort();
							execution.signal.removeEventListener('abort', onAbort);
							scopeSubscribers?.delete(send);
							if (scopeSubscribers?.size === 0) {
								subscribers.delete(scope);
							}
							releaseBus?.();
							releaseBus = undefined;
							if (heartbeat) {
								clearInterval(heartbeat);
								heartbeat = undefined;
							}
							try {
								if (failure) {
									controller.error(failure.cause);
								} else {
									controller.close();
								}
							} catch {}
						}
						cancelStream = close;

						function enqueue<TPayload>(eventName: string, payload: TPayload): void {
							if (closed) {
								return;
							}
							enqueueFrame(encodeSseFrame(eventName, payload));
						}

						function enqueueSync(envelope: SyncEnvelope): void {
							if (closed) {
								return;
							}
							const frames = encodeSyncEnvelopeSseFrames(envelope, { maxEnvelopeBytes: maxEventBytes });
							if (!frames) {
								close();
								return;
							}
							for (const frame of frames) {
								enqueueFrame(frame);
							}
						}

						function enqueueFrame(frame: Uint8Array): void {
							if (closed) {
								return;
							}
							if (frame.byteLength > MAX_SSE_FRAME_BYTES) {
								close();
								return;
							}
							if (
								controller.desiredSize !== null &&
								controller.desiredSize < -MAX_STREAM_BACKPRESSURE_QUEUE
							) {
								close();
								return;
							}
							try {
								controller.enqueue(frame);
							} catch {
								close();
							}
						}

						function send(envelope: SyncEnvelope): void {
							if (sentCursors.has(envelope.cursor)) {
								return;
							}
							if (replaying) {
								buffered.push(envelope);
								return;
							}
							rememberSentCursor(envelope.cursor);
							enqueueSync(envelope);
						}

						function rememberSentCursor(cursor: string): void {
							if (sentCursors.has(cursor)) {
								return;
							}
							sentCursors.add(cursor);
							sentCursorOrder.push(cursor);
							while (sentCursorOrder.length - sentCursorStart > sentCursorLimit) {
								const staleCursor = sentCursorOrder[sentCursorStart];
								sentCursorStart += 1;
								if (staleCursor !== undefined) {
									sentCursors.delete(staleCursor);
								}
							}
							if (sentCursorStart > 1024 && sentCursorStart * 2 > sentCursorOrder.length) {
								sentCursorOrder.splice(0, sentCursorStart);
								sentCursorStart = 0;
							}
						}

						try {
							scopeSubscribers.add(send);
							if (execution.signal.aborted) {
								close();
								return;
							}
							execution.signal.addEventListener('abort', onAbort, { once: true });
							const acquiredBus = await acquireBusSubscription(
								managerOptions,
								scope,
								streamExecution.signal
							);
							if (closed) {
								acquiredBus?.();
								return;
							}
							releaseBus = acquiredBus;
							enqueue('ready', { scope, after: afterCursor });
							const ping = { type: 'ping' as const, managerKey: managerOptions.key, scope, ts: 0 };
							heartbeat = setInterval(() => {
								// This direct stream is not the production browser transport, but the heartbeat keeps diagnostic streams bounded and observable.
								ping.ts = Date.now();
								enqueue('ping', ping);
							}, heartbeatMs);
							unrefTimer(heartbeat);

							if (afterCursor !== undefined) {
								const replay = await persistence.readAfter(
									scope,
									afterCursor,
									replayLimit,
									streamExecution
								);
								if (!replay.cursorFound && replay.retainedEnvelopeCount > 0) {
									const resetEnvelope = buildResetEnvelope(
										managerOptions.key,
										scope,
										afterCursor,
										nextCursor()
									);
									rememberSentCursor(resetEnvelope.cursor);
									enqueueSync(resetEnvelope);
								}
								for (const envelope of replay.envelopes) {
									rememberSentCursor(envelope.cursor);
									enqueueSync(envelope);
								}
							}

							replaying = false;
							for (const envelope of buffered) {
								if (!sentCursors.has(envelope.cursor)) {
									send(envelope);
								}
							}
						} catch (cause) {
							const cancelled = streamExecution.signal.aborted;
							close(cancelled ? undefined : { cause });
							if (cancelled) {
								return;
							}
							await reportManagerError(managerOptions, telemetryContext, normalizeSyncError(cause));
						}
					},
					cancel() {
						cancelStream();
					}
				},
				{ highWaterMark: 1 }
			);

			return new Response(stream, {
				headers: {
					'content-type': 'text/event-stream; charset=utf-8',
					'cache-control': 'no-store',
					connection: 'keep-alive'
				}
			});
		};

		return handlers;
	}

	async function recordAndPublish(envelope: SyncEnvelope, options?: PublishOptions): Promise<void> {
		if (options?.alreadyPersisted !== true && options?.transient !== true) {
			await persistence.append(envelope, options?.execution);
		}
		publishToLocalSubscribers(envelope);
		publishSharedStreamEnvelope(envelope);
		publishToRealtimeBusBestEffort(envelope);
	}

	async function publishToRealtimeBusBestEffort(envelope: SyncEnvelope): Promise<void> {
		const result = await callHook(() => managerOptions.realtimeBus?.publish(envelope));
		if (result.isErr()) {
			await reportRealtimeError('realtimeBus.publish', envelope.scope, result.error);
		}
	}

	function reportRealtimeError(method: string, scope: string, errorValue: SyncError): Promise<void> {
		return reportManagerError(
			managerOptions,
			{ manager: options.key, method, scope, ...unboundedExecution },
			errorValue
		);
	}

	function publishToLocalSubscribers(envelope: SyncEnvelope): void {
		const scopeSubscribers = subscribers.get(envelope.scope);
		if (!scopeSubscribers) {
			return;
		}

		for (const subscriber of scopeSubscribers) {
			subscriber(envelope);
		}
	}

	async function acquireBusSubscription(
		managerOptions: RuntimeManagerOptions<TContext, ResourceParams<TResource>>,
		scope: string,
		signal: AbortSignal
	): Promise<(() => void) | undefined> {
		const bus = managerOptions.realtimeBus;
		if (!bus || signal.aborted) {
			return undefined;
		}
		let entry = busSubscriptions.get(scope);
		if (entry?.closing) {
			await waitForInFlight(entry.closing, signal);
			return signal.aborted ? undefined : acquireBusSubscription(managerOptions, scope, signal);
		}
		if (!entry) {
			const controller = new AbortController();
			let created!: BusSubscriptionEntry;
			const ready = Promise.resolve()
				.then(() => bus.subscribe(scope, publishToLocalSubscribers, { signal: controller.signal }))
				.catch(async (cause: unknown) => {
					if (controller.signal.aborted) {
						return undefined;
					}
					if (busSubscriptions.get(scope) === created) {
						busSubscriptions.delete(scope);
					}
					await reportRealtimeError('events', scope, normalizeSyncError(cause));
					return undefined;
				});
			created = { controller, owners: 0, ready };
			entry = created;
			busSubscriptions.set(scope, entry);
		}
		entry.owners += 1;
		const unsubscribe = await waitForInFlight(entry.ready, signal);
		if (!unsubscribe || signal.aborted) {
			releaseBusOwner(scope, entry);
			return undefined;
		}
		let released = false;
		return () => {
			if (released) {
				return;
			}
			released = true;
			releaseBusOwner(scope, entry);
		};
	}

	function retainSharedBusSubscription(scope: string, release: (() => void) | undefined): void {
		if (!release) {
			return;
		}
		if (sharedBusLeases.has(scope)) {
			release();
			return;
		}
		sharedBusLeases.set(scope, release);
	}

	function releaseSharedBusSubscription(scope: string): void {
		const release = sharedBusLeases.get(scope);
		if (!release) {
			return;
		}
		sharedBusLeases.delete(scope);
		release();
	}

	function releaseBusOwner(scope: string, entry: BusSubscriptionEntry): void {
		entry.owners -= 1;
		if (entry.owners > 0 || entry.closing) {
			return;
		}
		entry.controller.abort();
		entry.closing = entry.ready
			.then(async (unsubscribe) => {
				if (!unsubscribe) {
					return;
				}
				const result = await callHook(unsubscribe);
				if (result.isErr()) {
					await reportRealtimeError('realtimeBus.unsubscribe', scope, result.error);
				}
			})
			.finally(() => {
				if (busSubscriptions.get(scope) === entry) {
					busSubscriptions.delete(scope);
				}
			});
	}
}

export class MemoryManagerOutbox implements ManagerSyncPersistence {
	private readonly scopes = new Map<string, SyncEnvelope[]>();
	private readonly mutations = new MemoryMutationRecordStore();
	private readonly maxEnvelopesPerScope: number;

	constructor(maxEnvelopesPerScope = 1000) {
		this.maxEnvelopesPerScope = maxEnvelopesPerScope;
	}

	append(envelope: SyncEnvelope): void {
		let envelopes = this.scopes.get(envelope.scope);
		if (!envelopes) {
			envelopes = [];
			this.scopes.set(envelope.scope, envelopes);
		}
		envelopes.push(envelope);
		const overflow = envelopes.length - this.maxEnvelopesPerScope;
		if (overflow > 0) {
			envelopes.splice(0, overflow);
		}
	}

	readAfter(scope: string, cursor: string, limit: number): ManagerOutboxRead {
		const envelopes = this.scopes.get(scope) ?? [];
		const cursorIndex = envelopes.findIndex((envelope) => envelope.cursor === cursor);
		if (cursorIndex < 0) {
			return {
				envelopes: [],
				cursorFound: false,
				retainedEnvelopeCount: envelopes.length
			};
		}
		return {
			envelopes: envelopes.slice(cursorIndex + 1, cursorIndex + 1 + limit),
			cursorFound: true,
			retainedEnvelopeCount: envelopes.length
		};
	}

	readMutation(scope: string, mutationId: string): ManagerMutationRecord | undefined {
		return this.mutations.read(scope, mutationId);
	}

	recordMutation(record: ManagerMutationRecord): void {
		if (record.envelope) {
			this.append(record.envelope);
		}
		this.mutations.record(record);
	}

	pruneScope(scope: string): void {
		this.mutations.pruneScope(scope);
	}
}

class MemoryMutationRecordStore {
	private readonly records = new Map<string, ManagerMutationRecord>();
	private readonly recordKeys: string[] = [];
	private recordKeyStart = 0;

	read(scope: string, mutationId: string): ManagerMutationRecord | undefined {
		this.prune(Date.now());
		return this.records.get(mutationKey(scope, mutationId));
	}

	record(record: ManagerMutationRecord): void {
		const key = mutationKey(record.scope, record.mutationId);
		if (!this.records.has(key)) {
			this.recordKeys.push(key);
		}
		this.records.set(key, record);
		this.prune(Date.now());
	}

	pruneScope(scope: string): void {
		let nextIndex = 0;
		for (let index = this.recordKeyStart; index < this.recordKeys.length; index += 1) {
			const key = this.recordKeys[index];
			if (!key) {
				continue;
			}
			const record = this.records.get(key);
			if (record?.scope === scope) {
				this.records.delete(key);
				continue;
			}
			this.recordKeys[nextIndex] = key;
			nextIndex += 1;
		}
		this.recordKeys.length = nextIndex;
		this.recordKeyStart = 0;
	}

	private prune(now: number): void {
		const expiresBefore = now - MEMORY_MUTATION_RECORD_TTL_MS;
		while (this.recordKeyStart < this.recordKeys.length) {
			const key = this.recordKeys[this.recordKeyStart];
			if (!key) {
				this.recordKeyStart += 1;
				continue;
			}
			const record = this.records.get(key);
			if (record && record.createdAt >= expiresBefore && this.records.size <= MAX_MEMORY_MUTATION_RECORDS) {
				break;
			}
			if (record) {
				this.records.delete(key);
			}
			this.recordKeyStart += 1;
		}
		this.compactRecordKeys();
	}

	private compactRecordKeys(): void {
		if (this.recordKeyStart > 1024 && this.recordKeyStart * 2 > this.recordKeys.length) {
			this.recordKeys.splice(0, this.recordKeyStart);
			this.recordKeyStart = 0;
		}
	}
}

export class MemoryManagerRealtimeBus implements ManagerRealtimeBus {
	private readonly subscribers = new Map<string, Set<(envelope: SyncEnvelope) => void>>();

	publish(envelope: SyncEnvelope): void {
		const scopeSubscribers = this.subscribers.get(envelope.scope);
		if (!scopeSubscribers) {
			return;
		}
		for (const subscriber of scopeSubscribers) {
			subscriber(envelope);
		}
	}

	subscribe(scope: string, onEnvelope: (envelope: SyncEnvelope) => void): () => void {
		let scopeSubscribers = this.subscribers.get(scope);
		if (!scopeSubscribers) {
			scopeSubscribers = new Set();
			this.subscribers.set(scope, scopeSubscribers);
		}
		scopeSubscribers.add(onEnvelope);
		return () => {
			scopeSubscribers?.delete(onEnvelope);
			if (scopeSubscribers?.size === 0) {
				this.subscribers.delete(scope);
			}
		};
	}
}

class OutboxBackedManagerPersistence implements ManagerSyncPersistence {
	private readonly mutations = new MemoryMutationRecordStore();
	private readonly outbox: ManagerOutbox;

	constructor(outbox: ManagerOutbox) {
		this.outbox = outbox;
	}

	append(envelope: SyncEnvelope, execution?: OperationExecution): Promise<void> | void {
		return this.outbox.append(envelope, execution);
	}

	readAfter(
		scope: string,
		cursor: string,
		limit: number,
		execution?: OperationExecution
	): Promise<ManagerOutboxRead> | ManagerOutboxRead {
		return this.outbox.readAfter(scope, cursor, limit, execution);
	}

	readMutation(scope: string, mutationId: string): ManagerMutationRecord | undefined {
		return this.mutations.read(scope, mutationId);
	}

	async recordMutation(record: ManagerMutationRecord, execution?: OperationExecution): Promise<void> {
		if (record.envelope) {
			await this.outbox.append(record.envelope, execution);
		}
		this.mutations.record(record);
	}

	pruneScope(scope: string): void {
		this.mutations.pruneScope(scope);
	}
}

function resolveManagerPersistence(
	persistence: ManagerSyncPersistence | undefined,
	outbox: ManagerOutbox | undefined
): ManagerSyncPersistence {
	if (persistence) {
		return persistence;
	}
	if (isManagerSyncPersistence(outbox)) {
		return outbox;
	}
	return new OutboxBackedManagerPersistence(outbox ?? new MemoryManagerOutbox());
}

function isManagerSyncPersistence(value: ManagerOutbox | undefined): value is ManagerSyncPersistence {
	return (
		value !== undefined &&
		'readMutation' in value &&
		isCallable(value.readMutation) &&
		'recordMutation' in value &&
		isCallable(value.recordMutation)
	);
}

function mutationKey(scope: string, mutationId: string): string {
	return `${scope}:${mutationId}`;
}

function createCursorSeed(): string {
	const cryptoValue = globalThis.crypto;
	if (cryptoValue?.randomUUID) {
		return cryptoValue.randomUUID();
	}
	return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

function withGeneratedMutationId(
	managerKey: string,
	method: MethodKind,
	options: OperationOptions | undefined
): OperationOptions | undefined {
	if (!isWriteMethod(method)) {
		return options;
	}
	if (options?.mutationId) {
		return options;
	}
	return {
		...options,
		mutationId: `${managerKey}:mutation:${Date.now()}:${Math.random().toString(36).slice(2)}`
	};
}

async function executeManagerMethod<TContext, TParams extends object, TArgs>(
	options: RuntimeManagerOptions<TContext, TParams>,
	resource: RuntimeResource,
	method: MethodKind,
	params: TParams,
	context: TContext | undefined,
	args: TArgs,
	callOptions: OperationOptions | undefined,
	nextCursor: () => string,
	publish: PublishEnvelope
): Promise<SyncResult<unknown, SyncError>> {
	const result = await executeManagerResourceMethod(
		options,
		resource,
		method,
		params,
		context,
		args,
		callOptions,
		nextCursor,
		publish
	);
	if (result.isErr()) {
		return result;
	}

	return ok(result.value.output);
}

async function executeManagerResourceMethod<TContext, TParams extends object, TArgs>(
	options: RuntimeManagerOptions<TContext, TParams>,
	resource: RuntimeResource,
	method: MethodKind,
	params: TParams,
	context: TContext | undefined,
	args: TArgs,
	callOptions: OperationOptions | undefined,
	nextCursor: () => string,
	publish: PublishEnvelope
): Promise<SyncResult<ManagerExecutionSuccess, SyncError>> {
	const scope = encodeScope(options.key, options.scope(params));
	const effectiveCallOptions = withGeneratedMutationId(options.key, method, callOptions);
	const execution = operationExecution(effectiveCallOptions);
	const telemetryContext = {
		manager: options.key,
		method,
		scope,
		mutationId: effectiveCallOptions?.mutationId,
		...execution
	};

	await callHook(() => options.telemetry?.start?.(telemetryContext));

	const authResult = await authorize(options, context, params, telemetryContext);
	if (authResult.isErr()) {
		return authResult;
	}

	try {
		const resourceMethod = resource[method];
		if (!resourceMethod) {
			const errorValue = syncError('bad_request', `Sync manager method is not defined: ${method}.`);
			await reportManagerError(options, telemetryContext, errorValue);
			return err(errorValue);
		}
		const executeResource = resourceMethod;

		const mutationIdentity =
			isWriteMethod(method) && effectiveCallOptions?.mutationId
				? {
						id: effectiveCallOptions.mutationId,
						argsHash: stableStringify(args)
					}
				: undefined;

		if (mutationIdentity) {
			const existing = await options.persistence.readMutation(scope, mutationIdentity.id, execution);
			if (existing) {
				if (existing.method !== method || existing.argsHash !== mutationIdentity.argsHash) {
					const errorValue = syncError('conflict', 'Mutation id was already used with different arguments.', {
						details: {
							mutationId: mutationIdentity.id,
							method
						}
					});
					await reportManagerError(options, telemetryContext, errorValue);
					return err(errorValue);
				}

				await callHook(() => options.telemetry?.success?.(telemetryContext, existing.metrics ?? []));
				return ok({
					output: existing.output,
					raw: {
						output: existing.output,
						changes: existing.envelope?.changes,
						syncCursor: existing.envelope?.cursor,
						metrics: existing.metrics
					},
					envelope: existing.envelope
				});
			}

			const inFlightKey = mutationKey(scope, mutationIdentity.id);
			const inFlight = options.inFlightMutations.get(inFlightKey);
			if (inFlight) {
				if (inFlight.method !== method || inFlight.argsHash !== mutationIdentity.argsHash) {
					const errorValue = syncError(
						'conflict',
						'Mutation id is already in flight with different arguments.',
						{
							details: {
								mutationId: mutationIdentity.id,
								method
							}
						}
					);
					await reportManagerError(options, telemetryContext, errorValue);
					return err(errorValue);
				}

				const inFlightResult = await waitForInFlight(inFlight.promise, execution.signal);
				if (!inFlightResult) {
					const errorValue = syncError('aborted', 'Sync operation was aborted.');
					await reportManagerError(options, telemetryContext, errorValue);
					return err(errorValue);
				}
				if (inFlightResult.isOk()) {
					await callHook(() =>
						options.telemetry?.success?.(telemetryContext, collectMetrics(inFlightResult.value.raw))
					);
				}
				return inFlightResult;
			}

			const promise = executeResourceAndPersist();
			options.inFlightMutations.set(inFlightKey, {
				method,
				argsHash: mutationIdentity.argsHash,
				promise
			});
			try {
				return await promise;
			} finally {
				if (options.inFlightMutations.get(inFlightKey)?.promise === promise) {
					options.inFlightMutations.delete(inFlightKey);
				}
			}
		}

		return executeResourceAndPersist();

		async function executeResourceAndPersist(): Promise<SyncResult<ManagerExecutionSuccess, SyncError>> {
			const resourceArgs = mergeParams(params, args);
			const result = await executeResource(resourceArgs, withDeferredSourceCommit(method, effectiveCallOptions));
			if (result.isErr()) {
				await reportManagerError(options, telemetryContext, result.error);
				return result;
			}

			const output = unwrapResourceValue(result.value);
			const envelope = buildEnvelope(
				options.key,
				scope,
				method,
				args,
				effectiveCallOptions,
				result.value,
				nextCursor
			);
			const mutationRecord = mutationIdentity
				? buildMutationRecord({
						scope,
						method,
						mutationIdentity,
						output,
						envelope,
						metrics: collectMetrics(result.value)
					})
				: undefined;
			const commitResult = await commitDeferredSourceWrite(
				result.value,
				withCommitExecution({ mutationRecord, envelope }, execution)
			);
			if (commitResult.isErr()) {
				await reportManagerError(options, telemetryContext, commitResult.error);
				return commitResult;
			}
			const raw = commitResult.value.raw;
			const metrics = collectMetrics(raw);
			if (envelope && isWriteMethod(method)) {
				if (mutationRecord && !commitResult.value.syncPersisted) {
					await options.persistence.recordMutation(
						{
							...mutationRecord,
							metrics
						},
						execution
					);
					await publish(envelope, { alreadyPersisted: true, execution });
				} else if (commitResult.value.syncPersisted) {
					await publish(envelope, { alreadyPersisted: true, execution });
				} else {
					await publish(envelope, { execution });
				}
			} else if (mutationRecord && !commitResult.value.syncPersisted) {
				await options.persistence.recordMutation(
					{
						...mutationRecord,
						metrics
					},
					execution
				);
			}

			const signalEnvelope = buildSignalEnvelope(options.key, scope, raw, nextCursor);
			if (signalEnvelope) {
				await publish(signalEnvelope, { transient: true, execution });
			}

			await callHook(() => options.telemetry?.success?.(telemetryContext, metrics));
			return ok({
				output,
				raw,
				envelope
			});
		}
	} catch (cause) {
		const errorValue = execution.signal.aborted
			? syncError('aborted', 'Sync operation was aborted.')
			: normalizeSyncError(cause);
		await reportManagerError(options, telemetryContext, errorValue);
		return err(errorValue);
	}
}

async function executeHttpMethod<TContext, TParams extends object>(
	options: RuntimeManagerOptions<TContext, TParams> & { readonly maxPayloadBytes?: number },
	resource: RuntimeResource,
	method: MethodKind,
	args: {
		readonly request: Request;
		readonly params: TParams;
		readonly context?: TContext;
		readonly options?: OperationOptions;
	},
	nextCursor: () => string,
	publish: PublishEnvelope
): Promise<Response> {
	const definition = resource.definition.methods[method];
	const parsedArgs = await parseHttpArgs(method, definition, args.request, options.maxPayloadBytes ?? 1024 * 1024);
	if (parsedArgs.isErr()) {
		return jsonResponse(syncHttpError(parsedArgs.error), syncErrorToHttpStatus(parsedArgs.error));
	}

	const operationOptions = mergeOperationOptions(operationOptionsFromRequest(args.request), args.options);
	const result = await executeManagerResourceMethod(
		options,
		resource,
		method,
		args.params,
		args.context,
		parsedArgs.value,
		operationOptions,
		nextCursor,
		publish
	);

	if (result.isErr()) {
		const body: SyncHttpResult<unknown> = {
			isOk: false,
			isError: true,
			error: toSyncProtocolError(result.error)
		};
		return jsonResponse(body, syncErrorToHttpStatus(result.error));
	}

	const body: SyncHttpResult<unknown> = {
		isOk: true,
		isError: false,
		value: result.value.output,
		envelope: result.value.envelope
	};
	return jsonResponse(body);
}

function mergeOperationOptions(base: OperationOptions, next: OperationOptions | undefined): OperationOptions {
	if (!next) {
		return base;
	}

	return {
		...base,
		...next,
		signal: base.signal ? combineAbortSignals(base.signal, next.signal) : next.signal,
		meta:
			base.meta || next.meta
				? {
						...base.meta,
						...next.meta
					}
				: undefined
	};
}

async function parseHttpArgs(
	method: MethodKind,
	definition: RuntimeMethodDefinition | undefined,
	request: Request,
	maxPayloadBytes: number
): Promise<SyncResult<unknown, SyncError>> {
	if (method === 'get' || method === 'list') {
		if (!methodHasQuery(definition)) {
			return ok(undefined);
		}
		return ok({
			query: parseQuery(request)
		});
	}

	const bodyResult = await readJsonBody(request, maxPayloadBytes);
	if (bodyResult.isErr()) {
		if (method === 'delete') {
			if (!methodHasQuery(definition)) {
				return ok(undefined);
			}
			return ok({
				query: parseQuery(request)
			});
		}
		return bodyResult;
	}

	return ok(bodyResult.value);
}

function parseQuery(request: Request): unknown {
	const url = new URL(request.url);
	const rawQuery = url.searchParams.get('query');
	if (rawQuery) {
		try {
			const value: unknown = JSON.parse(rawQuery);
			return value;
		} catch {
			return rawQuery;
		}
	}

	const query: Record<string, string> = {};
	for (const [key, value] of url.searchParams) {
		if (key === 'clientId' || key === 'mutationId') {
			continue;
		}
		query[key] = value;
	}
	return query;
}

async function readJsonBody(request: Request, maxPayloadBytes: number): Promise<SyncResult<unknown, SyncError>> {
	const contentLength = Number(request.headers.get('content-length') ?? '0');
	if (contentLength > maxPayloadBytes) {
		return err('payload_too_large', `Payload too large (>${maxPayloadBytes} bytes).`);
	}

	const body = request.body;
	if (!body) {
		return err('bad_request', 'Missing request body.');
	}

	try {
		const reader = body.getReader();
		const chunks: Uint8Array[] = [];
		let total = 0;
		try {
			while (true) {
				const chunk = await reader.read();
				if (chunk.done) {
					break;
				}
				if (!chunk.value || chunk.value.byteLength === 0) {
					continue;
				}
				// Count bytes while streaming so oversized bodies are rejected before building a full string.
				total += chunk.value.byteLength;
				if (total > maxPayloadBytes) {
					const cancelResult = await cancelReader(reader);
					return err('payload_too_large', `Payload too large (>${maxPayloadBytes} bytes).`, {
						details: cancelResult.isErr()
							? {
									cancelError: cancelResult.error.message
								}
							: undefined
					});
				}
				chunks.push(chunk.value);
			}
		} finally {
			reader.releaseLock();
		}

		if (total === 0) {
			return err('bad_request', 'Missing request body.');
		}

		const bytes = new Uint8Array(total);
		let offset = 0;
		for (const chunk of chunks) {
			bytes.set(chunk, offset);
			offset += chunk.byteLength;
		}

		const text = textDecoder.decode(bytes);
		const value: unknown = JSON.parse(text);
		return ok(value);
	} catch (cause) {
		return err('bad_request', 'Invalid JSON body.', { cause });
	}
}

async function cancelReader(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<SyncResult<void, SyncError>> {
	try {
		await reader.cancel();
		return ok();
	} catch (cause) {
		return err(normalizeSyncError(cause));
	}
}

function operationOptionsFromRequest(request: Request): OperationOptions {
	const url = new URL(request.url);
	const mutationId = request.headers.get('x-mutation-id') ?? url.searchParams.get('mutationId') ?? undefined;
	const clientId = request.headers.get('x-client-id') ?? url.searchParams.get('clientId') ?? undefined;
	const meta = readRequestMeta(request);
	if (!clientId && !meta) {
		return { mutationId, signal: request.signal };
	}

	const requestMeta = clientId ? { ...meta, clientId } : meta;
	return {
		mutationId,
		signal: request.signal,
		meta: requestMeta
	};
}

function readRequestMeta(request: Request): OperationMeta | undefined {
	const raw = request.headers.get('x-sync-meta');
	if (!raw) {
		return undefined;
	}

	try {
		const value = JSON.parse(raw);
		if (isRecord(value)) {
			return value;
		}
	} catch {}

	return undefined;
}

function readAfterCursor(request: Request): string | undefined {
	const url = new URL(request.url);
	return url.searchParams.get('after') ?? undefined;
}

async function authorize<TContext, TParams extends object>(
	options: RuntimeManagerOptions<TContext, TParams>,
	context: TContext | undefined,
	params: TParams,
	telemetryContext: ManagerTelemetryContext
): Promise<SyncResult<void, SyncError>> {
	try {
		const result = await options.authorize(context, params, telemetryContext);
		if (result === false) {
			const errorValue = syncError('forbidden', 'Forbidden.');
			await reportManagerError(options, telemetryContext, errorValue);
			return err(errorValue);
		}
		if (result && result !== true && result.isErr()) {
			await reportManagerError(options, telemetryContext, result.error);
			return result;
		}
		return ok();
	} catch (cause) {
		const errorValue = telemetryContext.signal.aborted
			? syncError('aborted', 'Sync operation was aborted.')
			: normalizeSyncError(cause);
		await reportManagerError(options, telemetryContext, errorValue);
		return err(errorValue);
	}
}

function mergeParams<TParams extends object, TArgs>(params: TParams, args: TArgs) {
	if (args === undefined) {
		return {
			params
		};
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

function unwrapResourceValue<TValue>(value: ResourceHandlerOk<TValue> | SyncBatchOutput<ResourceHandlerOk<TValue>>) {
	if (!isBatchOutput(value)) {
		return 'output' in value ? value.output : undefined;
	}

	const items = value.items.map((item) => {
		if (item.status === 'error') {
			return item;
		}

		return {
			index: item.index,
			status: item.status,
			value: 'output' in item.value ? item.value.output : undefined
		};
	});

	return {
		items,
		execution: value.execution
	};
}

function collectMetrics(
	value: ResourceHandlerOk<unknown> | SyncBatchOutput<ResourceHandlerOk<unknown>>
): readonly CostMetric[] {
	if (!isBatchOutput(value)) {
		return value.metrics ?? [];
	}

	const metrics: CostMetric[] = [...(value.metrics ?? [])];
	for (const item of value.items) {
		if (item.status === 'ok' && item.value.metrics) {
			metrics.push(...item.value.metrics);
		}
	}
	return metrics;
}

function collectSignals(
	value: ResourceHandlerOk<unknown> | SyncBatchOutput<ResourceHandlerOk<unknown>>
): readonly SyncSignal[] {
	if (!isBatchOutput(value)) {
		return value.signals ?? [];
	}

	const signals: SyncSignal[] = [];
	for (const item of value.items) {
		if (item.status === 'ok' && item.value.signals) {
			signals.push(...item.value.signals);
		}
	}
	return signals;
}

function buildMutationRecord(input: {
	readonly scope: string;
	readonly method: string;
	readonly mutationIdentity: MutationIdentity;
	readonly output: unknown;
	readonly envelope?: SyncEnvelope;
	readonly metrics: readonly CostMetric[];
}): ManagerMutationRecord {
	const now = Date.now();
	return {
		scope: input.scope,
		mutationId: input.mutationIdentity.id,
		method: input.method,
		argsHash: input.mutationIdentity.argsHash,
		output: input.output,
		envelope: input.envelope,
		metrics: input.metrics,
		status: input.envelope ? 'finalized' : 'acknowledged',
		createdAt: now,
		finalizedAt: input.envelope ? now : undefined
	};
}

function withDeferredSourceCommit(
	method: MethodKind,
	options: OperationOptions | undefined
): InternalOperationOptions | undefined {
	if (!isWriteMethod(method)) {
		return options;
	}

	return {
		...options,
		deferSourceCommit: true
	};
}

async function commitDeferredSourceWrite(
	value: ResourceHandlerOk<unknown> | SyncBatchOutput<ResourceHandlerOk<unknown>>,
	context: ResourceCommitContext
): Promise<SyncResult<DeferredSourceCommitResult, SyncError>> {
	if (isBatchOutput(value)) {
		if (value.sourceCommit) {
			const result = await value.sourceCommit.commit(context);
			if (result.isErr()) {
				return result;
			}

			return ok({
				raw: {
					...value,
					metrics: mergeMetrics(value.metrics, result.value?.metrics),
					sourceCommit: undefined
				},
				syncPersisted: result.value?.syncPersisted === true
			});
		}
		if (batchHasSourceCommit(value)) {
			return err(
				syncError(
					'bad_request',
					'Deferred source commits are not supported for looped batch writes. Use a resource-level atomic batch handler.'
				)
			);
		}
		return ok({
			raw: value,
			syncPersisted: false
		});
	}

	if (!value.sourceCommit) {
		return ok({
			raw: value,
			syncPersisted: false
		});
	}

	const result = await value.sourceCommit.commit(context);
	if (result.isErr()) {
		return result;
	}

	return ok({
		raw: {
			...value,
			metrics: mergeMetrics(value.metrics, result.value?.metrics),
			sourceCommit: undefined
		},
		syncPersisted: result.value?.syncPersisted === true
	});
}

function batchHasSourceCommit(value: SyncBatchOutput<ResourceHandlerOk<unknown>>): boolean {
	for (const item of value.items) {
		if (item.status === 'ok' && item.value.sourceCommit) {
			return true;
		}
	}
	return false;
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

function buildEnvelope<TArgs>(
	managerKey: string,
	scope: string,
	method: string,
	args: TArgs,
	options: OperationOptions | undefined,
	value: ResourceHandlerOk<unknown> | SyncBatchOutput<ResourceHandlerOk<unknown>>,
	nextCursor: () => string
): SyncEnvelope | undefined {
	const changes = collectChanges(method, args, value);
	if (changes.length === 0) {
		return undefined;
	}

	const replayCursor = getSyncCursor(value) ?? getChangesSyncCursor(changes);
	if (!replayCursor && !isWriteMethod(method)) {
		return undefined;
	}
	const cursor = replayCursor ?? nextCursor();
	const metrics = collectMetrics(value);
	return {
		managerKey,
		scope,
		cursor,
		sourceMutationId: options?.mutationId,
		sourceClientId: readStringField(options?.meta, 'clientId'),
		changes: replayCursor ? withPageSyncCursor(changes, replayCursor) : changes,
		reset: getResetManifest(changes),
		metrics: metrics.length > 0 ? metrics : undefined
	};
}

function buildSignalEnvelope(
	managerKey: string,
	scope: string,
	value: ResourceHandlerOk<unknown> | SyncBatchOutput<ResourceHandlerOk<unknown>>,
	nextCursor: () => string
): SyncEnvelope | undefined {
	const signals = collectSignals(value);
	if (signals.length === 0) {
		return undefined;
	}

	return {
		managerKey,
		scope,
		cursor: nextCursor(),
		changes: [],
		signals
	};
}

function collectChanges<TArgs>(
	method: string,
	args: TArgs,
	value: ResourceHandlerOk<unknown> | SyncBatchOutput<ResourceHandlerOk<unknown>>
): readonly SyncChange[] {
	if (!isBatchOutput(value)) {
		if (!('output' in value)) {
			return [];
		}
		return value.changes ?? inferChanges(method, args, value.output);
	}

	const changes: SyncChange[] = [];
	for (const item of value.items) {
		if (item.status !== 'ok' || !('output' in item.value)) {
			continue;
		}
		if (item.value.changes) {
			changes.push(...item.value.changes);
		} else {
			changes.push(
				...inferChanges(method, Array.isArray(args) ? args[item.index] : undefined, item.value.output)
			);
		}
	}
	return changes;
}

function inferChanges<TArgs, TOutput>(method: string, args: TArgs, output: TOutput): readonly SyncChange[] {
	switch (method) {
		case 'list': {
			if (isRecord(output) && Array.isArray(output.items)) {
				return [
					{
						type: 'pageLoaded',
						items: output.items,
						pageCursor: readString(output.pageCursor),
						syncCursor: readString(output.syncCursor)
					}
				];
			}
			return [];
		}
		case 'add': {
			const id = readStringField(output, 'id');
			if (!id) {
				return [];
			}
			return [
				{
					type: 'itemAdded',
					id,
					value: output
				}
			];
		}
		case 'mutate': {
			const id = readStringField(readRecordField(args, 'query'), 'id') ?? readStringField(output, 'id');
			if (!id) {
				return [];
			}
			return [
				{
					type: 'itemUpdated',
					id,
					patch: readRecordField(args, 'input'),
					value: output
				}
			];
		}
		case 'delete': {
			const id = readStringField(readRecordField(args, 'query'), 'id') ?? readStringField(output, 'id');
			if (!id) {
				return [];
			}
			return [
				{
					type: 'itemDeleted',
					id,
					tombstone: output
				}
			];
		}
		default:
			return [];
	}
}

function readRecordField<TValue>(value: TValue, key: string) {
	return isRecord(value) ? value[key] : undefined;
}

function readStringField<TValue>(value: TValue, key: string): string | undefined {
	return isRecord(value) ? readString(value[key]) : undefined;
}

function readString<TValue>(value: TValue): string | undefined {
	return isString(value) ? value : undefined;
}

function getSyncCursor(
	value: ResourceHandlerOk<unknown> | SyncBatchOutput<ResourceHandlerOk<unknown>>
): string | undefined {
	if (!isBatchOutput(value)) {
		return value.syncCursor;
	}

	for (let index = value.items.length - 1; index >= 0; index -= 1) {
		const item = value.items[index];
		if (item?.status === 'ok' && item.value.syncCursor) {
			return item.value.syncCursor;
		}
	}
	return undefined;
}

function getChangesSyncCursor(changes: readonly SyncChange[]): string | undefined {
	for (const syncChange of changes) {
		if (syncChange.type === 'pageLoaded' && syncChange.syncCursor) {
			return syncChange.syncCursor;
		}
	}
	return undefined;
}

function withPageSyncCursor(changes: readonly SyncChange[], cursor: string): readonly SyncChange[] {
	let needsCursor = false;
	for (const syncChange of changes) {
		if (syncChange.type === 'pageLoaded' && !syncChange.syncCursor) {
			needsCursor = true;
			break;
		}
	}
	if (!needsCursor) {
		return changes;
	}
	return changes.map((syncChange) => {
		if (syncChange.type !== 'pageLoaded' || syncChange.syncCursor) {
			return syncChange;
		}
		return {
			...syncChange,
			syncCursor: cursor
		};
	});
}

function getResetManifest(changes: readonly SyncChange[]): SyncEnvelope['reset'] {
	for (const syncChange of changes) {
		if (syncChange.type === 'reset') {
			return syncChange.manifest;
		}
	}
	return undefined;
}

function buildResetEnvelope(managerKey: string, scope: string, previousCursor: string, cursor: string): SyncEnvelope {
	const manifest: ResetManifest = {
		scope,
		reason: 'retention_gap',
		previousCursor,
		nextCursor: cursor
	};
	return {
		managerKey,
		scope,
		cursor,
		changes: [
			{
				type: 'reset',
				manifest
			}
		],
		reset: manifest
	};
}

function isWriteMethod(method: string): boolean {
	return method === 'add' || method === 'mutate' || method === 'delete';
}

function methodHasQuery(method: RuntimeMethodDefinition | undefined): boolean {
	return method?.query !== undefined;
}

function isBatchOutput(
	value: ResourceHandlerOk<unknown> | SyncBatchOutput<ResourceHandlerOk<unknown>>
): value is SyncBatchOutput<ResourceHandlerOk<unknown>> {
	return isRecord(value) && 'items' in value && Array.isArray(value.items);
}

export function encodeScope(key: string, scope: ScopeValue): string {
	const values = Array.isArray(scope) ? scope : [scope];
	return `${key}:${values.map((value) => encodeURIComponent(String(value))).join('/')}`;
}

async function reportManagerError<TContext, TParams extends object>(
	options: RuntimeManagerOptions<TContext, TParams>,
	context: ManagerTelemetryContext,
	errorValue: SyncError
): Promise<void> {
	await callHook(() => options.telemetry?.error?.(context, errorValue));
	await callHook(() => options.handleError?.(errorValue, context));
}

function operationExecution(options: OperationOptions | undefined): OperationExecution {
	return {
		signal: options?.signal ?? unboundedExecution.signal,
		environment: options?.environment
	};
}

function waitForInFlight<T>(promise: Promise<T>, signal: AbortSignal): Promise<T | undefined> {
	if (signal.aborted) {
		return Promise.resolve(undefined);
	}
	let onAbort!: () => void;
	const aborted = new Promise<undefined>((resolve) => {
		onAbort = () => resolve(undefined);
		signal.addEventListener('abort', onAbort, { once: true });
	});
	return Promise.race([promise, aborted]).finally(() => signal.removeEventListener('abort', onAbort));
}

async function callHook<TValue>(run: () => TValue): Promise<SyncResult<void, SyncError>> {
	try {
		await run();
		return ok();
	} catch (cause) {
		return err(normalizeSyncError(cause));
	}
}

function prunePersistenceScope(persistence: ManagerSyncPersistence, scope: string): void {
	if ('pruneScope' in persistence && isCallable(persistence.pruneScope)) {
		persistence.pruneScope(scope);
	}
}

function jsonResponse<TBody>(body: TBody, status = 200): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: JSON_HEADERS
	});
}

function syncHttpError(errorValue: SyncError): SyncHttpResult<unknown> {
	return {
		isOk: false,
		isError: true,
		error: toSyncProtocolError(errorValue)
	};
}

function unrefTimer(timer: ReturnType<typeof setInterval> | undefined): void {
	if (timer && isUnrefableTimer(timer)) {
		timer.unref();
	}
}
