import type {
	CostMetric,
	SyncChange,
	SyncEnvelope,
	SyncError,
	SyncErrorCode,
	SyncRecoveryMetadata,
	SyncSignal
} from '../shared/index.ts';
import type { SyncErr, SyncOk, SyncResult } from '../shared/result.js';

export type {
	CostMetric,
	ItemAddedChange,
	ItemDeletedChange,
	ItemUpdatedChange,
	PageLoadedChange,
	ResetChange,
	ResetManifest,
	SyncChange,
	SyncEnvelope,
	SyncError,
	SyncErrorCode,
	SyncErrorRecovery,
	SyncHttpError,
	SyncHttpOk,
	SyncHttpResult,
	SyncProtocolError,
	SyncRecoveryMetadata,
	SyncRecoverySource,
	SyncResetReason,
	SyncSignal,
	AnySchema,
	InferSchemaOutput,
	StandardSchema,
	StandardSchemaValidationResult,
	Validator
} from '../shared/index.ts';

export type Awaitable<TValue> = TValue | Promise<TValue>;

export interface OperationMeta<TValue = unknown> {
	readonly [key: string]: TValue;
}

export interface SyncActor {
	readonly id: string;
	readonly type?: string;
}

export interface OperationOptions {
	readonly signal?: AbortSignal;
	readonly timeoutMs?: number;
	readonly mutationId?: string;
	readonly meta?: OperationMeta;
	readonly actor?: SyncActor;
	/** Opaque server-owned execution state. Request metadata must never populate this field. */
	readonly environment?: unknown;
}

export interface OperationExecution {
	readonly signal: AbortSignal;
	readonly environment?: unknown;
}

export interface ResourceExecution extends OperationExecution {
	readonly deadline?: number;
	readonly mutationId?: string;
	readonly meta?: OperationMeta;
	readonly actor?: SyncActor;
}

export interface ResourceHandlerContext extends ResourceExecution {
	ok(): SyncOk<void>;
	ok<TValue>(value: TValue): SyncOk<TValue>;
	error(errorValue: SyncError): SyncErr<never, SyncError>;
	syncError(
		code: SyncErrorCode,
		message: string,
		options?: {
			readonly details?: unknown;
			readonly cause?: unknown;
			readonly recovery?: SyncRecoveryMetadata;
		}
	): SyncError;
}

export interface ResourceCommitContext {
	readonly mutationRecord?: ManagerMutationRecord;
	readonly envelope?: SyncEnvelope;
}

export interface ResourceCommitResult {
	readonly metrics?: readonly CostMetric[];
	readonly syncPersisted?: boolean;
}

export interface ResourceCommit {
	commit(context: ResourceCommitContext): Awaitable<SyncResult<ResourceCommitResult | void, SyncError>>;
}

export interface ResourceStateOk<TOutput> {
	readonly output: TOutput;
	readonly changes?: readonly SyncChange[];
	readonly signals?: never;
	readonly pageCursor?: string;
	readonly syncCursor?: string;
	readonly metrics?: readonly CostMetric[];
	readonly sourceCommit?: ResourceCommit;
}

export interface ResourceSignalOk {
	readonly signals: readonly SyncSignal[];
	readonly output?: never;
	readonly changes?: never;
	readonly pageCursor?: never;
	readonly syncCursor?: never;
	readonly metrics?: never;
	readonly sourceCommit?: never;
}

export type ResourceOk<TOutput> = ResourceStateOk<TOutput>;
export type ResourceHandlerOk<TOutput> = ResourceStateOk<TOutput> | ResourceSignalOk;

export interface SyncBatchOkItem<TValue> {
	readonly index: number;
	readonly status: 'ok';
	readonly value: TValue;
}

export interface SyncBatchErrorItem {
	readonly index: number;
	readonly status: 'error';
	readonly error: SyncError;
}

export type SyncBatchItem<TValue> = SyncBatchOkItem<TValue> | SyncBatchErrorItem;

export interface SyncBatchExecution {
	readonly mode: 'transaction' | 'bulk' | 'loop';
	readonly atomic: boolean;
	readonly okCount: number;
	readonly errorCount: number;
}

export interface SyncBatchOutput<TValue> {
	readonly items: readonly SyncBatchItem<TValue>[];
	readonly execution: SyncBatchExecution;
	readonly metrics?: readonly CostMetric[];
	readonly sourceCommit?: ResourceCommit;
}

export interface ManagerOutbox {
	append(envelope: SyncEnvelope, execution?: OperationExecution): Awaitable<void>;
	readAfter(
		scope: string,
		cursor: string,
		limit: number,
		execution?: OperationExecution
	): Awaitable<ManagerOutboxRead>;
}

export interface ManagerOutboxRead {
	readonly envelopes: readonly SyncEnvelope[];
	readonly cursorFound: boolean;
	readonly retainedEnvelopeCount: number;
}

export interface ManagerRealtimeBus {
	publish(envelope: SyncEnvelope): Awaitable<void>;
	subscribe(
		scope: string,
		onEnvelope: (envelope: SyncEnvelope) => void,
		execution?: OperationExecution
	): Awaitable<() => Awaitable<void>>;
}

export type ManagerMutationStatus = 'acknowledged' | 'finalized';

export interface ManagerMutationRecord {
	readonly scope: string;
	readonly mutationId: string;
	readonly method: string;
	readonly argsHash: string;
	readonly output: unknown;
	readonly envelope?: SyncEnvelope;
	readonly metrics?: readonly CostMetric[];
	readonly status: ManagerMutationStatus;
	readonly createdAt: number;
	readonly finalizedAt?: number;
}

export interface ManagerSyncPersistence extends ManagerOutbox {
	readMutation(
		scope: string,
		mutationId: string,
		execution?: OperationExecution
	): Awaitable<ManagerMutationRecord | undefined>;
	recordMutation(record: ManagerMutationRecord, execution?: OperationExecution): Awaitable<void>;
}

export interface ManagerHttpHandlerArgs<TContext, TParams> {
	readonly request: Request;
	readonly params: TParams;
	readonly context?: TContext;
	readonly options?: OperationOptions;
}

export interface ManagerHttpHandlers<TContext, TParams> {
	get?(args: ManagerHttpHandlerArgs<TContext, TParams>): Promise<Response>;
	list?(args: ManagerHttpHandlerArgs<TContext, TParams>): Promise<Response>;
	add?(args: ManagerHttpHandlerArgs<TContext, TParams>): Promise<Response>;
	mutate?(args: ManagerHttpHandlerArgs<TContext, TParams>): Promise<Response>;
	delete?(args: ManagerHttpHandlerArgs<TContext, TParams>): Promise<Response>;
	connect?(args: ManagerHttpHandlerArgs<TContext, TParams>): Promise<Response>;
	events?(args: ManagerHttpHandlerArgs<TContext, TParams>): Promise<Response>;
}

export interface MethodType<TQuery, TInput, TOutput> {
	readonly query: TQuery;
	readonly input: TInput;
	readonly output: TOutput;
}

export interface ManagerType<TKey extends string, TParams, TMethods> {
	readonly key: TKey;
	readonly params: TParams;
	readonly methods: TMethods;
}

export type ResourceHandlerResult<TOutput> = Awaitable<SyncResult<ResourceHandlerOk<TOutput> | TOutput, SyncError>>;

export interface ResourceTelemetryContext extends OperationExecution {
	readonly resource: string;
	readonly method: string;
	readonly mutationId?: string;
}

export interface ResourceTelemetry {
	start?(context: ResourceTelemetryContext): Awaitable<void>;
	success?(context: ResourceTelemetryContext, metrics: readonly CostMetric[]): Awaitable<void>;
	error?(context: ResourceTelemetryContext, error: SyncError): Awaitable<void>;
}

export interface ResourceOptions {
	readonly name?: string;
	readonly telemetry?: ResourceTelemetry;
	handleError?(error: SyncError, context: ResourceTelemetryContext): Awaitable<void>;
}

export interface ManagerTelemetryContext extends OperationExecution {
	readonly manager: string;
	readonly method: string;
	readonly scope: string;
	readonly mutationId?: string;
}

export interface ManagerTelemetry {
	start?(context: ManagerTelemetryContext): Awaitable<void>;
	success?(context: ManagerTelemetryContext, metrics: readonly CostMetric[]): Awaitable<void>;
	error?(context: ManagerTelemetryContext, error: SyncError): Awaitable<void>;
}

export type ScopeValue = string | number | boolean | readonly (string | number | boolean)[];

export type AuthorizeResult = void | boolean | SyncResult<void, SyncError>;

export type AuthorizeHook<TContext, TParams> = (
	context: TContext,
	params: TParams,
	execution: OperationExecution
) => Awaitable<AuthorizeResult>;

export interface ManagerOptions<TContext, TParams> {
	readonly key: string;
	readonly authorize: AuthorizeHook<TContext, TParams>;
	scope(params: TParams): ScopeValue;
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
	};
	handleError?(error: SyncError, context: ManagerTelemetryContext): Awaitable<void>;
}
