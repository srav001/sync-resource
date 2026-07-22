export const syncErrorCodes = [
	'validation',
	'bad_request',
	'unauthorized',
	'forbidden',
	'not_found',
	'conflict',
	'payload_too_large',
	'rate_limited',
	'aborted',
	'timeout',
	'disposed',
	'internal'
] as const;

export type SyncErrorCode = (typeof syncErrorCodes)[number];
export type SyncRecoverySource = 'memory' | 'idb' | 'server';

export interface SyncRestoredRecovery {
	readonly restored: true;
	readonly source: SyncRecoverySource;
}

export interface SyncUnrestoredRecovery {
	readonly restored?: false;
	readonly source?: never;
}

export type SyncErrorRecovery = SyncRestoredRecovery | SyncUnrestoredRecovery;
export type SyncRecoveryMetadata = SyncErrorRecovery;

export interface SyncProtocolError {
	readonly code: SyncErrorCode;
	readonly message: string;
	readonly details?: unknown;
	readonly recovery?: SyncErrorRecovery;
}

export interface SyncErrorOptions {
	readonly details?: unknown;
	readonly cause?: unknown;
	readonly recovery?: SyncErrorRecovery;
}

export class SyncError extends Error implements SyncProtocolError {
	readonly code: SyncErrorCode;
	readonly details?: unknown;
	readonly cause?: unknown;
	readonly recovery?: SyncErrorRecovery;

	constructor(code: SyncErrorCode, message: string, options?: SyncErrorOptions) {
		super(message);
		this.name = 'SyncError';
		this.code = code;
		this.details = options?.details;
		this.cause = options?.cause;
		this.recovery = options?.recovery;
	}

	toJSON(): SyncProtocolError {
		return toSyncProtocolError(this);
	}
}

export interface CostMetric {
	readonly name: string;
	readonly value: number;
	readonly unit: string;
}

export type SyncResetReason =
	| 'retention_gap'
	| 'manual'
	| 'auth_changed'
	| 'schema_changed'
	| 'cache_policy_changed'
	| 'repair'
	| 'replay_failed';

export interface ResetManifest {
	readonly scope: string;
	readonly reason: SyncResetReason;
	readonly affectedFamilies?: readonly string[];
	readonly previousCursor?: string;
	readonly nextCursor?: string;
	readonly schemaVersion?: string;
	readonly authVersion?: string;
	readonly cachePolicyVersion?: string;
}

export interface PageLoadedChange<TItem = unknown> {
	readonly type: 'pageLoaded';
	readonly items: readonly TItem[];
	readonly pageCursor?: string;
	readonly syncCursor?: string;
}

export interface ItemAddedChange<TValue = unknown> {
	readonly type: 'itemAdded';
	readonly id: string;
	readonly value: TValue;
}

export interface ItemUpdatedChange<TValue = unknown, TPatch = unknown> {
	readonly type: 'itemUpdated';
	readonly id: string;
	readonly patch?: TPatch;
	readonly value?: TValue;
}

export interface ItemDeletedChange<TTombstone = unknown> {
	readonly type: 'itemDeleted';
	readonly id: string;
	readonly tombstone?: TTombstone;
}

export interface ResetChange {
	readonly type: 'reset';
	readonly manifest: ResetManifest;
}

export type SyncChange = PageLoadedChange | ItemAddedChange | ItemUpdatedChange | ItemDeletedChange | ResetChange;

export interface SyncSignal<TPayload = unknown> {
	readonly type: string;
	readonly payload: TPayload;
}

export interface SyncEnvelope {
	readonly managerKey: string;
	readonly scope: string;
	readonly cursor: string;
	readonly sourceMutationId?: string;
	readonly sourceClientId?: string;
	readonly changes: readonly SyncChange[];
	readonly signals?: readonly SyncSignal[];
	readonly reset?: ResetManifest;
	readonly metrics?: readonly CostMetric[];
}

export interface SyncHttpOk<TValue> {
	readonly isOk: true;
	readonly isError: false;
	readonly value: TValue;
	readonly envelope?: SyncEnvelope;
}

export interface SyncHttpError {
	readonly isOk: false;
	readonly isError: true;
	readonly error: SyncProtocolError;
}

export type SyncHttpResult<TValue> = SyncHttpOk<TValue> | SyncHttpError;

export function syncError(code: SyncErrorCode, message: string, options?: SyncErrorOptions): SyncError {
	return new SyncError(code, message, options);
}

export function normalizeSyncError(cause: unknown, fallbackMessage = 'Sync operation failed.'): SyncError {
	if (isSyncError(cause)) {
		return cause;
	}

	if (isSyncProtocolError(cause)) {
		return syncError(cause.code, cause.message, { details: cause.details, recovery: cause.recovery });
	}

	if (cause instanceof Error) {
		return syncError('internal', cause.message || fallbackMessage, { cause });
	}

	return syncError('internal', String(cause), { cause });
}

export function toSyncProtocolError(error: SyncError | SyncProtocolError): SyncProtocolError {
	return {
		code: error.code,
		message: error.message,
		details: error.details,
		recovery: error.recovery
	};
}

export function syncErrorToHttpStatus(error: Pick<SyncProtocolError, 'code'>): number {
	switch (error.code) {
		case 'bad_request':
		case 'validation':
			return 400;
		case 'unauthorized':
			return 401;
		case 'forbidden':
			return 403;
		case 'not_found':
			return 404;
		case 'conflict':
			return 409;
		case 'payload_too_large':
			return 413;
		case 'rate_limited':
			return 429;
		case 'aborted':
			return 499;
		case 'timeout':
			return 504;
		case 'disposed':
		case 'internal':
			return 500;
		default: {
			const exhaustive: never = error.code;
			return exhaustive;
		}
	}
}

export function isSyncErrorCode(value: unknown): value is SyncErrorCode {
	return typeof value === 'string' && (syncErrorCodes as readonly string[]).includes(value);
}

export function isSyncErrorRecovery(value: unknown): value is SyncErrorRecovery {
	if (!isRecord(value)) {
		return false;
	}
	if (value.restored === true) {
		return isSyncRecoverySource(value.source);
	}
	if (value.restored === undefined || value.restored === false) {
		return value.source === undefined;
	}
	return false;
}

export function isSyncProtocolError(value: unknown): value is SyncProtocolError {
	if (!isRecord(value)) {
		return false;
	}
	if (!isSyncErrorCode(value.code) || typeof value.message !== 'string') {
		return false;
	}
	return value.recovery === undefined || isSyncErrorRecovery(value.recovery);
}

export function isSyncError(value: unknown): value is SyncError {
	return value instanceof SyncError;
}

export function isResetManifest(value: unknown): value is ResetManifest {
	if (!isRecord(value)) {
		return false;
	}
	if (
		typeof value.scope !== 'string' ||
		!isSyncResetReason(value.reason) ||
		!isOptionalString(value.previousCursor) ||
		!isOptionalString(value.nextCursor) ||
		!isOptionalString(value.schemaVersion) ||
		!isOptionalString(value.authVersion) ||
		!isOptionalString(value.cachePolicyVersion)
	) {
		return false;
	}
	if (value.affectedFamilies === undefined) {
		return true;
	}
	return Array.isArray(value.affectedFamilies) && value.affectedFamilies.every((item) => typeof item === 'string');
}

export function isSyncChange(value: unknown): value is SyncChange {
	if (!isRecord(value) || typeof value.type !== 'string') {
		return false;
	}
	switch (value.type) {
		case 'pageLoaded':
			return (
				Array.isArray(value.items) && isOptionalString(value.pageCursor) && isOptionalString(value.syncCursor)
			);
		case 'itemAdded':
			return typeof value.id === 'string' && 'value' in value;
		case 'itemUpdated':
			return typeof value.id === 'string';
		case 'itemDeleted':
			return typeof value.id === 'string';
		case 'reset':
			return isResetManifest(value.manifest);
		default:
			return false;
	}
}

export function isSyncSignal(value: unknown): value is SyncSignal {
	return isRecord(value) && typeof value.type === 'string' && 'payload' in value;
}

export function isSyncEnvelope(value: unknown): value is SyncEnvelope {
	if (!isRecord(value)) {
		return false;
	}
	if (
		typeof value.managerKey !== 'string' ||
		typeof value.scope !== 'string' ||
		typeof value.cursor !== 'string' ||
		!isOptionalString(value.sourceMutationId) ||
		!isOptionalString(value.sourceClientId) ||
		!Array.isArray(value.changes) ||
		!value.changes.every(isSyncChange)
	) {
		return false;
	}
	if (value.signals !== undefined && (!Array.isArray(value.signals) || !value.signals.every(isSyncSignal))) {
		return false;
	}
	if (value.reset !== undefined && !isResetManifest(value.reset)) {
		return false;
	}
	if (value.metrics !== undefined && (!Array.isArray(value.metrics) || !value.metrics.every(isCostMetric))) {
		return false;
	}
	return true;
}

export function isSyncHttpResult<TValue = unknown>(value: unknown): value is SyncHttpResult<TValue> {
	if (!isRecord(value)) {
		return false;
	}
	if (value.isOk === true && value.isError === false) {
		return value.envelope === undefined || isSyncEnvelope(value.envelope);
	}
	if (value.isOk === false && value.isError === true) {
		return isSyncProtocolError(value.error);
	}
	return false;
}

function isSyncRecoverySource(value: unknown): value is SyncRecoverySource {
	return value === 'memory' || value === 'idb' || value === 'server';
}

function isSyncResetReason(value: unknown): value is SyncResetReason {
	return (
		value === 'retention_gap' ||
		value === 'manual' ||
		value === 'auth_changed' ||
		value === 'schema_changed' ||
		value === 'cache_policy_changed' ||
		value === 'repair' ||
		value === 'replay_failed'
	);
}

function isCostMetric(value: unknown): value is CostMetric {
	return (
		isRecord(value) &&
		typeof value.name === 'string' &&
		typeof value.value === 'number' &&
		Number.isFinite(value.value) &&
		typeof value.unit === 'string'
	);
}

function isOptionalString(value: unknown): boolean {
	return value === undefined || typeof value === 'string';
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}
