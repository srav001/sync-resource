import { syncError, type SyncError, type SyncErrorCode, type SyncErrorOptions } from './protocol.ts';

export class SyncOk<TValue, TError extends SyncError = SyncError> {
	readonly status = 'ok';
	readonly value: TValue;

	constructor(value: TValue) {
		this.value = value;
	}

	isOk(): this is SyncOk<TValue, TError> {
		return true;
	}

	isErr(): this is SyncErr<TValue, TError> {
		return false;
	}
}

export class SyncErr<TValue, TError extends SyncError = SyncError> {
	readonly status = 'error';
	readonly error: TError;

	constructor(errorValue: TError) {
		this.error = errorValue;
	}

	isOk(): this is SyncOk<never, TError> {
		return false;
	}

	isErr(): this is SyncErr<TValue, TError> {
		return true;
	}
}

export type SyncResult<TValue, TError extends SyncError = SyncError> = SyncOk<TValue, TError> | SyncErr<TValue, TError>;

export type InferOk<TResult> = TResult extends { readonly status: 'ok'; readonly value: infer TValue } ? TValue : never;
export type InferErr<TResult> = TResult extends {
	readonly status: 'error';
	readonly error: infer TError extends SyncError;
}
	? TError
	: never;

export function ok(): SyncOk<void, never>;
export function ok<TValue>(value: TValue): SyncOk<TValue, never>;
export function ok<TValue>(value?: TValue): SyncOk<TValue | void, never> {
	return new SyncOk(value);
}

export function err(code: SyncErrorCode, message: string, options?: SyncErrorOptions): SyncErr<never, SyncError>;
export function err<TError extends SyncError>(errorValue: TError): SyncErr<never, TError>;
export function err(
	...args:
		| readonly [errorValue: SyncError]
		| readonly [code: SyncErrorCode, message: string, options?: SyncErrorOptions]
): SyncErr<never, SyncError> {
	if (typeof args[0] === 'string') {
		return new SyncErr(syncError(args[0], args[1], args[2]));
	}

	return new SyncErr(args[0]);
}

export function isOk<TValue, TError extends SyncError>(
	result: SyncResult<TValue, TError>
): result is SyncOk<TValue, TError> {
	return result.isOk();
}

export function isError<TValue, TError extends SyncError>(
	result: SyncResult<TValue, TError>
): result is SyncErr<TValue, TError> {
	return result.isErr();
}

export function map<TValue, TError extends SyncError, TNext>(
	result: SyncResult<TValue, TError>,
	mapper: (value: TValue) => TNext
): SyncResult<TNext, TError> {
	if (result.isErr()) {
		return result;
	}

	return ok(mapper(result.value));
}

export function mapError<TValue, TError extends SyncError, TNextError extends SyncError>(
	result: SyncResult<TValue, TError>,
	mapper: (errorValue: TError) => TNextError
): SyncResult<TValue, TNextError> {
	if (result.isErr()) {
		return err(mapper(result.error));
	}

	return result as unknown as SyncOk<TValue, TNextError>;
}

export function andThen<TValue, TError extends SyncError, TNext, TNextError extends SyncError>(
	result: SyncResult<TValue, TError>,
	mapper: (value: TValue) => SyncResult<TNext, TNextError>
): SyncResult<TNext, TError | TNextError> {
	if (result.isErr()) {
		return result;
	}

	return mapper(result.value);
}

export function trySync<TValue, TError extends SyncError>(
	run: () => TValue,
	mapper: (cause: unknown) => TError
): SyncResult<TValue, TError> {
	try {
		return ok(run());
	} catch (cause) {
		return err(mapper(cause));
	}
}

export async function tryPromise<TValue, TError extends SyncError>(
	run: () => Promise<TValue>,
	mapper: (cause: unknown) => TError
): Promise<SyncResult<TValue, TError>> {
	try {
		return ok(await run());
	} catch (cause) {
		return err(mapper(cause));
	}
}

export const Result = {
	ok,
	err,
	isOk,
	isError,
	map,
	mapError,
	andThen,
	try: trySync,
	tryPromise
};
