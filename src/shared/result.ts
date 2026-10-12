import { SyncError, syncError, type SyncErrorCode, type SyncErrorOptions } from './protocol.ts';

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
	if (args.length === 1) {
		return new SyncErr(args[0]);
	}

	const [code, message, options] = args;
	return new SyncErr(syncError(code, message, options));
}
