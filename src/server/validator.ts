import { normalizeValidator } from '../shared/index.ts';
import { err, ok, type SyncResult } from '../shared/result.js';
import type { AnySchema, SyncError } from './types.js';

export { normalizeValidator } from '../shared/index.ts';

export function parseSchema<TValue, TInput>(
	schema: AnySchema<TValue>,
	value: TInput,
	label: string
): SyncResult<TValue, SyncError> {
	const validator = normalizeValidator(schema);
	try {
		return ok(validator.parse(value));
	} catch (cause) {
		return err('validation', `Invalid ${label}.`, {
			cause,
			details: cause instanceof Error ? cause.message : String(cause)
		});
	}
}
