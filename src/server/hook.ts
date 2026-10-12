import { isThenable } from '../shared/guards.ts';
import { err, ok, type SyncResult } from '../shared/result.js';
import { normalizeSyncError, type SyncError } from './errors.js';
import type { Awaitable } from './types.js';

// Hooks are usually absent or synchronous, so only a thenable result costs an extra promise.
export function callHook<TValue>(run: () => TValue): Awaitable<SyncResult<void, SyncError>> {
	try {
		const value = run();
		return isThenable(value)
			? Promise.resolve(value).then(
					() => ok(),
					(cause: unknown) => err(normalizeSyncError(cause))
				)
			: ok();
	} catch (cause) {
		return err(normalizeSyncError(cause));
	}
}
