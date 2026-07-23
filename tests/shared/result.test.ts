import { describe, expect, it } from 'vite-plus/test';

import {
	Result,
	andThen,
	err,
	isError,
	isOk,
	map,
	mapError,
	ok,
	tryPromise,
	trySync,
	type SyncResult
} from '../../src/server/index.ts';

function valueOf<T>(result: SyncResult<T>): T {
	if (result.isErr()) {
		throw new Error('expected success');
	}
	return result.value;
}

describe('SyncResult', () => {
	it('constructs and narrows success and error', () => {
		const success = ok({ id: '1' });
		const failure = err('not_found', 'Missing');
		expect(success.status).toBe('ok');
		expect(success.isOk()).toBe(true);
		expect(isOk(success)).toBe(true);
		expect(success.value).toEqual({ id: '1' });
		expect(failure.status).toBe('error');
		expect(failure.isErr()).toBe(true);
		expect(isError(failure)).toBe(true);
		expect(failure.error.code).toBe('not_found');
	});
	it('preserves existing errors and maps/chains results', () => {
		const error = err('conflict', 'Already used').error;
		expect(err(error).error).toBe(error);
		expect(valueOf(map(ok(2), (value) => value * 2))).toBe(4);
		const failure = err('internal', 'broken');
		expect(map(failure, () => 2)).toBe(failure);
		const mapped = mapError(failure, (value) => err('timeout', value.message).error);
		expect(mapped.isErr() && mapped.error.code).toBe('timeout');
		expect(valueOf(mapError(ok(2), () => error))).toBe(2);
		expect(valueOf(andThen(ok(2), (value) => ok(value + 1)))).toBe(3);
		expect(andThen(failure, () => ok(3))).toBe(failure);
	});
	it('converts thrown sync and async work', async () => {
		expect(
			valueOf(
				trySync(
					() => 3,
					() => err('internal', 'x').error
				)
			)
		).toBe(3);
		const syncFailure = trySync(
			() => {
				throw new Error('boom');
			},
			() => err('internal', 'mapped').error
		);
		expect(syncFailure.isErr() && syncFailure.error.message).toBe('mapped');
		expect(
			valueOf(
				await tryPromise(
					async () => 'ok',
					() => err('internal', 'x').error
				)
			)
		).toBe('ok');
		const asyncFailure = await tryPromise(
			async () => {
				throw new Error('boom');
			},
			() => err('internal', 'mapped').error
		);
		expect(asyncFailure.isErr() && asyncFailure.error.message).toBe('mapped');
	});
	it('exposes equivalent Result namespace operations', () => {
		expect(Result.isOk(Result.ok('value'))).toBe(true);
		expect(Result.isError(Result.err('internal', 'x'))).toBe(true);
		expect(valueOf(Result.map(Result.ok(1), (v) => v + 1))).toBe(2);
	});
});
