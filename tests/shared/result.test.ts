import { describe, expect, it } from 'vite-plus/test';

import { err, ok } from '../../src/server/index.ts';

describe('SyncResult', () => {
	it('constructs and narrows success and error', () => {
		const success = ok({ id: '1' });
		const failure = err('not_found', 'Missing');
		expect(success.status).toBe('ok');
		expect(success.isOk()).toBe(true);
		expect(success.value).toEqual({ id: '1' });
		expect(failure.status).toBe('error');
		expect(failure.isErr()).toBe(true);
		expect(failure.error.code).toBe('not_found');
	});
	it('preserves existing errors', () => {
		const error = err('conflict', 'Already used').error;
		expect(err(error).error).toBe(error);
	});
});
