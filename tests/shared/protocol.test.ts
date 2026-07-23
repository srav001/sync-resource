import { describe, expect, it } from 'vite-plus/test';

import {
	isResetManifest,
	isSyncChange,
	isSyncEnvelope,
	isSyncErrorCode,
	isSyncErrorRecovery,
	isSyncHttpResult,
	isSyncProtocolError,
	isSyncSignal,
	syncError,
	syncErrorCodes,
	syncErrorToHttpStatus,
	toSyncProtocolError
} from '../../src/shared/index.ts';

const manifest = { scope: 'notes:workspace', reason: 'retention_gap' as const, previousCursor: 'c0', nextCursor: 'c1' };
const envelope = {
	managerKey: 'notes',
	scope: 'notes:workspace',
	cursor: 'c1',
	changes: [{ type: 'itemAdded' as const, id: 'n1', value: { id: 'n1' } }],
	metrics: [{ name: 'db', value: 1, unit: 'units' }]
};

describe('shared protocol guards', () => {
	it('validates error codes, recovery, and protocol errors', () => {
		for (const code of syncErrorCodes) {
			expect(isSyncErrorCode(code)).toBe(true);
		}
		expect(isSyncErrorCode('unknown')).toBe(false);
		expect(isSyncErrorRecovery({ restored: true, source: 'server' })).toBe(true);
		expect(isSyncErrorRecovery({ restored: true, source: 'bad' })).toBe(false);
		expect(isSyncProtocolError({ code: 'bad_request', message: 'bad' })).toBe(true);
		expect(isSyncProtocolError({ code: 'bad_request', message: 4 })).toBe(false);
	});
	it('validates changes, signals, manifests, and envelopes', () => {
		expect(isResetManifest(manifest)).toBe(true);
		expect(isResetManifest({ ...manifest, reason: 'nope' })).toBe(false);
		expect(isSyncChange({ type: 'pageLoaded', items: [], pageCursor: 'p' })).toBe(true);
		expect(isSyncChange({ type: 'itemUpdated', id: 'n1' })).toBe(true);
		expect(isSyncChange({ type: 'reset', manifest })).toBe(true);
		expect(isSyncChange({ type: 'unknown' })).toBe(false);
		expect(isSyncSignal({ type: 'refresh', payload: { id: 'n1' } })).toBe(true);
		expect(isSyncSignal({ type: 'refresh' })).toBe(false);
		expect(isSyncEnvelope(envelope)).toBe(true);
		expect(isSyncEnvelope({ ...envelope, changes: [{ type: 'bad' }] })).toBe(false);
		expect(isSyncEnvelope({ ...envelope, metrics: [{ name: 'db', value: Infinity, unit: 'u' }] })).toBe(false);
	});
	it('validates HTTP result unions', () => {
		expect(isSyncHttpResult({ isOk: true, isError: false, value: { ok: true }, envelope })).toBe(true);
		expect(isSyncHttpResult({ isOk: false, isError: true, error: { code: 'forbidden', message: 'no' } })).toBe(
			true
		);
		expect(isSyncHttpResult({ isOk: true, isError: true, value: null })).toBe(false);
	});
	it('maps every error code to its HTTP status', () => {
		const expected: Record<(typeof syncErrorCodes)[number], number> = {
			validation: 400,
			bad_request: 400,
			unauthorized: 401,
			forbidden: 403,
			not_found: 404,
			conflict: 409,
			payload_too_large: 413,
			rate_limited: 429,
			aborted: 499,
			timeout: 504,
			disposed: 500,
			internal: 500
		};
		for (const code of syncErrorCodes) {
			expect(syncErrorToHttpStatus({ code })).toBe(expected[code]);
		}
	});
	it('converts SyncError to a wire-safe error', () => {
		const error = syncError('internal', 'failed', { details: { retry: true }, recovery: { restored: false } });
		expect(toSyncProtocolError(error)).toEqual({
			code: 'internal',
			message: 'failed',
			details: { retry: true },
			recovery: { restored: false }
		});
	});
});
