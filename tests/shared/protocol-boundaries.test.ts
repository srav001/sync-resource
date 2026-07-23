import { describe, expect, it } from 'vite-plus/test';

import {
	isResetManifest,
	isSyncChange,
	isSyncEnvelope,
	isSyncErrorRecovery,
	isSyncHttpResult,
	isSyncProtocolError,
	isSyncSignal,
	normalizeSyncError,
	syncError
} from '../../src/shared/index.ts';

const resetReasons = [
	'retention_gap',
	'manual',
	'auth_changed',
	'schema_changed',
	'cache_policy_changed',
	'repair',
	'replay_failed'
] as const;

describe('protocol boundary rejection', () => {
	it('normalizes native, protocol, primitive, and existing sync errors', () => {
		const existing = syncError('conflict', 'same', { recovery: { restored: true, source: 'server' } });
		expect(normalizeSyncError(existing)).toBe(existing);

		const protocol = normalizeSyncError({
			code: 'forbidden',
			message: 'denied',
			details: { role: 'viewer' },
			recovery: { restored: false }
		});
		expect(protocol).toMatchObject({
			code: 'forbidden',
			message: 'denied',
			details: { role: 'viewer' },
			recovery: { restored: false }
		});
		expect(protocol.toJSON()).toEqual({
			code: 'forbidden',
			message: 'denied',
			details: { role: 'viewer' },
			recovery: { restored: false }
		});

		const native = new Error('');
		const normalizedNative = normalizeSyncError(native, 'fallback');
		expect(normalizedNative).toMatchObject({ code: 'internal', message: 'fallback', cause: native });
		expect(normalizeSyncError(42)).toMatchObject({ code: 'internal', message: '42', cause: 42 });
	});

	it('accepts only the two recovery shapes and complete protocol errors', () => {
		for (const source of ['memory', 'idb', 'server']) {
			expect(isSyncErrorRecovery({ restored: true, source })).toBe(true);
		}
		expect(isSyncErrorRecovery({})).toBe(true);
		expect(isSyncErrorRecovery({ restored: false })).toBe(true);
		expect(isSyncErrorRecovery(null)).toBe(false);
		expect(isSyncErrorRecovery({ restored: true })).toBe(false);
		expect(isSyncErrorRecovery({ restored: false, source: 'memory' })).toBe(false);
		expect(isSyncErrorRecovery({ restored: 'yes' })).toBe(false);
		expect(isSyncProtocolError(null)).toBe(false);
		expect(isSyncProtocolError({ code: 'internal', message: 'failed', recovery: { restored: true } })).toBe(false);
	});

	it('validates every reset reason and rejects malformed optional manifest fields', () => {
		for (const reason of resetReasons) {
			expect(
				isResetManifest({
					scope: 'notes:workspace-1',
					reason,
					affectedFamilies: ['active'],
					previousCursor: 'before',
					nextCursor: 'after',
					schemaVersion: '1',
					authVersion: '2',
					cachePolicyVersion: '3'
				})
			).toBe(true);
		}
		for (const manifest of [
			null,
			{ scope: 1, reason: 'manual' },
			{ scope: 'scope', reason: 'unknown' },
			{ scope: 'scope', reason: 'manual', previousCursor: 1 },
			{ scope: 'scope', reason: 'manual', affectedFamilies: ['one', 2] }
		]) {
			expect(isResetManifest(manifest)).toBe(false);
		}
	});

	it('validates every change and signal discriminator at the wire boundary', () => {
		expect(isSyncChange({ type: 'pageLoaded', items: [], pageCursor: 1 })).toBe(false);
		expect(isSyncChange({ type: 'pageLoaded', items: {}, syncCursor: 'cursor' })).toBe(false);
		expect(isSyncChange({ type: 'itemAdded', id: 'note-1' })).toBe(false);
		expect(isSyncChange({ type: 'itemAdded', id: 1, value: {} })).toBe(false);
		expect(isSyncChange({ type: 'itemUpdated', id: 1 })).toBe(false);
		expect(isSyncChange({ type: 'itemDeleted', id: 1 })).toBe(false);
		expect(isSyncChange({ type: 'reset', manifest: {} })).toBe(false);
		expect(isSyncChange([])).toBe(false);
		expect(isSyncSignal({ type: 'notice', payload: undefined })).toBe(true);
		expect(isSyncSignal({ type: 1, payload: {} })).toBe(false);
		expect(isSyncSignal([])).toBe(false);
	});

	it('rejects malformed optional envelope fields and result envelopes', () => {
		const envelope = {
			managerKey: 'notes',
			scope: 'notes:workspace-1',
			cursor: 'cursor-1',
			sourceMutationId: 'mutation-1',
			sourceClientId: 'client-1',
			changes: [],
			signals: [{ type: 'notice', payload: { id: 'note-1' } }],
			reset: { scope: 'notes:workspace-1', reason: 'manual' },
			metrics: [{ name: 'database.cost', value: 1, unit: 'units' }]
		};
		expect(isSyncEnvelope(envelope)).toBe(true);
		for (const malformed of [
			null,
			{ ...envelope, managerKey: 1 },
			{ ...envelope, sourceMutationId: 1 },
			{ ...envelope, sourceClientId: 1 },
			{ ...envelope, changes: {} },
			{ ...envelope, signals: [{}] },
			{ ...envelope, reset: {} },
			{ ...envelope, metrics: [{ name: 'cost', value: Number.NaN, unit: 'units' }] },
			{ ...envelope, metrics: [{ name: 1, value: 1, unit: 'units' }] }
		]) {
			expect(isSyncEnvelope(malformed)).toBe(false);
		}
		expect(isSyncHttpResult({ isOk: true, isError: false, value: null, envelope })).toBe(true);
		expect(isSyncHttpResult({ isOk: true, isError: false, value: null, envelope: {} })).toBe(false);
		expect(isSyncHttpResult({ isOk: false, isError: true, error: {} })).toBe(false);
		expect(isSyncHttpResult(null)).toBe(false);

		// DIVERGENCE: the wire guard currently accepts a success object with no own value field.
		expect(isSyncHttpResult({ isOk: true, isError: false })).toBe(true);
	});
});
