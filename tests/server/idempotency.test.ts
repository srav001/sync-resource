import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import { manager, resetSharedStreamForTests, resource } from '../../src/server/index.ts';
import { createSyncTestSystem } from '../fixtures/syncSystem.ts';

afterEach(() => {
	vi.useRealTimers();
	resetSharedStreamForTests();
});

describe('manager mutation idempotency', () => {
	it('replays matching duplicate output/envelope without a second database write', async () => {
		const system = createSyncTestSystem();
		const bound = system.manager.bind({ workspaceId: 'workspace-1' });
		const args = { input: { id: 'n1', title: 'one' } };
		const first = await bound.add(args, { mutationId: 'same' });
		const second = await bound.add(args, { mutationId: 'same' });
		expect(first.isOk() && second.isOk()).toBe(true);
		expect(system.database.operations.filter((operation) => operation.startsWith('add:'))).toHaveLength(1);
		expect(system.persistence.mutationWrites).toHaveLength(1);
		expect(second.isOk() && second.value).toEqual(first.isOk() && first.value);
	});

	it('returns conflict for a reused mutation id with different method or args', async () => {
		const system = createSyncTestSystem();
		const bound = system.manager.bind({ workspaceId: 'workspace-1' });
		await bound.add({ input: { id: 'n1', title: 'one' } }, { mutationId: 'same' });
		const changed = await bound.add({ input: { id: 'n2', title: 'two' } }, { mutationId: 'same' });
		expect(changed.isErr() && changed.error.code).toBe('conflict');
		const method = await bound.delete({ query: { id: 'n1' } }, { mutationId: 'same' });
		expect(method.isErr() && method.error.code).toBe('conflict');
	});

	it('shares same-process concurrent execution for matching ids', async () => {
		const system = createSyncTestSystem();
		const gate = system.database.pauseNextCommit();
		const bound = system.manager.bind({ workspaceId: 'workspace-1' });
		const args = { input: { id: 'n1', title: 'one' } };
		// Assumes both calls take the same pre-registration await path, so the second observes the first in flight.
		const first = bound.add(args, { mutationId: 'same' });
		const second = bound.add(args, { mutationId: 'same' });
		await Promise.resolve();
		gate.resolve();
		const results = await Promise.all([first, second]);
		expect(results.every((result) => result.isOk())).toBe(true);
		expect(system.database.operations.filter((operation) => operation.startsWith('add:'))).toHaveLength(1);
	});

	it('re-executes a mutation only after the default memory idempotency window expires', async () => {
		vi.useFakeTimers();
		vi.setSystemTime(new Date('2026-07-23T00:00:00.000Z'));
		let handlerCalls = 0;
		const paramsSchema = {
			parse(value: unknown): { workspaceId: string } {
				if (
					!value ||
					typeof value !== 'object' ||
					typeof (value as { workspaceId?: unknown }).workspaceId !== 'string'
				) {
					throw new Error('invalid params');
				}
				return value as { workspaceId: string };
			}
		};
		const noteSchema = {
			parse(value: unknown): { id: string } {
				if (!value || typeof value !== 'object' || typeof (value as { id?: unknown }).id !== 'string') {
					throw new Error('invalid note');
				}
				return value as { id: string };
			}
		};
		const repository = resource(paramsSchema, (method) => ({
			add: method.add({
				input: noteSchema,
				output: noteSchema,
				handler({ input, ctx }) {
					handlerCalls += 1;
					return ctx.ok({ id: input.id });
				}
			})
		}));
		const notes = manager({
			key: 'memory-idempotency-expiry',
			resource: repository,
			authorize: () => true,
			scope: (params) => params.workspaceId
		});
		const bound = notes.bind({ workspaceId: 'w1' });
		const args = { input: { id: 'n1' } };

		expect((await bound.add(args, { mutationId: 'same' })).isOk()).toBe(true);
		expect((await bound.add(args, { mutationId: 'same' })).isOk()).toBe(true);
		expect(handlerCalls).toBe(1);

		vi.advanceTimersByTime(60 * 60_000);
		expect((await bound.add(args, { mutationId: 'same' })).isOk()).toBe(true);
		expect(handlerCalls).toBe(1);

		vi.advanceTimersByTime(1);
		expect((await bound.add(args, { mutationId: 'same' })).isOk()).toBe(true);
		expect(handlerCalls).toBe(2);
	});
});
