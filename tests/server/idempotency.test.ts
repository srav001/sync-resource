import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import {
	manager,
	resource,
	type ManagerMutationRecord,
	type ManagerOutboxRead,
	type ManagerSyncPersistence,
	type OperationExecution,
	type SyncEnvelope
} from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
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

	it('lets a duplicate caller cancel its own wait without cancelling the execution owner', async () => {
		const system = createSyncTestSystem();
		const gate = system.database.pauseNextCommit();
		const bound = system.manager.bind({ workspaceId: 'workspace-1' });
		const args = { input: { id: 'n1', title: 'one' } };
		let ownerSettled = false;
		const owner = bound.add(args, { mutationId: 'same' }).then((result) => {
			ownerSettled = true;
			return result;
		});
		for (let index = 0; index < 10; index += 1) {
			await Promise.resolve();
		}

		const abort = new AbortController();
		abort.abort();
		const duplicate = await bound.add(args, { mutationId: 'same', signal: abort.signal });
		expect(duplicate.isErr() && duplicate.error.code).toBe('aborted');
		expect(ownerSettled).toBe(false);

		gate.resolve();
		expect((await owner).isOk()).toBe(true);
		expect(system.database.operations).toEqual(['add:workspace-1:n1']);
	});

	it('awaits asynchronous persistence and propagates operation execution state', async () => {
		const paramsSchema = {
			parse(value: unknown): { workspaceId: string } {
				return value as { workspaceId: string };
			}
		};
		const noteSchema = {
			parse(value: unknown): { id: string } {
				return value as { id: string };
			}
		};
		const records: ManagerMutationRecord[] = [];
		const executions: OperationExecution[] = [];
		const persistence: ManagerSyncPersistence = {
			async append(_envelope: SyncEnvelope, execution?: OperationExecution) {
				await Promise.resolve();
				if (execution) {
					executions.push(execution);
				}
			},
			async readAfter(): Promise<ManagerOutboxRead> {
				return { cursorFound: true, envelopes: [], retainedEnvelopeCount: 0 };
			},
			async readMutation(_scope, _mutationId, execution) {
				await Promise.resolve();
				if (execution) {
					executions.push(execution);
				}
				return undefined;
			},
			async recordMutation(record, execution) {
				await Promise.resolve();
				records.push(record);
				if (execution) {
					executions.push(execution);
				}
			}
		};
		const repository = resource(paramsSchema, (method) => ({
			add: method.add({
				input: noteSchema,
				output: noteSchema,
				handler({ input, ctx }) {
					return ctx.ok({ id: input.id });
				}
			})
		}));
		const notes = manager({
			key: 'async-persistence',
			resource: repository,
			authorize: () => true,
			scope: (params) => params.workspaceId,
			persistence
		});
		const signal = new AbortController().signal;
		const result = await notes
			.bind({ workspaceId: 'w1' })
			.add({ input: { id: 'n1' } }, { mutationId: 'm1', signal });

		expect(result.isOk()).toBe(true);
		expect(records).toHaveLength(1);
		expect(executions.length).toBeGreaterThanOrEqual(2);
		expect(executions.every((execution) => execution.signal === signal)).toBe(true);
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
