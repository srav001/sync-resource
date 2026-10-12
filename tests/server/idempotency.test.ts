import { afterEach, describe, expect, it, vi } from 'vite-plus/test';

import { resetSyncConfiguration } from '../../src/client/core.ts';
import {
	manager,
	resource,
	type ManagerMutationRecord,
	type ManagerOutboxRead,
	type ManagerSyncPersistence,
	type OperationExecution
} from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { idSchema, workspaceParams } from '../fixtures/schemas.ts';
import { createSyncTestSystem } from '../fixtures/syncSystem.ts';

afterEach(() => {
	vi.useRealTimers();
	resetSyncConfiguration();
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

	it('skips the duplicate lookup and arguments hash for a generated mutation id', async () => {
		const system = createSyncTestSystem();
		const readMutation = vi.spyOn(system.persistence, 'readMutation');
		const bound = system.manager.bind({ workspaceId: 'workspace-1' });
		await bound.add({ input: { id: 'n1', title: 'one' } });
		expect(readMutation).not.toHaveBeenCalled();
		expect(system.persistence.mutationWrites[0]).toMatchObject({ argsHash: '', status: 'finalized' });
		await bound.add({ input: { id: 'n2', title: 'two' } }, { mutationId: 'supplied' });
		expect(readMutation).toHaveBeenCalledTimes(1);
		expect(system.persistence.mutationWrites[1]?.argsHash).not.toBe('');
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
		const first = bound.add(args, { mutationId: 'same' });
		await gate.reached.promise;
		const second = bound.add(args, { mutationId: 'same' });
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
		await gate.reached.promise;

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
		const records: ManagerMutationRecord[] = [];
		const executions: OperationExecution[] = [];
		const persistence: ManagerSyncPersistence = {
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
		const repository = resource(workspaceParams, (method) => ({
			add: method.add({
				input: idSchema,
				output: idSchema,
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
		const repository = resource(workspaceParams, (method) => ({
			add: method.add({
				input: idSchema,
				output: idSchema,
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
