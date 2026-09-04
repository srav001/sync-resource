import { afterEach, describe, expect, it } from 'vite-plus/test';

import {
	err,
	manager,
	ok,
	resource,
	syncError,
	type ManagerMutationRecord,
	type ManagerOutboxRead,
	type ManagerRealtimeBus,
	type ManagerSyncPersistence,
	type ResourceCommitContext,
	type SyncEnvelope
} from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';

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

const inputSchema = {
	parse(value: unknown): { id: string } {
		if (!value || typeof value !== 'object' || typeof (value as { id?: unknown }).id !== 'string') {
			throw new Error('invalid input');
		}
		return value as { id: string };
	}
};

const outputSchema = {
	parse(value: unknown): { id: string } {
		if (!value || typeof value !== 'object' || typeof (value as { id?: unknown }).id !== 'string') {
			throw new Error('invalid output');
		}
		return value as { id: string };
	}
};

class TrackingPersistence implements ManagerSyncPersistence {
	readonly appended: SyncEnvelope[] = [];
	readonly records: ManagerMutationRecord[] = [];

	append(envelope: SyncEnvelope): void {
		this.appended.push(envelope);
	}

	readAfter(): ManagerOutboxRead {
		return { envelopes: [], cursorFound: false, retainedEnvelopeCount: 0 };
	}

	readMutation(scope: string, mutationId: string): ManagerMutationRecord | undefined {
		return this.records.find((record) => record.scope === scope && record.mutationId === mutationId);
	}

	recordMutation(record: ManagerMutationRecord): void {
		this.records.push(record);
		if (record.envelope) {
			this.appended.push(record.envelope);
		}
	}
}

class TrackingRealtimeBus implements ManagerRealtimeBus {
	readonly published: SyncEnvelope[] = [];

	publish(envelope: SyncEnvelope): void {
		this.published.push(envelope);
	}

	subscribe(): () => void {
		return () => {};
	}
}

afterEach(() => resetSharedStreamForTests());

describe('manager deferred source commits', () => {
	it.each([
		{ syncPersisted: false, expectedRecords: 1, expectedAppends: 1 },
		{ syncPersisted: true, expectedRecords: 0, expectedAppends: 0 }
	])(
		'publishes finality after a source commit with syncPersisted=$syncPersisted',
		async ({ syncPersisted, expectedRecords, expectedAppends }) => {
			const persistence = new TrackingPersistence();
			const bus = new TrackingRealtimeBus();
			const commits: ResourceCommitContext[] = [];
			const repository = resource(paramsSchema, (method) => ({
				add: method.add({
					input: inputSchema,
					output: outputSchema,
					handler({ input, ctx }) {
						return ctx.ok({
							output: { id: input.id },
							sourceCommit: {
								commit(context) {
									commits.push(context);
									return ok({
										syncPersisted,
										metrics: [{ name: 'commit', value: 1, unit: 'write' }]
									});
								}
							}
						});
					}
				})
			}));
			const notes = manager({
				key: `commit-${String(syncPersisted)}`,
				resource: repository,
				authorize: () => true,
				scope: (params) => params.workspaceId,
				persistence,
				realtimeBus: bus
			});

			const result = await notes
				.bind({ workspaceId: 'w1' })
				.add({ input: { id: 'n1' } }, { mutationId: 'm1', meta: { clientId: 'c1' } });

			expect(result.isOk() && result.value).toEqual({ id: 'n1' });
			expect(commits).toHaveLength(1);
			expect(commits[0]?.mutationRecord).toMatchObject({
				mutationId: 'm1',
				method: 'add',
				status: 'finalized',
				output: { id: 'n1' }
			});
			expect(commits[0]?.envelope).toMatchObject({
				sourceMutationId: 'm1',
				sourceClientId: 'c1',
				changes: [{ type: 'itemAdded', id: 'n1' }]
			});
			expect(persistence.records).toHaveLength(expectedRecords);
			expect(persistence.appended).toHaveLength(expectedAppends);
			expect(bus.published).toHaveLength(1);
			expect(bus.published[0]).toEqual(commits[0]?.envelope);
		}
	);

	it('returns a commit error without persistence or realtime publication', async () => {
		const persistence = new TrackingPersistence();
		const bus = new TrackingRealtimeBus();
		const repository = resource(paramsSchema, (method) => ({
			add: method.add({
				input: inputSchema,
				output: outputSchema,
				handler({ input, ctx }) {
					return ctx.ok({
						output: { id: input.id },
						sourceCommit: {
							commit() {
								return err(syncError('conflict', 'commit rejected'));
							}
						}
					});
				}
			})
		}));
		const notes = manager({
			key: 'commit-error',
			resource: repository,
			authorize: () => true,
			scope: (params) => params.workspaceId,
			persistence,
			realtimeBus: bus
		});

		const result = await notes.bind({ workspaceId: 'w1' }).add({ input: { id: 'n1' } }, { mutationId: 'm1' });
		expect(result.isErr() && result.error).toMatchObject({ code: 'conflict', message: 'commit rejected' });
		expect(persistence.records).toEqual([]);
		expect(persistence.appended).toEqual([]);
		expect(bus.published).toEqual([]);
	});

	it('records a successful write without changes as acknowledged rather than finalized', async () => {
		const persistence = new TrackingPersistence();
		const repository = resource(paramsSchema, (method) => ({
			add: method.add({
				input: inputSchema,
				output: {
					parse(value: unknown): string {
						if (typeof value !== 'string') {
							throw new Error('invalid output');
						}
						return value;
					}
				},
				handler({ ctx }) {
					return ctx.ok('accepted');
				}
			})
		}));
		const notes = manager({
			key: 'acknowledged-write',
			resource: repository,
			authorize: () => true,
			scope: (params) => params.workspaceId,
			persistence
		});

		const result = await notes.bind({ workspaceId: 'w1' }).add({ input: { id: 'n1' } }, { mutationId: 'm1' });
		expect(result.isOk() && result.value).toBe('accepted');
		expect(persistence.records).toHaveLength(1);
		expect(persistence.records[0]).toMatchObject({
			mutationId: 'm1',
			status: 'acknowledged',
			output: 'accepted'
		});
		expect(persistence.records[0]?.envelope).toBeUndefined();
		expect(persistence.appended).toEqual([]);
	});

	it('publishes signals as transient envelopes without appending them to the outbox', async () => {
		const persistence = new TrackingPersistence();
		const bus = new TrackingRealtimeBus();
		const repository = resource(paramsSchema, (method) => ({
			add: method.add({
				input: inputSchema,
				handler({ input, ctx }) {
					return ctx.ok({ signals: [{ type: 'created', payload: { id: input.id } }] });
				}
			})
		}));
		const notes = manager({
			key: 'transient-signal',
			resource: repository,
			authorize: () => true,
			scope: (params) => params.workspaceId,
			persistence,
			realtimeBus: bus
		});

		const result = await notes.bind({ workspaceId: 'w1' }).add({ input: { id: 'n1' } }, { mutationId: 'm1' });
		expect(result.isOk() && result.value).toBeUndefined();
		expect(persistence.records[0]).toMatchObject({ mutationId: 'm1', status: 'acknowledged' });
		expect(persistence.appended).toEqual([]);
		expect(bus.published).toHaveLength(1);
		expect(bus.published[0]).toMatchObject({
			changes: [],
			signals: [{ type: 'created', payload: { id: 'n1' } }]
		});
	});

	it('does not fail or block committed writes when realtime publication throws, rejects, or stays pending', async () => {
		const persistence = new TrackingPersistence();
		const committedIds: string[] = [];
		const publicationModes: string[] = [];
		const slowPublication = deferred<void>();
		let mode: 'throw' | 'reject' | 'slow' = 'throw';
		let slowPublicationStarted = false;
		const bus: ManagerRealtimeBus = {
			publish() {
				publicationModes.push(mode);
				if (mode === 'throw') {
					throw new Error('synchronous bus failure');
				}
				if (mode === 'reject') {
					return Promise.reject(new Error('asynchronous bus failure'));
				}
				slowPublicationStarted = true;
				return slowPublication.promise;
			},
			subscribe() {
				return () => {};
			}
		};
		const repository = resource(paramsSchema, (method) => ({
			add: method.add({
				input: inputSchema,
				output: outputSchema,
				handler({ input, ctx }) {
					committedIds.push(input.id);
					return ctx.ok({ id: input.id });
				}
			})
		}));
		const notes = manager({
			key: 'realtime-failure-isolation',
			resource: repository,
			authorize: () => true,
			scope: (params) => params.workspaceId,
			persistence,
			realtimeBus: bus
		});
		const bound = notes.bind({ workspaceId: 'w1' });

		const thrown = await bound.add({ input: { id: 'throw' } }, { mutationId: 'throw' });
		expect(thrown.isOk() && thrown.value).toEqual({ id: 'throw' });

		mode = 'reject';
		const rejected = await bound.add({ input: { id: 'reject' } }, { mutationId: 'reject' });
		expect(rejected.isOk() && rejected.value).toEqual({ id: 'reject' });

		mode = 'slow';
		let slowWriteSettled = false;
		const slowWrite = bound.add({ input: { id: 'slow' } }, { mutationId: 'slow' }).then((result) => {
			slowWriteSettled = true;
			return result;
		});
		await waitFor(() => slowPublicationStarted && slowWriteSettled);
		const slow = await slowWrite;
		expect(slow.isOk() && slow.value).toEqual({ id: 'slow' });

		expect(committedIds).toEqual(['throw', 'reject', 'slow']);
		expect(persistence.records.map((record) => record.output)).toEqual([
			{ id: 'throw' },
			{ id: 'reject' },
			{ id: 'slow' }
		]);
		expect(publicationModes).toEqual(['throw', 'reject', 'slow']);

		slowPublication.resolve();
	});

	it('rejects per-item source commits deferred through a looped manager batch', async () => {
		const persistence = new TrackingPersistence();
		let commits = 0;
		const repository = resource(paramsSchema, (method) => ({
			add: method.add({
				input: inputSchema,
				output: outputSchema,
				handler({ input, ctx }) {
					return ctx.ok({
						output: { id: input.id },
						sourceCommit: {
							commit() {
								commits += 1;
								return ok();
							}
						}
					});
				}
			})
		}));
		const notes = manager({
			key: 'loop-source-commit',
			resource: repository,
			authorize: () => true,
			scope: (params) => params.workspaceId,
			persistence
		});

		const result = await notes
			.bind({ workspaceId: 'w1' })
			.add([{ input: { id: 'a' } }, { input: { id: 'b' } }], { mutationId: 'batch' });
		expect(result.isErr() && result.error).toMatchObject({
			code: 'bad_request',
			message: expect.stringContaining('atomic batch handler')
		});
		expect(commits).toBe(0);
		expect(persistence.records).toEqual([]);
		expect(persistence.appended).toEqual([]);
	});
});

function deferred<TValue>() {
	let resolvePromise!: (value: TValue | PromiseLike<TValue>) => void;
	const promise = new Promise<TValue>((resolve) => {
		resolvePromise = resolve;
	});
	return {
		promise,
		resolve: resolvePromise
	};
}

async function waitFor(condition: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (condition()) {
			return;
		}
		await Promise.resolve();
	}
	throw new Error('Condition was not reached.');
}
