import { describe, expect, it } from 'vite-plus/test';

import { err, ok, resource, syncError } from '../../src/server/index.ts';

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

describe('resource batch execution', () => {
	it('validates one shared scope and invokes an atomic batch handler once with partial results', async () => {
		let batchCalls = 0;
		const notes = resource(paramsSchema, (method) => ({
			add: method.add({
				input: inputSchema,
				output: outputSchema,
				handler({ ctx }) {
					return ctx.error(ctx.syncError('internal', 'single handler should not run'));
				},
				batchHandler({ params, items }) {
					batchCalls += 1;
					expect(params).toEqual({ workspaceId: 'w1' });
					expect(items).toEqual([{ input: { id: 'a' } }, { input: { id: 'b' } }]);
					return ok({
						items: [
							{ index: 0, status: 'ok', value: { output: { id: 'a' } } },
							{ index: 1, status: 'error', error: syncError('conflict', 'duplicate') }
						],
						execution: { mode: 'transaction', atomic: true, okCount: 1, errorCount: 1 }
					});
				}
			})
		}));

		const result = await notes.add([
			{ params: { workspaceId: 'w1' }, input: { id: 'a' } },
			{ params: { workspaceId: 'w1' }, input: { id: 'b' } }
		]);
		expect(result.isOk()).toBe(true);
		expect(result.isOk() && result.value.execution).toEqual({
			mode: 'transaction',
			atomic: true,
			okCount: 1,
			errorCount: 1
		});
		expect(result.isOk() && result.value.items[1]?.status).toBe('error');
		expect(batchCalls).toBe(1);

		const mixedScope = await notes.add([
			{ params: { workspaceId: 'w1' }, input: { id: 'a' } },
			{ params: { workspaceId: 'w2' }, input: { id: 'b' } }
		]);
		expect(mixedScope.isErr() && mixedScope.error.code).toBe('bad_request');
		expect(batchCalls).toBe(1);
	});

	it('continues loop batches after item errors and derives indexed mutation ids', async () => {
		const seen: string[] = [];
		const notes = resource(paramsSchema, (method) => ({
			add: method.add({
				input: inputSchema,
				output: outputSchema,
				handler({ input, ctx }) {
					seen.push(`${input.id}:${ctx.mutationId}`);
					if (input.id === 'b') {
						return ctx.error(ctx.syncError('conflict', 'duplicate'));
					}
					return ctx.ok({ id: input.id });
				}
			})
		}));

		const result = await notes.add(
			[
				{ params: { workspaceId: 'w1' }, input: { id: 'a' } },
				{ params: { workspaceId: 'w1' }, input: { id: 'b' } },
				{ params: { workspaceId: 'w1' }, input: { id: 'c' } }
			],
			{ mutationId: 'bulk' }
		);
		expect(result.isOk() && result.value.execution).toEqual({
			mode: 'loop',
			atomic: false,
			okCount: 2,
			errorCount: 1
		});
		expect(result.isOk() && result.value.items.map((item) => item.status)).toEqual(['ok', 'error', 'ok']);
		expect(seen).toEqual(['a:bulk:0', 'b:bulk:1', 'c:bulk:2']);
	});

	it('commits a resource-level atomic batch once and merges commit metrics', async () => {
		const commitContexts: unknown[] = [];
		const notes = resource(paramsSchema, (method) => ({
			add: method.add({
				input: inputSchema,
				output: outputSchema,
				handler({ ctx }) {
					return ctx.error(ctx.syncError('internal', 'single handler should not run'));
				},
				batchHandler({ items }) {
					return ok({
						items: items.map((item, index) => ({
							index,
							status: 'ok' as const,
							value: { output: { id: item.input.id } }
						})),
						execution: { mode: 'transaction', atomic: true, okCount: items.length, errorCount: 0 },
						metrics: [{ name: 'write', value: 2, unit: 'row' }],
						sourceCommit: {
							commit(context) {
								commitContexts.push(context);
								return ok({ metrics: [{ name: 'commit', value: 1, unit: 'transaction' }] });
							}
						}
					});
				}
			})
		}));

		const result = await notes.add([
			{ params: { workspaceId: 'w1' }, input: { id: 'a' } },
			{ params: { workspaceId: 'w1' }, input: { id: 'b' } }
		]);
		expect(result.isOk()).toBe(true);
		expect(commitContexts).toEqual([{}]);
		expect(result.isOk() && result.value.metrics).toEqual([
			{ name: 'write', value: 2, unit: 'row' },
			{ name: 'commit', value: 1, unit: 'transaction' }
		]);
		expect(result.isOk() && result.value.sourceCommit).toBeUndefined();
	});
});

describe('resource output interpretation', () => {
	it('treats a domain object named output as bare output unless resource metadata disambiguates it', async () => {
		const nestedOutputSchema = {
			parse(value: unknown): { output: string } {
				if (!value || typeof value !== 'object' || typeof (value as { output?: unknown }).output !== 'string') {
					throw new Error('invalid nested output');
				}
				return value as { output: string };
			}
		};
		const notes = resource(paramsSchema, (method) => ({
			get: method.get<{ output: string }>({
				output: nestedOutputSchema,
				handler() {
					return ok({ output: 'domain-value' });
				}
			}),
			list: method.list<{ id: string }>({
				output: outputSchema,
				handler() {
					return ok({
						output: { id: 'n1' },
						metrics: [{ name: 'read', value: 1, unit: 'row' }]
					});
				}
			})
		}));

		const bare = await notes.get({ params: { workspaceId: 'w1' } });
		expect(bare.isOk() && bare.value.output).toEqual({ output: 'domain-value' });
		const structured = await notes.list({ params: { workspaceId: 'w1' } });
		expect(structured.isOk() && structured.value).toEqual({
			output: { id: 'n1' },
			metrics: [{ name: 'read', value: 1, unit: 'row' }]
		});
	});

	it('rejects invalid structured batch output before running its source commit', async () => {
		let commits = 0;
		const notes = resource(paramsSchema, (method) => ({
			add: method.add<{ id: string }, { id: string }>({
				input: inputSchema,
				output: outputSchema,
				handler({ ctx }) {
					return ctx.ok({ id: 'unused' });
				},
				batchHandler() {
					return ok({
						items: [
							{
								index: 0,
								status: 'ok',
								value: { output: { wrong: true } as unknown as { id: string } }
							}
						],
						execution: { mode: 'bulk', atomic: false, okCount: 1, errorCount: 0 },
						sourceCommit: {
							commit() {
								commits += 1;
								return err(syncError('internal', 'should not commit'));
							}
						}
					});
				}
			})
		}));

		const result = await notes.add([{ params: { workspaceId: 'w1' }, input: { id: 'a' } }]);
		expect(result.isErr() && result.error.code).toBe('validation');
		expect(commits).toBe(0);
	});
});
