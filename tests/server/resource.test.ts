import { describe, expect, it } from 'vite-plus/test';

import { ok, resource } from '../../src/server/index.ts';

const params = {
	parse(value: unknown) {
		if (!value || typeof value !== 'object' || typeof (value as { scope?: unknown }).scope !== 'string') {
			throw new Error('bad params');
		}
		return value as { scope: string };
	}
};
const query = {
	parse(value: unknown) {
		if (!value || typeof value !== 'object' || typeof (value as { id?: unknown }).id !== 'string') {
			throw new Error('bad query');
		}
		return value as { id: string };
	}
};
const input = {
	parse(value: unknown) {
		if (!value || typeof value !== 'object' || typeof (value as { title?: unknown }).title !== 'string') {
			throw new Error('bad input');
		}
		return value as { title: string };
	}
};
const output = {
	parse(value: unknown) {
		if (!value || typeof value !== 'object' || typeof (value as { id?: unknown }).id !== 'string') {
			throw new Error('bad output');
		}
		return value as { id: string; title?: string };
	}
};

describe('resource public execution', () => {
	it('validates params/query/input before invoking handlers and exposes context', async () => {
		let calls = 0;
		const notes = resource(params, (method) => ({
			mutate: method.mutate({
				query,
				input,
				output,
				handler({ params: bound, query: selected, input: patch, ctx, self }) {
					calls += 1;
					expect(bound.scope).toBe('s');
					expect(selected.id).toBe('n');
					expect(patch.title).toBe('new');
					expect(ctx.mutationId).toBe('m1');
					expect(self).toEqual({});
					return ctx.ok({ id: selected.id, title: patch.title });
				}
			})
		}));
		const result = await notes.mutate(
			{ params: { scope: 's' }, query: { id: 'n' }, input: { title: 'new' } },
			{ mutationId: 'm1' }
		);
		expect(result.isOk()).toBe(true);
		expect(calls).toBe(1);
		const invalid = await notes.mutate({
			params: { scope: 's' },
			query: { id: 1 } as unknown as { id: string },
			input: { title: 'new' }
		});
		expect(invalid.isErr() && invalid.error.code).toBe('validation');
		expect(calls).toBe(1);
	});

	it('normalizes bare output, structured state, and rejects mixed signals', async () => {
		const notes = resource(params, (method) => ({
			get: method.get({ output, handler: () => ok({ id: 'n' }) }),
			list: method.list({
				output: { parse: (value: unknown) => value as { id: string } },
				handler({ ctx }) {
					return ctx.ok({
						output: { id: 'n' },
						changes: [{ type: 'itemAdded' as const, id: 'n', value: { id: 'n' } }]
					});
				}
			}),
			add: method.add({
				input,
				output,
				handler({ ctx }) {
					return ctx.ok({ signals: [{ type: 'x', payload: 1 }], output: { id: 'n' } } as never);
				}
			})
		}));
		const bare = await notes.get({ params: { scope: 's' } });
		expect(bare.isOk() && bare.value).toEqual({ output: { id: 'n' } });
		const structured = await notes.list({ params: { scope: 's' } });
		expect(structured.isOk() && structured.value.changes).toHaveLength(1);
		const mixed = await notes.add({ params: { scope: 's' }, input: { title: 'x' } });
		expect(mixed.isErr() && mixed.error.code).toBe('validation');
	});

	it('supports sibling self calls with inherited execution context', async () => {
		let seen = '';
		const notes = resource(params, (method) => ({
			get: method.get({
				output,
				handler({ ctx }) {
					seen = ctx.meta?.source as string;
					return ctx.ok({ id: 'n' });
				}
			}),
			mutate: method.mutate({
				input,
				output,
				handler({ self, ctx }) {
					const sibling = (self as { get: (args: unknown, options?: unknown) => Promise<unknown> }).get;
					return sibling({ source: 'x' }, { meta: { source: 'self' } }).then(() =>
						ctx.ok({ id: 'n', title: 'ok' })
					);
				}
			})
		}));
		const result = await notes.mutate(
			{ params: { scope: 's' }, input: { title: 'x' } },
			{ meta: { source: 'outer' } }
		);
		expect(result.isOk()).toBe(true);
		expect(seen).toBe('outer');
	});

	it('loops writes without a batch handler and suffixes mutation ids', async () => {
		const seen: string[] = [];
		const notes = resource(params, (method) => ({
			add: method.add({
				input,
				output,
				handler({ input: value, ctx }) {
					seen.push(`${value.title}:${ctx.mutationId}`);
					return ctx.ok({ id: value.title });
				}
			})
		}));
		const result = await notes.add(
			[
				{ params: { scope: 's' }, input: { title: 'a' } },
				{ params: { scope: 's' }, input: { title: 'b' } }
			],
			{ mutationId: 'batch' }
		);
		expect(result.isOk() && result.value.execution.mode).toBe('loop');
		expect(seen).toEqual(['a:batch:0', 'b:batch:1']);
	});
});
