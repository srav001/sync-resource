import { Context, Effect } from 'effect';
import { describe, expect, expectTypeOf, it } from 'vite-plus/test';

import { resource } from '../../src/server/effect.ts';

const params = { parse: (value: unknown) => value as { readonly workspaceId: string } };
const query = { parse: (value: unknown) => value as { readonly limit: number } };
const page = { parse: (value: unknown) => value as { readonly items: readonly string[] } };
const Repository = Context.Service<{ readonly list: (limit: number) => Effect.Effect<readonly string[]> }>(
	'test/Repository'
);

describe('effect read method query types', () => {
	it('types the query of get and list handlers from their query schema', () => {
		const notes = resource(params, (method) => ({
			get: method.get({
				query,
				output: page,
				handler: ({ query }) => {
					expectTypeOf(query).toEqualTypeOf<{ readonly limit: number }>();
					return Effect.succeed({ items: Array.from({ length: query.limit }, String) });
				}
			}),
			list: method.list({
				query,
				output: page,
				handler: ({ query }) =>
					Effect.gen(function* () {
						expectTypeOf(query).toEqualTypeOf<{ readonly limit: number }>();
						return { items: yield* (yield* Repository).list(query.limit) };
					})
			})
		}));
		expect(notes.definition.methods.list.kind).toBe('list');
	});
});
