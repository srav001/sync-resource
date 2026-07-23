import { describe, expect, it } from 'vite-plus/test';

import { change } from '../../src/server/index.ts';

describe('public change constructors', () => {
	it('builds every durable change shape without cloning caller values', () => {
		const items = [{ id: 'note-1' }];
		const value = { id: 'note-2', title: 'Added' };
		const patch = { title: 'Updated' };
		const tombstone = { id: 'note-3', deleted: true };
		const manifest = {
			scope: 'notes:workspace-1',
			reason: 'retention_gap' as const,
			previousCursor: 'cursor-1',
			nextCursor: 'cursor-2'
		};

		const pageLoaded = change.pageLoaded({ items, pageCursor: 'page-2', syncCursor: 'cursor-2' });
		const itemAdded = change.itemAdded({ id: 'note-2', value });
		const itemUpdated = change.itemUpdated({ id: 'note-2', patch, value });
		const itemDeleted = change.itemDeleted({ id: 'note-3', tombstone });
		const reset = change.reset(manifest);

		expect(pageLoaded).toEqual({
			type: 'pageLoaded',
			items,
			pageCursor: 'page-2',
			syncCursor: 'cursor-2'
		});
		expect(pageLoaded.items).toBe(items);
		expect(itemAdded).toEqual({ type: 'itemAdded', id: 'note-2', value });
		expect(itemAdded.value).toBe(value);
		expect(itemUpdated).toEqual({ type: 'itemUpdated', id: 'note-2', patch, value });
		expect(itemUpdated.patch).toBe(patch);
		expect(itemDeleted).toEqual({ type: 'itemDeleted', id: 'note-3', tombstone });
		expect(itemDeleted.tombstone).toBe(tombstone);
		expect(reset).toEqual({ type: 'reset', manifest });
		expect(reset.manifest).toBe(manifest);
	});
});
