import type {
	ItemAddedChange,
	ItemDeletedChange,
	ItemUpdatedChange,
	PageLoadedChange,
	ResetChange,
	ResetManifest
} from './types.js';

export const change = {
	pageLoaded<TItem>(input: {
		readonly items: readonly TItem[];
		readonly pageCursor?: string;
		readonly syncCursor?: string;
	}): PageLoadedChange<TItem> {
		return {
			type: 'pageLoaded',
			items: input.items,
			pageCursor: input.pageCursor,
			syncCursor: input.syncCursor
		};
	},

	itemAdded<TValue>(input: { readonly id: string; readonly value: TValue }): ItemAddedChange<TValue> {
		return {
			type: 'itemAdded',
			id: input.id,
			value: input.value
		};
	},

	itemUpdated<TValue, TPatch>(input: {
		readonly id: string;
		readonly patch?: TPatch;
		readonly value?: TValue;
	}): ItemUpdatedChange<TValue, TPatch> {
		return {
			type: 'itemUpdated',
			id: input.id,
			patch: input.patch,
			value: input.value
		};
	},

	itemDeleted<TTombstone>(input: {
		readonly id: string;
		readonly tombstone?: TTombstone;
	}): ItemDeletedChange<TTombstone> {
		return {
			type: 'itemDeleted',
			id: input.id,
			tombstone: input.tombstone
		};
	},

	reset(manifest: ResetManifest): ResetChange {
		return {
			type: 'reset',
			manifest
		};
	}
};
