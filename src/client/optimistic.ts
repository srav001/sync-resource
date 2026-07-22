import { stableStringify, toStableJson } from '../shared/stableJson.ts';
import type { PageState, ReconcileConfig, SyncEnvelope } from './types.ts';

export type WriteMethod = 'add' | 'mutate' | 'delete';
export type PendingState = 'local_accepted' | 'acked_waiting_delta';

export interface PendingCommand {
	readonly mutationId: string;
	readonly method: WriteMethod;
	readonly state: PendingState;
	readonly targetIds: readonly string[];
}

export interface InternalPendingCommand extends PendingCommand {
	readonly args: unknown;
}

export interface PageFamilyState {
	readonly key: string;
	query: unknown;
	pages: PageState[];
	meta?: unknown;
}

export interface OptimisticState {
	data?: unknown;
	baseData?: unknown;
	readonly items: Map<string, unknown>;
	readonly baseItems: Map<string, unknown>;
	readonly families: Map<string, PageFamilyState>;
	activeFamilyKey?: string;
	pages: PageState[];
	listMeta?: unknown;
	cursor?: string;
}

export interface CachedPageFamilyState {
	readonly key: string;
	readonly query?: unknown;
	readonly pages: readonly PageState[];
	readonly meta?: unknown;
}

export interface CachedOptimisticState {
	readonly data?: unknown;
	readonly baseData?: unknown;
	readonly items: readonly unknown[];
	readonly baseItems?: readonly unknown[];
	readonly families?: readonly CachedPageFamilyState[];
	readonly activeFamilyKey?: string;
	readonly pages: readonly PageState[];
	readonly listMeta?: unknown;
	readonly cursor?: string;
}

export interface OptimisticReconcile {
	itemId(item: unknown): string | undefined;
	targetId(query: unknown, input: unknown): string | undefined;
	matchesQuery?(item: unknown, query: unknown): boolean;
	compare?(this: void, left: unknown, right: unknown): number;
}

export function createOptimisticState(): OptimisticState {
	return {
		items: new Map(),
		baseItems: new Map(),
		families: new Map(),
		pages: []
	};
}

export function pendingSnapshot(command: InternalPendingCommand): PendingCommand {
	return {
		mutationId: command.mutationId,
		method: command.method,
		state: command.state,
		targetIds: command.targetIds
	};
}

export function captureOptimisticState(state: OptimisticState): CachedOptimisticState {
	const baseItems =
		state.families.size > 0 || state.pages.length > 0 ? pageItems(state) : [...state.baseItems.values()];
	const families: CachedPageFamilyState[] = [];
	for (const family of state.families.values()) {
		families.push({
			key: family.key,
			query: family.query,
			pages: family.pages,
			meta: family.meta
		});
	}
	return {
		data: state.baseData,
		baseData: state.baseData,
		items: baseItems,
		baseItems,
		families,
		activeFamilyKey: state.activeFamilyKey,
		pages: state.pages,
		listMeta: state.listMeta,
		cursor: state.cursor
	};
}

export function restoreOptimisticState(
	state: OptimisticState,
	snapshot: CachedOptimisticState,
	reconcile: OptimisticReconcile
): void {
	state.baseData = snapshot.baseData ?? snapshot.data;
	state.data = state.baseData;
	state.items.clear();
	state.baseItems.clear();
	state.families.clear();
	state.activeFamilyKey = snapshot.activeFamilyKey;

	const restoredItems = new Map<string, unknown>();
	for (const item of snapshot.baseItems ?? snapshot.items) {
		const id = reconcile.itemId(item);
		if (id) {
			state.baseItems.set(id, item);
			restoredItems.set(id, item);
		}
	}

	if (snapshot.families) {
		for (const family of snapshot.families) {
			state.families.set(family.key, {
				key: family.key,
				query: family.query,
				pages: [...family.pages],
				meta: family.meta
			});
		}
	}

	const activeFamily = state.activeFamilyKey ? state.families.get(state.activeFamilyKey) : undefined;
	state.pages = activeFamily ? activeFamily.pages : [...snapshot.pages];
	state.listMeta = activeFamily ? activeFamily.meta : snapshot.listMeta;
	if (state.pages.length > 0) {
		for (const page of state.pages) {
			for (const id of page.ids) {
				const item = state.baseItems.get(id);
				if (item) {
					state.items.set(id, item);
				}
			}
		}
	} else {
		for (const [id, item] of restoredItems) {
			state.items.set(id, item);
		}
	}
	state.cursor = snapshot.cursor;
}

export function applyPageOutput(
	state: OptimisticState,
	output: unknown,
	query: unknown,
	reconcile: OptimisticReconcile,
	pendingCommands: Iterable<InternalPendingCommand>,
	activate = true
): void {
	if (!isRecord(output) || !Array.isArray(output.items)) {
		return;
	}

	const hasDefinedMeta = hasOwn(output, 'meta') && output.meta !== undefined;
	applyPageItems(
		state,
		output.items,
		query,
		readString(output.pageCursor),
		readString(output.syncCursor),
		hasDefinedMeta ? output.meta : undefined,
		hasDefinedMeta,
		'network',
		reconcile,
		pendingCommands,
		activate
	);
}

export function setBaseData(
	state: OptimisticState,
	value: unknown,
	reconcile: OptimisticReconcile,
	pendingCommands: Iterable<InternalPendingCommand>
): void {
	state.baseData = value;
	rebuildVisible(state, reconcile, pendingCommands);
}

export function selectQueryFamily(
	state: OptimisticState,
	query: unknown,
	reconcile: OptimisticReconcile,
	pendingCommands: Iterable<InternalPendingCommand>
): void {
	const familyKey = queryFamilyKey(query);
	state.activeFamilyKey = familyKey;
	const family = state.families.get(familyKey);
	state.pages = family?.pages ?? [];
	state.listMeta = family?.meta;
	rebuildVisible(state, reconcile, pendingCommands);
}

export function hasQueryFamily(state: OptimisticState, query: unknown): boolean {
	return state.families.has(queryFamilyKey(query));
}

export function createPendingCommand(
	method: WriteMethod,
	args: unknown,
	mutationId: string,
	reconcile: OptimisticReconcile
): InternalPendingCommand {
	const targetIds = collectTargetIds(method, args, reconcile);
	return {
		mutationId,
		method,
		state: 'local_accepted',
		targetIds,
		args
	};
}

export function ackPendingCommand(command: InternalPendingCommand): InternalPendingCommand {
	return {
		...command,
		state: 'acked_waiting_delta'
	};
}

export function applyEnvelope(
	state: OptimisticState,
	envelope: SyncEnvelope,
	reconcile: OptimisticReconcile,
	pendingCommands: Map<string, InternalPendingCommand>,
	activeQuery: unknown
): void {
	if (envelope.changes.some((change) => change.type === 'pageLoaded' || change.type === 'reset')) {
		applyEnvelopeWithRebuild(state, envelope, reconcile, pendingCommands, activeQuery);
		return;
	}

	const sourceCommand = envelope.sourceMutationId ? pendingCommands.get(envelope.sourceMutationId) : undefined;
	const affectedIds = new Set<string>();
	for (const syncChange of envelope.changes) {
		if (syncChange.type === 'itemAdded') {
			state.baseItems.set(syncChange.id, syncChange.value);
			if (sourceCommand?.method === 'add') {
				addToLoadedPage(state, syncChange.id);
			} else {
				for (const affectedId of reconcileAddedItemAcrossFamilies(
					state,
					syncChange.id,
					syncChange.value,
					reconcile
				)) {
					affectedIds.add(affectedId);
				}
			}
			affectedIds.add(syncChange.id);
		}
		if (syncChange.type === 'itemUpdated') {
			const current = state.baseItems.get(syncChange.id);
			const next = syncChange.value ?? applyPatch(current, syncChange.patch);
			state.baseItems.set(syncChange.id, next);

			if (state.baseData && reconcile.itemId(state.baseData) === syncChange.id) {
				state.baseData = syncChange.value ?? applyPatch(state.baseData, syncChange.patch);
			} else if (state.pages.length === 0 && syncChange.value) {
				state.baseData = syncChange.value;
			}
			for (const affectedId of reconcileUpdatedItemAcrossFamilies(state, syncChange.id, next, reconcile)) {
				affectedIds.add(affectedId);
			}
			affectedIds.add(syncChange.id);
		}
		if (syncChange.type === 'itemDeleted') {
			state.baseItems.delete(syncChange.id);
			if (state.baseData && reconcile.itemId(state.baseData) === syncChange.id) {
				state.baseData = undefined;
			}
			removeIdFromFamilies(state, syncChange.id);
			affectedIds.add(syncChange.id);
		}
	}

	state.cursor = envelope.cursor;
	syncActivePages(state);
	if (envelope.sourceMutationId) {
		pendingCommands.delete(envelope.sourceMutationId);
	}
	for (const id of sourceCommand?.targetIds ?? []) {
		affectedIds.add(id);
	}
	updateVisibleItems(state, affectedIds, reconcile, pendingCommands.values());
}

export function rebuildVisible(
	state: OptimisticState,
	reconcile: OptimisticReconcile,
	pendingCommands: Iterable<InternalPendingCommand>
): void {
	state.items.clear();
	if (state.pages.length > 0) {
		for (const page of state.pages) {
			for (const id of page.ids) {
				const item = state.baseItems.get(id);
				if (item) {
					state.items.set(id, item);
				}
			}
		}
	} else if (!state.activeFamilyKey && state.families.size === 0) {
		for (const [id, item] of state.baseItems) {
			state.items.set(id, item);
		}
	}

	state.data = state.baseData;

	for (const command of pendingCommands) {
		applyPendingToVisible(state, command, reconcile);
	}

	sortVisibleItems(state, reconcile);
}

export function itemsForQueryFamily(
	state: OptimisticState,
	query: unknown,
	reconcile: OptimisticReconcile,
	pendingCommands: Iterable<InternalPendingCommand>
): unknown[] {
	const family = state.families.get(queryFamilyKey(query));
	if (!family) {
		return [];
	}

	const scopedState: OptimisticState = {
		data: state.data,
		baseData: state.baseData,
		items: new Map(),
		baseItems: state.baseItems,
		families: new Map(),
		pages: family.pages,
		listMeta: family.meta,
		cursor: state.cursor
	};
	for (const page of family.pages) {
		for (const id of page.ids) {
			const item = state.baseItems.get(id);
			if (item) {
				scopedState.items.set(id, item);
			}
		}
	}
	for (const command of pendingCommands) {
		applyPendingToVisible(scopedState, command, reconcile, (item) =>
			pendingItemMatchesQuery(item, family.query, reconcile)
		);
	}
	sortVisibleItems(scopedState, reconcile);
	return [...scopedState.items.values()];
}

export function pagesForQueryFamily(state: OptimisticState, query: unknown): readonly PageState[] {
	return state.families.get(queryFamilyKey(query))?.pages ?? [];
}

export function metaForQueryFamily(state: OptimisticState, query: unknown): unknown {
	return state.families.get(queryFamilyKey(query))?.meta;
}

export function reconcileFromConfig(
	config: ReconcileConfig | undefined,
	getParams: () => unknown = () => undefined
): OptimisticReconcile {
	const reconcile: OptimisticReconcile = {
		itemId(item) {
			return config?.itemId?.(item, { params: getParams() }) ?? getDefaultId(item);
		},
		targetId(query, input) {
			return config?.targetId?.({ params: getParams(), query, input }) ?? getDefaultId(query);
		}
	};
	if (config?.matchesQuery) {
		reconcile.matchesQuery = (item, query) => config.matchesQuery?.(item, { params: getParams(), query }) ?? false;
	}
	if (config?.compare) {
		reconcile.compare = config.compare;
	}
	return reconcile;
}

export function applyPatch(current: unknown, patch: unknown): unknown {
	const base = isRecord(current) ? { ...current } : {};
	if (!isRecord(patch)) {
		return base;
	}
	const setValue = patch.$set;
	if (isRecord(setValue)) {
		// Patch application copies only the root and mutates touched paths; pending overlays avoid full rollback snapshots.
		for (const [key, value] of Object.entries(setValue)) {
			setPathMutable(base, key, value);
		}
	}
	const unsetValue = patch.$unset;
	if (Array.isArray(unsetValue)) {
		for (const key of unsetValue) {
			if (typeof key === 'string') {
				unsetPathMutable(base, key);
			}
		}
	}
	if (!('$set' in patch) && !('$unset' in patch)) {
		Object.assign(base, patch);
	}
	return base;
}

function setPathMutable(target: Record<string, unknown>, path: string, value: unknown): void {
	const parts = path.split('.');
	let cursor: Record<string, unknown> = target;
	for (let index = 0; index < parts.length - 1; index += 1) {
		const key = parts[index];
		if (!key) {
			return;
		}
		const current = cursor[key];
		if (isRecord(current)) {
			cursor = current;
			continue;
		}
		const next: Record<string, unknown> = {};
		cursor[key] = next;
		cursor = next;
	}
	const last = parts[parts.length - 1];
	if (last) {
		cursor[last] = value;
	}
}

function unsetPathMutable(target: Record<string, unknown>, path: string): void {
	const parts = path.split('.');
	let cursor: Record<string, unknown> = target;
	for (let index = 0; index < parts.length - 1; index += 1) {
		const key = parts[index];
		if (!key || !isRecord(cursor[key])) {
			return;
		}
		cursor = cursor[key] as Record<string, unknown>;
	}
	const last = parts[parts.length - 1];
	if (last) {
		delete cursor[last];
	}
}

function applyPageItems(
	state: OptimisticState,
	items: readonly unknown[],
	query: unknown,
	pageCursor: string | undefined,
	syncCursor: string | undefined,
	meta: unknown,
	hasMeta: boolean,
	source: 'cache' | 'network' | 'realtime',
	reconcile: OptimisticReconcile,
	pendingCommands: Iterable<InternalPendingCommand>,
	activate: boolean
): void {
	const familyKey = queryFamilyKey(query);
	const family = getOrCreateFamily(state, familyKey, queryWithoutCursor(query));
	const ids: string[] = [];
	for (const item of items) {
		const id = reconcile.itemId(item);
		if (!id) {
			continue;
		}
		state.baseItems.set(id, item);
		ids.push(id);
	}
	family.pages = nextPages(family.pages, {
		cursorIn: readStringFromRecord(query, 'cursor'),
		cursorOut: pageCursor,
		ids,
		coverage: 'full',
		complete: pageCursor === undefined,
		stale: false,
		repairNeeded: false,
		lastSyncCursor: syncCursor,
		source
	});
	if (hasMeta) {
		family.meta = meta;
	}
	if (activate) {
		state.activeFamilyKey = familyKey;
		state.pages = family.pages;
		state.listMeta = family.meta;
	} else {
		syncActivePages(state);
	}
	rebuildVisible(state, reconcile, pendingCommands);
}

function applyEnvelopeWithRebuild(
	state: OptimisticState,
	envelope: SyncEnvelope,
	reconcile: OptimisticReconcile,
	pendingCommands: Map<string, InternalPendingCommand>,
	activeQuery: unknown
): void {
	const sourceCommand = envelope.sourceMutationId ? pendingCommands.get(envelope.sourceMutationId) : undefined;
	for (const syncChange of envelope.changes) {
		if (syncChange.type === 'pageLoaded') {
			applyPageItems(
				state,
				syncChange.items,
				activeQuery,
				syncChange.pageCursor,
				syncChange.syncCursor ?? envelope.cursor,
				undefined,
				false,
				'network',
				reconcile,
				pendingCommands.values(),
				true
			);
		}
		if (syncChange.type === 'itemAdded') {
			state.baseItems.set(syncChange.id, syncChange.value);
			if (sourceCommand?.method === 'add') {
				addToLoadedPage(state, syncChange.id);
			} else {
				reconcileAddedItemAcrossFamilies(state, syncChange.id, syncChange.value, reconcile);
			}
		}
		if (syncChange.type === 'itemUpdated') {
			const current = state.baseItems.get(syncChange.id);
			const next = syncChange.value ?? applyPatch(current, syncChange.patch);
			state.baseItems.set(syncChange.id, next);

			if (state.baseData && reconcile.itemId(state.baseData) === syncChange.id) {
				state.baseData = syncChange.value ?? applyPatch(state.baseData, syncChange.patch);
			} else if (state.pages.length === 0 && syncChange.value) {
				state.baseData = syncChange.value;
			}
			reconcileUpdatedItemAcrossFamilies(state, syncChange.id, next, reconcile);
		}
		if (syncChange.type === 'itemDeleted') {
			state.baseItems.delete(syncChange.id);
			if (state.baseData && reconcile.itemId(state.baseData) === syncChange.id) {
				state.baseData = undefined;
			}
			removeIdFromFamilies(state, syncChange.id);
		}
		if (syncChange.type === 'reset') {
			pendingCommands.clear();
			markPagesStale(state, envelope.cursor);
		}
	}

	state.cursor = envelope.cursor;
	syncActivePages(state);
	if (envelope.sourceMutationId) {
		pendingCommands.delete(envelope.sourceMutationId);
	}
	rebuildVisible(state, reconcile, pendingCommands.values());
}

function nextPages(existing: readonly PageState[], page: PageState): PageState[] {
	if (!page.cursorIn) {
		return [page];
	}

	const pages: PageState[] = [];
	let replaced = false;
	for (const current of existing) {
		if (current.cursorIn === page.cursorIn) {
			pages.push(page);
			replaced = true;
		} else {
			pages.push(current);
		}
	}
	if (!replaced) {
		pages.push(page);
	}
	return pages;
}

function addToLoadedPage(state: OptimisticState, id: string): void {
	const family = getActiveFamily(state);
	const pages = family?.pages ?? state.pages;
	const firstPage = pages[0];
	if (!firstPage || firstPage.ids.includes(id)) {
		return;
	}
	const firstPageIds = new Array<string>(firstPage.ids.length + 1);
	for (let index = 0; index < firstPage.ids.length; index += 1) {
		firstPageIds[index] = firstPage.ids[index] as string;
	}
	firstPageIds[firstPage.ids.length] = id;
	const nextFamilyPages = new Array<PageState>(pages.length);
	nextFamilyPages[0] = {
		...firstPage,
		ids: firstPageIds
	};
	for (let index = 1; index < pages.length; index += 1) {
		nextFamilyPages[index] = pages[index] as PageState;
	}
	if (family) {
		family.pages = nextFamilyPages;
	}
	state.pages = nextFamilyPages;
}

function reconcileAddedItemAcrossFamilies(
	state: OptimisticState,
	id: string,
	item: unknown,
	reconcile: OptimisticReconcile
): readonly string[] {
	const affectedIds: string[] = [];
	for (const family of loadedFamilies(state)) {
		for (const affectedId of reconcileAddedItemWithFamily(state, family, id, item, reconcile)) {
			affectedIds.push(affectedId);
		}
	}
	return affectedIds;
}

function reconcileAddedItemWithFamily(
	state: OptimisticState,
	family: PageFamilyState,
	id: string,
	item: unknown,
	reconcile: OptimisticReconcile
): readonly string[] {
	if (family.pages.length === 0 || isLoadedInPages(family.pages, id)) {
		return [];
	}
	if (reconcile.matchesQuery && !reconcile.matchesQuery(item, family.query)) {
		return [];
	}

	const loadedIds = loadedPageIds(family.pages);
	const existingCount = loadedIds.length;
	if (existingCount === 0) {
		const firstPage = family.pages[0];
		if (
			firstPage &&
			family.pages.length === 1 &&
			firstPage.ids.length === 0 &&
			firstPage.complete &&
			firstPage.coverage === 'full' &&
			!firstPage.stale
		) {
			family.pages = [
				{
					...firstPage,
					ids: [id],
					source: 'realtime'
				}
			];
			return [id];
		}
		return [];
	}
	if (!reconcile.compare) {
		return [];
	}

	const insertedIndex = findInsertIndexByCompare(state, reconcile, loadedIds, id);
	const familyComplete = family.pages[family.pages.length - 1]?.complete === true;
	if (!familyComplete && insertedIndex >= existingCount) {
		return [];
	}

	let droppedId: string | undefined;
	if (familyComplete) {
		loadedIds.splice(insertedIndex, 0, id);
	} else {
		droppedId = loadedIds[existingCount - 1];
		for (let index = existingCount - 1; index > insertedIndex; index -= 1) {
			loadedIds[index] = loadedIds[index - 1] as string;
		}
		loadedIds[insertedIndex] = id;
	}
	family.pages = distributeLoadedIdsToPages(family.pages, loadedIds);
	if (droppedId) {
		return [id, droppedId];
	}
	return [id];
}

function reconcileUpdatedItemAcrossFamilies(
	state: OptimisticState,
	id: string,
	item: unknown,
	reconcile: OptimisticReconcile
): readonly string[] {
	const affectedIds: string[] = [];
	for (const family of loadedFamilies(state)) {
		const loaded = isLoadedInPages(family.pages, id);
		const matches = !reconcile.matchesQuery || reconcile.matchesQuery(item, family.query);
		if (loaded && !matches) {
			family.pages = removeIdFromPages(family.pages, id);
			affectedIds.push(id);
			continue;
		}
		if (!loaded && matches) {
			for (const affectedId of reconcileAddedItemWithFamily(state, family, id, item, reconcile)) {
				affectedIds.push(affectedId);
			}
			continue;
		}
		if (loaded && reconcile.compare) {
			const loadedIds = loadedPageIds(family.pages);
			moveLoadedIdByCompare(state, reconcile, loadedIds, id);
			family.pages = distributeLoadedIdsToPages(family.pages, loadedIds);
			affectedIds.push(id);
		}
	}
	return affectedIds;
}

function removeIdFromFamilies(state: OptimisticState, id: string): void {
	for (const family of loadedFamilies(state)) {
		if (isLoadedInPages(family.pages, id)) {
			family.pages = removeIdFromPages(family.pages, id);
		}
	}
}

function removeIdFromPages(pages: readonly PageState[], id: string): PageState[] {
	const nextPages: PageState[] = [];
	for (const page of pages) {
		if (!page.ids.includes(id)) {
			nextPages.push(page);
			continue;
		}
		nextPages.push({
			...page,
			ids: page.ids.filter((candidate) => candidate !== id),
			source: 'realtime'
		});
	}
	return nextPages;
}

function isLoadedInPages(pages: readonly PageState[], id: string): boolean {
	for (const page of pages) {
		if (page.ids.includes(id)) {
			return true;
		}
	}
	return false;
}

function loadedFamilies(state: OptimisticState): Iterable<PageFamilyState> {
	if (state.families.size > 0) {
		return state.families.values();
	}
	return [
		{
			key: queryFamilyKey(undefined),
			query: undefined,
			pages: state.pages
		}
	];
}

function loadedPageIds(pages: readonly PageState[]): string[] {
	const ids: string[] = [];
	const seen = new Set<string>();
	for (const page of pages) {
		for (const id of page.ids) {
			if (seen.has(id)) {
				continue;
			}
			seen.add(id);
			ids.push(id);
		}
	}
	return ids;
}

function compareItemsById(
	state: OptimisticState,
	reconcile: OptimisticReconcile,
	leftId: string,
	rightId: string
): number {
	const left = state.baseItems.get(leftId);
	const right = state.baseItems.get(rightId);
	if (left && right) {
		const compared = reconcile.compare?.(left, right) ?? 0;
		if (compared !== 0) {
			return compared;
		}
	}
	return leftId.localeCompare(rightId);
}

function findInsertIndexByCompare(
	state: OptimisticState,
	reconcile: OptimisticReconcile,
	ids: readonly string[],
	id: string
): number {
	let low = 0;
	let high = ids.length;
	while (low < high) {
		const middle = Math.floor((low + high) / 2);
		const candidate = ids[middle];
		if (!candidate || compareItemsById(state, reconcile, id, candidate) <= 0) {
			high = middle;
		} else {
			low = middle + 1;
		}
	}
	return low;
}

function moveLoadedIdByCompare(
	state: OptimisticState,
	reconcile: OptimisticReconcile,
	ids: string[],
	id: string
): void {
	const currentIndex = ids.indexOf(id);
	if (currentIndex < 0) {
		return;
	}
	ids.splice(currentIndex, 1);
	const nextIndex = findInsertIndexByCompare(state, reconcile, ids, id);
	ids.splice(nextIndex, 0, id);
}

function distributeLoadedIdsToPages(pages: readonly PageState[], ids: readonly string[]): PageState[] {
	const nextPages: PageState[] = [];
	let cursor = 0;
	for (const page of pages) {
		const pageIds: string[] = [];
		for (let index = 0; index < page.ids.length && cursor < ids.length; index += 1) {
			const id = ids[cursor];
			cursor += 1;
			if (id) {
				pageIds.push(id);
			}
		}
		nextPages.push({
			...page,
			ids: pageIds,
			source: 'realtime'
		});
	}
	const lastPage = nextPages[nextPages.length - 1];
	while (lastPage && cursor < ids.length) {
		const id = ids[cursor];
		cursor += 1;
		if (id) {
			(lastPage.ids as string[]).push(id);
		}
	}
	return nextPages;
}

function pageItems(state: OptimisticState): unknown[] {
	const items: unknown[] = [];
	const seen = new Set<string>();
	for (const family of loadedFamilies(state)) {
		for (const page of family.pages) {
			for (const id of page.ids) {
				if (seen.has(id)) {
					continue;
				}
				seen.add(id);
				const item = state.baseItems.get(id);
				if (item) {
					items.push(item);
				}
			}
		}
	}
	return items;
}

function syncActivePages(state: OptimisticState): void {
	const activeFamily = getActiveFamily(state);
	if (activeFamily) {
		state.pages = activeFamily.pages;
		state.listMeta = activeFamily.meta;
	}
}

function getActiveFamily(state: OptimisticState): PageFamilyState | undefined {
	return state.activeFamilyKey ? state.families.get(state.activeFamilyKey) : undefined;
}

function getOrCreateFamily(state: OptimisticState, key: string, query: unknown): PageFamilyState {
	const existing = state.families.get(key);
	if (existing) {
		existing.query = query;
		return existing;
	}
	const family = {
		key,
		query,
		pages: []
	};
	state.families.set(key, family);
	return family;
}

function hasOwn(value: object, key: string): boolean {
	return Object.prototype.hasOwnProperty.call(value, key);
}

function queryFamilyKey(query: unknown): string {
	return stableStringify(queryWithoutCursor(query));
}

function queryWithoutCursor(query: unknown): unknown {
	if (!isRecord(query)) {
		return query;
	}
	const next: Record<string, unknown> = {};
	for (const key of Object.keys(query).sort()) {
		if (key === 'cursor') {
			continue;
		}
		next[key] = toStableJson(query[key]);
	}
	return next;
}

function applyPendingToVisible(
	state: OptimisticState,
	command: InternalPendingCommand,
	reconcile: OptimisticReconcile,
	shouldApplyAdd?: (item: unknown) => boolean
): void {
	const items =
		command.method === 'delete' && command.args === undefined
			? [undefined]
			: Array.isArray(command.args)
				? command.args
				: [command.args];
	for (const item of items) {
		if (!isRecord(item)) {
			if (command.method === 'delete') {
				const id = reconcile.targetId(undefined, undefined);
				if (id) {
					state.items.delete(id);
					if (state.data && reconcile.itemId(state.data) === id) {
						state.data = undefined;
					}
				}
			}
			continue;
		}

		if (command.method === 'add') {
			const input = item.input;
			if (shouldApplyAdd && !shouldApplyAdd(input)) {
				continue;
			}
			const id = reconcile.itemId(input);
			if (id) {
				state.items.set(id, input);
			}
		}

		if (command.method === 'mutate') {
			const id = reconcile.targetId(item.query, item.input);
			if (id) {
				const current = state.items.get(id);
				if (current) {
					state.items.set(id, applyPatch(current, item.input));
				}
				if (state.data && reconcile.itemId(state.data) === id) {
					state.data = applyPatch(state.data, item.input);
				}
			}
		}

		if (command.method === 'delete') {
			const id = reconcile.targetId(item.query, undefined);
			if (id) {
				state.items.delete(id);
				if (state.data && reconcile.itemId(state.data) === id) {
					state.data = undefined;
				}
			}
		}
	}
}

function pendingItemMatchesQuery(item: unknown, query: unknown, reconcile: OptimisticReconcile): boolean {
	return !reconcile.matchesQuery || reconcile.matchesQuery(item, query);
}

function collectTargetIds(method: WriteMethod, args: unknown, reconcile: OptimisticReconcile): readonly string[] {
	const items = method === 'delete' && args === undefined ? [undefined] : Array.isArray(args) ? args : [args];
	const targetIds: string[] = [];
	for (const item of items) {
		if (!isRecord(item)) {
			if (method === 'delete') {
				const id = reconcile.targetId(undefined, undefined);
				if (id) {
					targetIds.push(id);
				}
			}
			continue;
		}
		const id = method === 'add' ? reconcile.itemId(item.input) : reconcile.targetId(item.query, item.input);
		if (id) {
			targetIds.push(id);
		}
	}
	return targetIds;
}

export function updateVisibleItems(
	state: OptimisticState,
	affectedIds: ReadonlySet<string>,
	reconcile: OptimisticReconcile,
	pendingCommands: Iterable<InternalPendingCommand>
): void {
	if (affectedIds.size === 0) {
		return;
	}

	const dataId = state.data ? reconcile.itemId(state.data) : undefined;
	if (dataId && affectedIds.has(dataId)) {
		state.data = state.baseData;
	} else if (state.baseData) {
		const baseDataId = reconcile.itemId(state.baseData);
		if (baseDataId && affectedIds.has(baseDataId)) {
			state.data = state.baseData;
		}
	}

	for (const id of affectedIds) {
		const next = state.baseItems.get(id);
		if (!next || !isLoadedItem(state, id)) {
			state.items.delete(id);
			continue;
		}
		state.items.set(id, next);
	}

	for (const command of pendingCommands) {
		if (command.targetIds.some((id) => affectedIds.has(id))) {
			applyPendingToVisible(state, command, reconcile);
		}
	}

	repositionVisibleItems(state, reconcile, affectedIds);
}

function sortVisibleItems(state: OptimisticState, reconcile: OptimisticReconcile): void {
	if (!reconcile.compare || state.items.size < 2) {
		return;
	}

	const sorted = [...state.items.entries()];
	sorted.sort((left, right) => reconcile.compare?.(left[1], right[1]) ?? 0);
	state.items.clear();
	for (const [id, item] of sorted) {
		state.items.set(id, item);
	}
}

function repositionVisibleItems(
	state: OptimisticState,
	reconcile: OptimisticReconcile,
	affectedIds: ReadonlySet<string>
): void {
	if (!reconcile.compare || state.items.size < 2 || affectedIds.size === 0) {
		return;
	}

	const movedEntries: [string, unknown][] = [];
	const stableEntries: [string, unknown][] = [];
	for (const entry of state.items.entries()) {
		if (affectedIds.has(entry[0])) {
			movedEntries.push(entry);
		} else {
			stableEntries.push(entry);
		}
	}
	if (movedEntries.length === 0) {
		return;
	}

	movedEntries.sort((left, right) => reconcile.compare?.(left[1], right[1]) ?? 0);
	const merged: [string, unknown][] = [];
	let stableIndex = 0;
	let movedIndex = 0;
	while (stableIndex < stableEntries.length && movedIndex < movedEntries.length) {
		if ((reconcile.compare?.(movedEntries[movedIndex]?.[1], stableEntries[stableIndex]?.[1]) ?? 0) <= 0) {
			const moved = movedEntries[movedIndex];
			if (moved) {
				merged.push(moved);
			}
			movedIndex += 1;
		} else {
			const stable = stableEntries[stableIndex];
			if (stable) {
				merged.push(stable);
			}
			stableIndex += 1;
		}
	}
	for (; stableIndex < stableEntries.length; stableIndex += 1) {
		const stable = stableEntries[stableIndex];
		if (stable) {
			merged.push(stable);
		}
	}
	for (; movedIndex < movedEntries.length; movedIndex += 1) {
		const moved = movedEntries[movedIndex];
		if (moved) {
			merged.push(moved);
		}
	}

	state.items.clear();
	for (const [id, item] of merged) {
		state.items.set(id, item);
	}
}

function markPagesStale(state: OptimisticState, cursor: string): void {
	if (state.families.size === 0) {
		state.pages = markPageListStale(state.pages, cursor);
		return;
	}
	for (const family of state.families.values()) {
		family.pages = markPageListStale(family.pages, cursor);
	}
	syncActivePages(state);
}

function getDefaultId(value: unknown): string | undefined {
	if (!isRecord(value)) {
		return undefined;
	}
	return typeof value.id === 'string' ? value.id : undefined;
}

function readString(value: unknown): string | undefined {
	return typeof value === 'string' ? value : undefined;
}

function readStringFromRecord(value: unknown, key: string): string | undefined {
	if (!isRecord(value)) {
		return undefined;
	}
	return readString(value[key]);
}

function isLoadedItem(state: OptimisticState, id: string): boolean {
	if (state.pages.length === 0) {
		return state.families.size === 0 && !state.activeFamilyKey;
	}
	return state.pages.some((page) => page.ids.includes(id));
}

function markPageListStale(pages: readonly PageState[], cursor: string): PageState[] {
	const nextPages: PageState[] = [];
	for (const page of pages) {
		nextPages.push({
			...page,
			coverage: 'stale',
			stale: true,
			repairNeeded: true,
			lastSyncCursor: cursor,
			source: 'realtime'
		});
	}
	return nextPages;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}
