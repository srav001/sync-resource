import { isRecord, isString, readString, type ValueRecord } from '../shared/guards.ts';
import type { SyncEnvelope } from '../shared/index.ts';
import { stableStringify, toStableJson } from '../shared/stableJson.ts';
import type { PageState, ReconcileConfig } from './types.ts';

const MAX_FAMILIES = 8;

export type WriteMethod = 'add' | 'mutate' | 'delete';
type PendingState = 'local_accepted' | 'acked_waiting_delta';

export interface PendingCommand {
	readonly mutationId: string;
	readonly method: WriteMethod;
	readonly state: PendingState;
	readonly targetIds: readonly string[];
}

export interface InternalPendingCommand<TArgs = unknown> extends PendingCommand {
	readonly args: TArgs;
}

interface PendingItems<TItem> {
	get(id: string): TItem | undefined;
	set(id: string, item: TItem): unknown;
	delete(id: string): unknown;
}

interface PendingView<TItem = unknown> {
	readonly items: PendingItems<TItem>;
	data: unknown;
}

export interface PageMeta extends Omit<PageState, 'ids'> {
	readonly count: number;
}

// A family keeps its loaded ids and items as two aligned flat arrays; pages only record how many ids each page owns.
export interface PageFamilyState<TItem = unknown, TQuery = unknown, TMeta = unknown> {
	readonly key: string;
	query: TQuery;
	meta?: TMeta;
	pages: readonly PageMeta[];
	ids: string[];
	items: TItem[];
	sorted: boolean;
	itemsView?: readonly TItem[];
	pagesView?: readonly PageState[];
}

export interface OptimisticState<TItem = unknown, TData = unknown, TQuery = unknown, TMeta = unknown> {
	data?: TData;
	baseData?: TData;
	readonly baseItems: Map<string, TItem>;
	readonly families: Map<string, PageFamilyState<TItem, TQuery, TMeta>>;
	readonly familyRefs: Map<string, number>;
	activeFamilyKey?: string;
	recentFamilyKey?: string;
	cursor?: string;
}

interface CachedPageFamilyState<TQuery = unknown, TMeta = unknown> {
	readonly key: string;
	readonly query: TQuery;
	readonly meta?: TMeta;
	readonly pages: readonly PageMeta[];
	readonly ids: readonly string[];
	readonly sorted: boolean;
}

export interface CachedOptimisticState<TItem = unknown, TData = unknown, TQuery = unknown, TMeta = unknown> {
	readonly baseData?: TData;
	readonly baseItems: readonly TItem[];
	readonly families: readonly CachedPageFamilyState<TQuery, TMeta>[];
	readonly activeFamilyKey?: string;
	readonly cursor?: string;
}

export interface OptimisticReconcile<TItem = unknown, TQuery = unknown, TInput = unknown> {
	itemId(item: TItem): string | undefined;
	targetId(query: TQuery, input: TInput): string | undefined;
	matchesQuery?(item: TItem, query: TQuery): boolean;
	compare?(this: void, left: TItem, right: TItem): number;
	addPlacement?: 'sorted';
}

export function createOptimisticState<TItem, TData, TQuery, TMeta>(): OptimisticState<TItem, TData, TQuery, TMeta> {
	return {
		baseItems: new Map(),
		families: new Map(),
		familyRefs: new Map()
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
	return {
		baseData: state.baseData,
		baseItems: [...state.baseItems.values()],
		families: [...state.families.values()].map((family) => ({
			key: family.key,
			query: family.query,
			meta: family.meta,
			pages: family.pages,
			ids: [...family.ids],
			sorted: family.sorted
		})),
		activeFamilyKey: state.activeFamilyKey,
		cursor: state.cursor
	};
}

export function restoreOptimisticState(
	state: OptimisticState,
	snapshot: CachedOptimisticState,
	reconcile: OptimisticReconcile
): void {
	state.baseData = snapshot.baseData;
	state.data = state.baseData;
	state.baseItems.clear();
	state.families.clear();
	state.familyRefs.clear();
	state.recentFamilyKey = undefined;
	state.activeFamilyKey = snapshot.activeFamilyKey;
	for (const item of snapshot.baseItems) {
		const id = reconcile.itemId(item);
		if (id) {
			state.baseItems.set(id, item);
		}
	}
	for (const family of snapshot.families) {
		const ids = [...family.ids];
		ids.forEach((id) => retain(state, id));
		state.families.set(family.key, {
			key: family.key,
			query: family.query,
			meta: family.meta,
			pages: family.pages,
			ids,
			items: ids.map((id) => state.baseItems.get(id)),
			sorted: family.sorted
		});
	}
	state.cursor = snapshot.cursor;
}

export function applyPageOutput<TOutput, TQuery>(
	state: OptimisticState,
	output: TOutput,
	query: TQuery,
	reconcile: OptimisticReconcile,
	activate = true
): void {
	if (!isRecord(output) || !Array.isArray(output.items)) {
		return;
	}

	applyPageItems(
		state,
		output.items,
		query,
		readString(output.pageCursor),
		readString(output.syncCursor),
		output.meta,
		reconcile,
		activate
	);
}

export function setBaseData<TValue>(
	state: OptimisticState,
	value: TValue,
	reconcile: OptimisticReconcile,
	pendingCommands: Iterable<InternalPendingCommand>
): void {
	state.baseData = value;
	refreshData(state, reconcile, pendingCommands);
}

export function selectQueryFamily<TQuery>(state: OptimisticState, query: TQuery): void {
	state.activeFamilyKey = queryFamilyKey(query);
}

export function hasQueryFamily<TQuery>(state: OptimisticState, query: TQuery): boolean {
	return state.families.has(queryFamilyKey(query));
}

export function createPendingCommand<TArgs>(
	method: WriteMethod,
	args: TArgs,
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

export function applyEnvelope<TQuery>(
	state: OptimisticState,
	envelope: SyncEnvelope,
	reconcile: OptimisticReconcile,
	pendingCommands: Map<string, InternalPendingCommand>,
	activeQuery: TQuery
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
				reconcile,
				true
			);
		}
		if (syncChange.type === 'itemAdded') {
			const appendToPage = sourceCommand?.method === 'add' && reconcile.addPlacement !== 'sorted';
			reconcileAddedItemAcrossFamilies(state, syncChange.id, syncChange.value, reconcile, appendToPage);
			storeIfReferenced(state, syncChange.id, syncChange.value);
		}
		if (syncChange.type === 'itemUpdated') {
			const current = state.baseItems.get(syncChange.id);
			const next = syncChange.value ?? (current && applyPatch(current, syncChange.patch));

			if (state.baseData && reconcile.itemId(state.baseData) === syncChange.id) {
				state.baseData = syncChange.value ?? applyPatch(state.baseData, syncChange.patch);
			} else if (activePageMetas(state).length === 0 && syncChange.value) {
				state.baseData = syncChange.value;
			}
			if (next) {
				reconcileUpdatedItemAcrossFamilies(state, syncChange.id, next, reconcile);
				storeIfReferenced(state, syncChange.id, next);
			}
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
	if (envelope.sourceMutationId) {
		pendingCommands.delete(envelope.sourceMutationId);
	}
	refreshData(state, reconcile, pendingCommands.values());
}

export function refreshData(
	state: OptimisticState,
	reconcile: OptimisticReconcile,
	pendingCommands: Iterable<InternalPendingCommand>
): void {
	const view: PendingView = { items: new Map(), data: state.baseData };
	for (const command of pendingCommands) {
		applyPendingToView(view, command, reconcile);
	}
	state.data = view.data;
}

export function activeItems<TItem>(
	state: OptimisticState<TItem>,
	reconcile: OptimisticReconcile<TItem>,
	pendingCommands: Iterable<InternalPendingCommand>
): readonly TItem[] {
	const family = getActiveFamily(state);
	if (family || state.activeFamilyKey || state.families.size > 0) {
		return familyItems(family, reconcile, pendingCommands);
	}
	return overlayItems([...state.baseItems.keys()], [...state.baseItems.values()], reconcile, [...pendingCommands]);
}

export function itemsForQueryFamily<TItem, TQuery>(
	state: OptimisticState<TItem>,
	query: TQuery,
	reconcile: OptimisticReconcile<TItem>,
	pendingCommands: Iterable<InternalPendingCommand>
): readonly TItem[] {
	const family = readFamily(state, queryFamilyKey(query));
	if (!family) {
		return [];
	}
	return familyItems(family, reconcile, pendingCommands, (item) =>
		pendingItemMatchesQuery(item, family.query, reconcile)
	);
}

// Without pending commands the family's own array is the answer; with them, the overlay is applied over a copy.
function familyItems<TItem>(
	family: PageFamilyState<TItem> | undefined,
	reconcile: OptimisticReconcile<TItem>,
	pendingCommands: Iterable<InternalPendingCommand>,
	shouldApplyAdd?: (item: TItem) => boolean
): readonly TItem[] {
	const commands = [...pendingCommands];
	if (commands.length === 0 && family) {
		family.itemsView ??= family.sorted ? [...family.items] : [...family.items].sort(reconcile.compare);
		return family.itemsView;
	}
	return overlayItems(family?.ids ?? [], family?.items ?? [], reconcile, commands, shouldApplyAdd);
}

// Pending commands are applied over a copy that is indexed only for the ids they target; a removed entry leaves a gap.
function overlayItems<TItem>(
	ids: readonly string[],
	items: readonly TItem[],
	reconcile: OptimisticReconcile<TItem>,
	commands: readonly InternalPendingCommand[],
	shouldApplyAdd?: (item: TItem) => boolean
): TItem[] {
	const targets = new Set(commands.flatMap((command) => command.targetIds));
	const visible: (TItem | undefined)[] = [...items];
	const index = new Map<string, number>();
	ids.forEach((id, position) => targets.has(id) && index.set(id, position));
	const overlay: PendingItems<TItem> = {
		get: (id) => visible[index.get(id) ?? -1],
		set(id, item) {
			const position = index.get(id);
			if (position === undefined) {
				index.set(id, visible.length);
				visible.push(item);
			} else {
				visible[position] = item;
			}
		},
		delete(id) {
			const position = index.get(id);
			if (position !== undefined) {
				visible[position] = undefined;
				index.delete(id);
			}
		}
	};
	for (const command of commands) {
		applyPendingToView({ items: overlay, data: undefined }, command, reconcile, shouldApplyAdd);
	}
	const result = visible.filter((item) => item !== undefined);
	return reconcile.compare ? result.sort(reconcile.compare) : result;
}

export function activePageMetas(state: OptimisticState): readonly PageMeta[] {
	return getActiveFamily(state)?.pages ?? [];
}

export function activeListMeta<TMeta>(state: OptimisticState<unknown, unknown, unknown, TMeta>): TMeta | undefined {
	return getActiveFamily(state)?.meta;
}

export function activePages(state: OptimisticState): readonly PageState[] {
	const family = getActiveFamily(state);
	return family ? pagesOf(family) : [];
}

export function pagesForQueryFamily<TQuery>(state: OptimisticState, query: TQuery): readonly PageState[] {
	const family = readFamily(state, queryFamilyKey(query));
	return family ? pagesOf(family) : [];
}

export function pageMetasForQueryFamily<TQuery>(state: OptimisticState, query: TQuery): readonly PageMeta[] {
	return readFamily(state, queryFamilyKey(query))?.pages ?? [];
}

export function metaForQueryFamily<TQuery, TMeta>(
	state: OptimisticState<unknown, unknown, TQuery, TMeta>,
	query: TQuery
): TMeta | undefined {
	return readFamily(state, queryFamilyKey(query))?.meta;
}

function pagesOf(family: PageFamilyState): readonly PageState[] {
	if (!family.pagesView) {
		let start = 0;
		family.pagesView = family.pages.map(({ count, ...page }) => {
			const ids = family.ids.slice(start, start + count);
			start += count;
			return { ...page, ids };
		});
	}
	return family.pagesView;
}

export function reconcileFromConfig<TItem, TQuery, TInput, TParams>(
	config: ReconcileConfig<TItem, TParams, TQuery, TInput> | undefined,
	getParams: () => TParams
): OptimisticReconcile<TItem, TQuery, TInput> {
	const reconcile: OptimisticReconcile<TItem, TQuery, TInput> = {
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
	if (config?.addPlacement) {
		reconcile.addPlacement = config.addPlacement;
	}
	return reconcile;
}

function applyPatch<TCurrent, TPatch>(current: TCurrent, patch: TPatch) {
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
			if (isString(key)) {
				unsetPathMutable(base, key);
			}
		}
	}
	if (!('$set' in patch) && !('$unset' in patch)) {
		Object.assign(base, patch);
	}
	return base;
}

function setPathMutable<TValue>(target: ValueRecord, path: string, value: TValue): void {
	const parts = path.split('.');
	let cursor = target;
	for (let index = 0; index < parts.length - 1; index += 1) {
		const key = parts[index];
		if (!key) {
			return;
		}
		const current = Object.getOwnPropertyDescriptor(cursor, key)?.value;
		if (isRecord(current)) {
			cursor = current;
			continue;
		}
		const next = {};
		Object.defineProperty(cursor, key, { configurable: true, enumerable: true, value: next, writable: true });
		cursor = next;
	}
	const last = parts[parts.length - 1];
	if (last) {
		Object.defineProperty(cursor, last, { configurable: true, enumerable: true, value, writable: true });
	}
}

function unsetPathMutable(target: ValueRecord, path: string): void {
	const parts = path.split('.');
	let cursor = target;
	for (let index = 0; index < parts.length - 1; index += 1) {
		const key = parts[index];
		if (!key) {
			return;
		}
		const next = Object.getOwnPropertyDescriptor(cursor, key)?.value;
		if (!isRecord(next)) {
			return;
		}
		cursor = next;
	}
	const last = parts[parts.length - 1];
	if (last) {
		Reflect.deleteProperty(cursor, last);
	}
}

function applyPageItems<TItem, TQuery, TMeta>(
	state: OptimisticState<TItem>,
	items: readonly TItem[],
	query: TQuery,
	pageCursor: string | undefined,
	syncCursor: string | undefined,
	meta: TMeta,
	reconcile: OptimisticReconcile<TItem, TQuery>,
	activate: boolean
): void {
	const familyKey = queryFamilyKey(query);
	const family = getOrCreateFamily(state, familyKey, queryWithoutCursor(query));
	const cursorIn = isRecord(query) ? readString(query.cursor) : undefined;
	let ids: string[] = [];
	let pageValues: TItem[] = [];
	let seenBefore = false;
	for (const item of items) {
		const id = reconcile.itemId(item);
		if (!id) {
			continue;
		}
		seenBefore ||= state.baseItems.has(id);
		state.baseItems.set(id, item);
		ids.push(id);
		pageValues.push(item);
	}
	// The new page replaces the pages' range of ids in the flat arrays; without a cursor it replaces everything.
	const found = cursorIn ? family.pages.findIndex((current) => current.cursorIn === cursorIn) : -1;
	let replaceStart = cursorIn ? family.ids.length : 0;
	let replaceEnd = family.ids.length;
	if (found >= 0) {
		replaceStart = family.pages.slice(0, found).reduce((total, current) => total + current.count, 0);
		replaceEnd = replaceStart + (family.pages[found]?.count ?? 0);
	}
	if (cursorIn && seenBefore) {
		// Loaded ids stay unique across a family's pages, so realtime-added items are not repeated by a later page.
		const loaded = new Set([...family.ids.slice(0, replaceStart), ...family.ids.slice(replaceEnd)]);
		const keep = ids.map((id) => !loaded.has(id));
		pageValues = pageValues.filter((_, index) => keep[index]);
		ids = ids.filter((_, index) => keep[index]);
	}
	const page: PageMeta = {
		cursorIn,
		cursorOut: pageCursor,
		count: ids.length,
		coverage: 'full',
		complete: pageCursor === undefined,
		stale: false,
		repairNeeded: false,
		lastSyncCursor: syncCursor,
		source: 'network'
	};
	const replaced = family.ids.slice(replaceStart, replaceEnd);
	replaceRange(family.ids, replaceStart, replaceEnd, ids);
	replaceRange(family.items, replaceStart, replaceEnd, pageValues);
	ids.forEach((id) => retain(state, id));
	replaced.forEach((id) => release(state, id));
	if (!cursorIn) {
		family.pages = [page];
	} else if (found >= 0) {
		family.pages = family.pages.map((current, index) => (index === found ? page : current));
	} else {
		family.pages = [...family.pages, page];
	}
	if (seenBefore) {
		refreshLoadedItems(state, reconcile);
	}
	if (meta !== undefined) {
		family.meta = meta;
	}
	family.sorted = (cursorIn ? family.sorted : true) && isSortedFrom(family, replaceStart, reconcile.compare);
	touch(family);
	if (activate) {
		state.activeFamilyKey = familyKey;
	}
}

// A page appended after the last one costs only its own size; a page replaced in the middle moves the tail.
function replaceRange<TValue>(target: TValue[], start: number, end: number, values: readonly TValue[]): void {
	const tail = target.splice(end);
	target.length = start;
	for (const value of values) {
		target.push(value);
	}
	for (const value of tail) {
		target.push(value);
	}
}

// Reconcile operations keep ids in comparator order; a page that arrives out of order is sorted only in the visible view.
function isSortedFrom(family: PageFamilyState, from: number, compare: OptimisticReconcile['compare']): boolean {
	for (let index = Math.max(from, 1); index < family.items.length; index += 1) {
		if ((compare?.(family.items[index - 1], family.items[index]) ?? 0) > 0) {
			return false;
		}
	}
	return true;
}

// A page can carry newer values for ids other families (or this one, outside the page) already hold.
function refreshLoadedItems<TItem>(state: OptimisticState<TItem>, reconcile: OptimisticReconcile<TItem>): void {
	for (const family of state.families.values()) {
		let changed = false;
		family.ids.forEach((id, index) => {
			const current = state.baseItems.get(id);
			if (current !== family.items[index]) {
				family.items[index] = current as TItem;
				changed = true;
			}
		});
		if (changed) {
			family.itemsView = undefined;
			family.sorted = family.sorted && isSortedFrom(family, 0, reconcile.compare);
		}
	}
}

function touch(family: PageFamilyState): void {
	family.itemsView = undefined;
	family.pagesView = undefined;
}

// An add for an id a family already holds only refreshes its value; the family order is left as it was.
function reconcileAddedItemAcrossFamilies<TItem>(
	state: OptimisticState<TItem>,
	id: string,
	item: TItem,
	reconcile: OptimisticReconcile<TItem>,
	appendToPage: boolean
): void {
	const active = getActiveFamily(state);
	for (const family of state.families.values()) {
		const index = family.ids.indexOf(id);
		if (index >= 0) {
			setLoadedItem(family, index, item, reconcile);
		} else if (!appendToPage) {
			reconcileAddedItemWithFamily(state, family, id, item, reconcile);
		} else if (family === active && family.pages[0]) {
			const firstPage = family.pages[0];
			family.ids.splice(firstPage.count, 0, id);
			family.items.splice(firstPage.count, 0, item);
			family.pages = [{ ...firstPage, count: firstPage.count + 1 }, ...family.pages.slice(1)];
			retain(state, id);
			touch(family);
		}
	}
}

function setLoadedItem<TItem>(
	family: PageFamilyState<TItem>,
	index: number,
	item: TItem,
	reconcile: OptimisticReconcile<TItem>
): void {
	family.items[index] = item;
	family.itemsView = undefined;
	const compare = reconcile.compare;
	if (compare && family.sorted) {
		const previous = family.items[index - 1];
		const next = family.items[index + 1];
		family.sorted =
			(previous === undefined || compare(previous, item) <= 0) &&
			(next === undefined || compare(item, next) <= 0);
	}
}

function reconcileAddedItemWithFamily<TItem>(
	state: OptimisticState<TItem>,
	family: PageFamilyState<TItem>,
	id: string,
	item: TItem,
	reconcile: OptimisticReconcile<TItem>
): void {
	if (family.pages.length === 0) {
		return;
	}
	if (reconcile.matchesQuery && !reconcile.matchesQuery(item, family.query)) {
		return;
	}

	const existingCount = family.ids.length;
	if (existingCount === 0) {
		const firstPage = family.pages[0];
		if (
			firstPage &&
			family.pages.length === 1 &&
			firstPage.complete &&
			firstPage.coverage === 'full' &&
			!firstPage.stale
		) {
			family.ids = [id];
			family.items = [item];
			family.pages = [{ ...firstPage, count: 1, source: 'realtime' }];
			retain(state, id);
			touch(family);
		}
		return;
	}
	if (!reconcile.compare) {
		return;
	}

	const insertedIndex = findInsertIndex(family, reconcile, item, id);
	const familyComplete = family.pages[family.pages.length - 1]?.complete === true;
	if (!familyComplete && insertedIndex >= existingCount) {
		return;
	}

	family.ids.splice(insertedIndex, 0, id);
	family.items.splice(insertedIndex, 0, item);
	retain(state, id);
	// Sorted opt-in additions must keep the existing cursor boundary reachable by a later loadMore.
	const dropped = !familyComplete && reconcile.addPlacement !== 'sorted';
	if (dropped) {
		family.items.pop();
		const droppedId = family.ids.pop();
		if (droppedId) {
			release(state, droppedId);
		}
	}
	touch(family);
	setRealtimePages(family, dropped ? 0 : 1);
}

function reconcileUpdatedItemAcrossFamilies<TItem>(
	state: OptimisticState<TItem>,
	id: string,
	item: TItem,
	reconcile: OptimisticReconcile<TItem>
): void {
	for (const family of state.families.values()) {
		const index = family.ids.indexOf(id);
		const matches = !reconcile.matchesQuery || reconcile.matchesQuery(item, family.query);
		if (index >= 0 && !matches) {
			removeAt(state, family, index);
		} else if (index < 0 && matches) {
			reconcileAddedItemWithFamily(state, family, id, item, reconcile);
		} else if (index >= 0) {
			family.items[index] = item;
			family.itemsView = undefined;
			if (reconcile.compare) {
				moveByCompare(family, reconcile, index, item, id);
			}
		}
	}
}

function removeIdFromFamilies(state: OptimisticState, id: string): void {
	for (const family of state.families.values()) {
		const index = family.ids.indexOf(id);
		if (index >= 0) {
			removeAt(state, family, index);
		}
	}
}

function removeAt(state: OptimisticState, family: PageFamilyState, index: number): void {
	const [id] = family.ids.splice(index, 1);
	if (id) {
		release(state, id);
	}
	family.items.splice(index, 1);
	let end = 0;
	family.pages = family.pages.map((page) => {
		const start = end;
		end += page.count;
		return index >= start && index < end ? { ...page, count: page.count - 1, source: 'realtime' } : page;
	});
	touch(family);
}

// Every page is marked realtime and the last page absorbs the size change, so the other pages keep their sizes.
function setRealtimePages(family: PageFamilyState, lastPageGrowth: number): void {
	if (lastPageGrowth === 0 && family.pages.every((page) => page.source === 'realtime')) {
		return;
	}
	const last = family.pages.length - 1;
	family.pages = family.pages.map((page, index) => ({
		...page,
		count: index === last ? page.count + lastPageGrowth : page.count,
		source: 'realtime'
	}));
	family.pagesView = undefined;
}

function moveByCompare<TItem>(
	family: PageFamilyState<TItem>,
	reconcile: OptimisticReconcile<TItem>,
	index: number,
	item: TItem,
	id: string
): void {
	family.ids.splice(index, 1);
	family.items.splice(index, 1);
	const nextIndex = findInsertIndex(family, reconcile, item, id);
	family.ids.splice(nextIndex, 0, id);
	family.items.splice(nextIndex, 0, item);
	if (nextIndex !== index) {
		touch(family);
	}
	setRealtimePages(family, 0);
}

function findInsertIndex<TItem>(
	family: PageFamilyState<TItem>,
	reconcile: OptimisticReconcile<TItem>,
	item: TItem,
	id: string
): number {
	let low = 0;
	let high = family.ids.length;
	while (low < high) {
		const middle = Math.floor((low + high) / 2);
		const compared = reconcile.compare?.(item, family.items[middle] as TItem) ?? 0;
		if (compared < 0 || (compared === 0 && id.localeCompare(family.ids[middle] as string) <= 0)) {
			high = middle;
		} else {
			low = middle + 1;
		}
	}
	return low;
}

function getActiveFamily<TItem, TQuery, TMeta>(
	state: OptimisticState<TItem, unknown, TQuery, TMeta>
): PageFamilyState<TItem, TQuery, TMeta> | undefined {
	return state.activeFamilyKey ? readFamily(state, state.activeFamilyKey) : undefined;
}

// Map order is recency: a read or page load moves the family to the end, so eviction takes the first.
export function readFamily<TItem, TQuery, TMeta>(
	state: OptimisticState<TItem, unknown, TQuery, TMeta>,
	key: string
): PageFamilyState<TItem, TQuery, TMeta> | undefined {
	const family = state.families.get(key);
	if (family && state.recentFamilyKey !== key) {
		state.families.delete(key);
		state.families.set(key, family);
		state.recentFamilyKey = key;
	}
	return family;
}

// While families exist, baseItems holds exactly the items some family holds; familyRefs counts the families per id.
function retain(state: OptimisticState, id: string): void {
	state.familyRefs.set(id, (state.familyRefs.get(id) ?? 0) + 1);
}

function release(state: OptimisticState, id: string): void {
	const count = state.familyRefs.get(id) ?? 0;
	if (count > 1) {
		state.familyRefs.set(id, count - 1);
	} else {
		state.familyRefs.delete(id);
		state.baseItems.delete(id);
	}
}

function storeIfReferenced<TItem>(state: OptimisticState<TItem>, id: string, item: TItem): void {
	if (state.families.size === 0 || state.familyRefs.has(id)) {
		state.baseItems.set(id, item);
	}
}

function getOrCreateFamily<TItem, TQuery>(
	state: OptimisticState<TItem>,
	key: string,
	query: TQuery
): PageFamilyState<TItem> {
	const existing = readFamily(state, key);
	if (existing) {
		existing.query = query;
		return existing;
	}
	if (state.families.size === 0) {
		state.baseItems.clear();
	}
	const family: PageFamilyState<TItem> = { key, query, pages: [], ids: [], items: [], sorted: true };
	state.families.set(key, family);
	state.recentFamilyKey = key;
	if (state.families.size > MAX_FAMILIES) {
		for (const [oldestKey, oldest] of state.families) {
			if (oldestKey !== state.activeFamilyKey) {
				state.families.delete(oldestKey);
				oldest.ids.forEach((id) => release(state, id));
				break;
			}
		}
	}
	return family;
}

function queryFamilyKey<TQuery>(query: TQuery): string {
	return stableStringify(queryWithoutCursor(query));
}

function queryWithoutCursor<TQuery>(query: TQuery) {
	if (!isRecord(query)) {
		return query;
	}
	const entries: [string, ReturnType<typeof toStableJson>][] = [];
	for (const key of Object.keys(query).sort()) {
		if (key !== 'cursor') {
			entries.push([key, toStableJson(query[key])]);
		}
	}
	return Object.fromEntries(entries);
}

function applyPendingToView<TItem>(
	state: PendingView<TItem>,
	command: InternalPendingCommand,
	reconcile: OptimisticReconcile,
	shouldApplyAdd?: (item: TItem) => boolean
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
			// Automatic optimistic adds use the client-known input as the collection item.
			const input = item.input as TItem;
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
					state.items.set(id, applyPatch(current, item.input) as TItem);
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

function pendingItemMatchesQuery<TItem, TQuery>(item: TItem, query: TQuery, reconcile: OptimisticReconcile): boolean {
	return !reconcile.matchesQuery || reconcile.matchesQuery(item, query);
}

function collectTargetIds<TArgs>(method: WriteMethod, args: TArgs, reconcile: OptimisticReconcile): readonly string[] {
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

function markPagesStale(state: OptimisticState, cursor: string): void {
	for (const family of state.families.values()) {
		family.pages = family.pages.map((page) => ({
			...page,
			coverage: 'stale',
			stale: true,
			repairNeeded: true,
			lastSyncCursor: cursor,
			source: 'realtime'
		}));
		family.pagesView = undefined;
	}
}

function getDefaultId<TValue>(value: TValue): string | undefined {
	if (!isRecord(value)) {
		return undefined;
	}
	return isString(value.id) ? value.id : undefined;
}
