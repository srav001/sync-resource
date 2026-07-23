import {
	configureSync,
	createStore,
	type CacheAdapter,
	type CacheItem,
	type ClientStore,
	type RuntimeTransport,
	type SyncError
} from '../../src/client/core.ts';
import {
	manager,
	ok,
	resource,
	type ManagerHttpHandlerArgs,
	type ManagerMutationRecord,
	type ManagerOutboxRead,
	type ManagerSyncPersistence,
	type SyncEnvelope,
	type SyncResult,
	type Validator
} from '../../src/server/index.ts';
import {
	parseSseEvent,
	parseSyncEnvelopeChunkJson,
	parseSyncEnvelopeJson,
	type SyncEnvelopeChunk
} from '../../src/shared/sse.ts';

export const SYNC_TEST_NOW = 1_700_000_000_000;

export interface Note {
	readonly id: string;
	readonly title: string;
}

export interface NotesPage {
	readonly items: readonly Note[];
}

export interface NotesParams {
	readonly workspaceId: string;
}

export interface NotesListQuery {
	readonly limit: number;
}

interface AddNoteInput {
	readonly id: string;
	readonly title: string;
}

interface MutateNoteInput {
	readonly title: string;
}

interface NoteQuery {
	readonly id: string;
}

interface NoteTombstone {
	readonly id: string;
	readonly deleted: true;
}

export class Deferred<TValue = void> {
	readonly promise: Promise<TValue>;
	private resolvePromise!: (value: TValue | PromiseLike<TValue>) => void;
	private rejectPromise!: (cause?: unknown) => void;

	constructor() {
		this.promise = new Promise<TValue>((resolve, reject) => {
			this.resolvePromise = resolve;
			this.rejectPromise = reject;
		});
	}

	resolve(value: TValue): void {
		this.resolvePromise(value);
	}

	reject(cause?: unknown): void {
		this.rejectPromise(cause);
	}
}

export class InMemoryNotesDatabase {
	readonly operations: string[] = [];
	private readonly scopes = new Map<string, Map<string, Note>>();
	private commitGate: Promise<void> | undefined;
	private nextCommitError: Error | undefined;

	seed(workspaceId: string, notes: readonly Note[]): void {
		this.scopes.set(workspaceId, new Map(notes.map((note) => [note.id, note])));
	}

	list(workspaceId: string): readonly Note[] {
		this.operations.push(`list:${workspaceId}`);
		return [...this.scope(workspaceId).values()];
	}

	read(workspaceId: string, id: string): Note | undefined {
		return this.scope(workspaceId).get(id);
	}

	pauseNextCommit(): Deferred {
		const deferred = new Deferred();
		this.commitGate = deferred.promise;
		return deferred;
	}

	failNextCommit(message: string): void {
		this.nextCommitError = new Error(message);
	}

	async add(workspaceId: string, note: Note): Promise<void> {
		await this.beforeCommit();
		this.scope(workspaceId).set(note.id, note);
		this.operations.push(`add:${workspaceId}:${note.id}`);
	}

	async mutate(workspaceId: string, note: Note): Promise<void> {
		await this.beforeCommit();
		this.scope(workspaceId).set(note.id, note);
		this.operations.push(`mutate:${workspaceId}:${note.id}`);
	}

	async delete(workspaceId: string, id: string): Promise<void> {
		await this.beforeCommit();
		this.scope(workspaceId).delete(id);
		this.operations.push(`delete:${workspaceId}:${id}`);
	}

	private scope(workspaceId: string): Map<string, Note> {
		let notes = this.scopes.get(workspaceId);
		if (!notes) {
			notes = new Map();
			this.scopes.set(workspaceId, notes);
		}
		return notes;
	}

	private async beforeCommit(): Promise<void> {
		const gate = this.commitGate;
		this.commitGate = undefined;
		if (gate) {
			await gate;
		}
		const error = this.nextCommitError;
		this.nextCommitError = undefined;
		if (error) {
			throw error;
		}
	}
}

export class InMemorySyncPersistence implements ManagerSyncPersistence {
	readonly mutationWrites: ManagerMutationRecord[] = [];
	readonly envelopeWrites: SyncEnvelope[] = [];
	private readonly records = new Map<string, ManagerMutationRecord>();
	private readonly scopes = new Map<string, SyncEnvelope[]>();
	private maxEnvelopesPerScope: number;

	constructor(maxEnvelopesPerScope = 1000) {
		this.maxEnvelopesPerScope = maxEnvelopesPerScope;
	}

	setRetention(maxEnvelopesPerScope: number): void {
		this.maxEnvelopesPerScope = maxEnvelopesPerScope;
		for (const envelopes of this.scopes.values()) {
			this.trim(envelopes);
		}
	}

	append(envelope: SyncEnvelope): void {
		const envelopes = this.scopes.get(envelope.scope) ?? [];
		if (!this.scopes.has(envelope.scope)) {
			this.scopes.set(envelope.scope, envelopes);
		}
		envelopes.push(envelope);
		this.envelopeWrites.push(envelope);
		this.trim(envelopes);
	}

	readAfter(scope: string, cursor: string, limit: number): ManagerOutboxRead {
		const envelopes = this.scopes.get(scope) ?? [];
		const index = envelopes.findIndex((envelope) => envelope.cursor === cursor);
		if (index < 0) {
			return {
				envelopes: [],
				cursorFound: false,
				retainedEnvelopeCount: envelopes.length
			};
		}
		return {
			envelopes: envelopes.slice(index + 1, index + 1 + limit),
			cursorFound: true,
			retainedEnvelopeCount: envelopes.length
		};
	}

	readMutation(scope: string, mutationId: string): ManagerMutationRecord | undefined {
		return this.records.get(`${scope}:${mutationId}`);
	}

	recordMutation(record: ManagerMutationRecord): void {
		this.records.set(`${record.scope}:${record.mutationId}`, record);
		this.mutationWrites.push(record);
		if (record.envelope) {
			this.append(record.envelope);
		}
	}

	retained(scope: string): readonly SyncEnvelope[] {
		return this.scopes.get(scope) ?? [];
	}

	private trim(envelopes: SyncEnvelope[]): void {
		const overflow = envelopes.length - this.maxEnvelopesPerScope;
		if (overflow > 0) {
			envelopes.splice(0, overflow);
		}
	}
}

export class InMemoryCache implements CacheAdapter {
	readonly kind = 'idb' as const;
	readonly reads: string[] = [];
	readonly writes: string[] = [];
	readonly deletes: string[] = [];
	private readonly values = new Map<string, CacheItem<unknown>>();

	async get<TValue>(key: string): Promise<CacheItem<TValue> | undefined> {
		this.reads.push(key);
		return this.values.get(key) as CacheItem<TValue> | undefined;
	}

	async set<TValue>(key: string, value: CacheItem<TValue>): Promise<void> {
		this.writes.push(key);
		this.values.set(key, value as CacheItem<unknown>);
	}

	async del(key: string): Promise<void> {
		this.deletes.push(key);
		this.values.delete(key);
	}

	peek<TValue>(key: string): CacheItem<TValue> | undefined {
		return this.values.get(key) as CacheItem<TValue> | undefined;
	}
}

type NotesManager = ReturnType<typeof createNotesManager>;
type SubscriptionOptions = Parameters<RuntimeTransport['subscribe']>[0];

export class DirectManagerSseTransport implements RuntimeTransport {
	readonly received: SyncEnvelope[] = [];
	private readonly manager: NotesManager;
	private readonly params: NotesParams;
	private options: SubscriptionOptions | undefined;
	private controller: AbortController | undefined;
	private readerTask: Promise<void> | undefined;
	private readonly envelopeWaiters = new Set<() => void>();
	private readonly chunks = new Map<string, SyncEnvelopeChunk[]>();

	constructor(managerValue: NotesManager, params: NotesParams) {
		this.manager = managerValue;
		this.params = params;
	}

	async subscribe(options: SubscriptionOptions): Promise<SyncResult<() => void, SyncError>> {
		this.options = options;
		await this.open(options.url);
		return ok(() => this.disconnect());
	}

	disconnect(): void {
		this.controller?.abort();
		this.controller = undefined;
	}

	async reconnect(notifyStore = false): Promise<void> {
		const options = this.options;
		if (!options) {
			throw new Error('Transport has no logical subscription.');
		}
		this.disconnect();
		const url = new URL(options.url, 'http://sync.test');
		const cursor = options.getCursor?.();
		if (cursor) {
			url.searchParams.set('after', cursor);
		}
		await this.open(url.href);
		if (notifyStore) {
			options.onReconnect?.();
		}
	}

	async waitForEnvelopeCount(count: number): Promise<void> {
		if (this.received.length >= count) {
			return;
		}
		await new Promise<void>((resolve, reject) => {
			const notify = () => {
				if (this.received.length >= count) {
					this.envelopeWaiters.delete(notify);
					clearTimeout(timeout);
					resolve();
				}
			};
			const timeout = setTimeout(() => {
				this.envelopeWaiters.delete(notify);
				reject(
					new Error(
						`Timed out after 3000ms waiting for ${count} SSE envelopes; received ${this.received.length}.`
					)
				);
			}, 3000);
			this.envelopeWaiters.add(notify);
		});
	}

	emit(envelope: SyncEnvelope): void {
		this.deliver(envelope);
	}

	async close(): Promise<void> {
		this.disconnect();
		await this.readerTask;
	}

	private async open(url: string): Promise<void> {
		if (!this.manager.http.events) {
			throw new Error('Test manager did not expose an events handler.');
		}
		const controller = new AbortController();
		this.controller = controller;
		const response = await this.manager.http.events({
			request: new Request(url, {
				headers: { accept: 'text/event-stream' },
				signal: controller.signal
			}),
			params: this.params
		});
		if (!response.ok || !response.body) {
			throw new Error(`Events stream failed with ${response.status}.`);
		}
		this.readerTask = this.read(response.body, controller.signal);
	}

	private async read(stream: ReadableStream<Uint8Array>, signal: AbortSignal): Promise<void> {
		const reader = stream.getReader();
		const decoder = new TextDecoder();
		let buffer = '';
		try {
			while (!signal.aborted) {
				const next = await reader.read();
				if (next.done) {
					return;
				}
				buffer += decoder.decode(next.value, { stream: true });
				let boundary = buffer.indexOf('\n\n');
				while (boundary >= 0) {
					const eventText = buffer.slice(0, boundary);
					buffer = buffer.slice(boundary + 2);
					this.handleEvent(eventText);
					boundary = buffer.indexOf('\n\n');
				}
			}
		} catch (cause) {
			if (!signal.aborted) {
				throw cause;
			}
		} finally {
			reader.releaseLock();
		}
	}

	private handleEvent(eventText: string): void {
		const event = parseSseEvent(eventText);
		if (event.eventName === 'sync') {
			const envelope = parseSyncEnvelopeJson(event.data);
			if (envelope) {
				this.deliver(envelope);
			}
			return;
		}
		if (event.eventName === 'sync-chunk') {
			const chunk = parseSyncEnvelopeChunkJson(event.data);
			if (chunk) {
				this.acceptChunk(chunk);
			}
		}
	}

	private acceptChunk(chunk: SyncEnvelopeChunk): void {
		const chunks = this.chunks.get(chunk.id) ?? [];
		chunks[chunk.index] = chunk;
		this.chunks.set(chunk.id, chunks);
		if (chunks.filter(Boolean).length !== chunk.total) {
			return;
		}
		this.chunks.delete(chunk.id);
		const envelope = parseSyncEnvelopeJson(chunks.map((part) => part.data).join(''));
		if (envelope) {
			this.deliver(envelope);
		}
	}

	private deliver(envelope: SyncEnvelope): void {
		this.received.push(envelope);
		this.options?.onEnvelope(envelope);
		for (const notify of this.envelopeWaiters) {
			notify();
		}
	}
}

export interface SyncTestSystem {
	readonly database: InMemoryNotesDatabase;
	readonly persistence: InMemorySyncPersistence;
	readonly cache: InMemoryCache;
	readonly manager: NotesManager;
	readonly transport: DirectManagerSseTransport;
	readonly fetchRequests: readonly Request[];
	createStore(): ClientStore<NotesManager['type']>;
}

export function createSyncTestSystem(workspaceId = 'workspace-1'): SyncTestSystem {
	const database = new InMemoryNotesDatabase();
	const persistence = new InMemorySyncPersistence();
	const cache = new InMemoryCache();
	const managerValue = createNotesManager(database, persistence);
	const params = { workspaceId };
	const transport = new DirectManagerSseTransport(managerValue, params);
	const fetchRequests: Request[] = [];
	let idIndex = 0;

	const fetchImpl: typeof fetch = async (input, init) => {
		const request = new Request(input, init);
		fetchRequests.push(request.clone());
		const url = new URL(request.url);
		const parts = url.pathname.split('/').filter(Boolean);
		const workspaceIndex = parts.indexOf('workspaces');
		const requestWorkspaceId = workspaceIndex >= 0 ? parts[workspaceIndex + 1] : undefined;
		const method = parts.at(-1) as keyof NotesManager['http'];
		const handler = managerValue.http[method];
		if (!requestWorkspaceId || typeof handler !== 'function') {
			return new Response('Not found', { status: 404 });
		}
		return handler({
			request,
			params: { workspaceId: requestWorkspaceId }
		} as ManagerHttpHandlerArgs<undefined, NotesParams>);
	};

	configureSync({
		fetch: fetchImpl,
		cache: {
			adapter: cache,
			ttlMs: 14 * 24 * 60 * 60 * 1000
		},
		transport,
		now: () => SYNC_TEST_NOW,
		createId(prefix) {
			idIndex += 1;
			return `${prefix}-${idIndex}`;
		}
	});

	return {
		database,
		persistence,
		cache,
		manager: managerValue,
		transport,
		fetchRequests,
		createStore() {
			return createStore<NotesManager['type']>(
				{
					key: 'notes',
					getParams: () => params,
					getUrl: ({ workspaceId: nextWorkspaceId }) =>
						`http://sync.test/workspaces/${nextWorkspaceId}/notes`,
					query: () => ({ limit: 20 })
				},
				(reconcile) =>
					reconcile.defaults({
						matchesQuery: () => true,
						compare: (left, right) => left.id.localeCompare(right.id)
					})
			);
		}
	};
}

function createNotesManager(database: InMemoryNotesDatabase, persistence: InMemorySyncPersistence) {
	const notesResource = resource(notesParamsValidator, (method) => ({
		list: method.list({
			query: notesListQueryValidator,
			output: notesPageValidator,
			handler({ params, ctx }) {
				return ctx.ok({
					items: database.list(params.workspaceId)
				});
			}
		}),
		add: method.add({
			input: addNoteValidator,
			output: noteValidator,
			handler({ params, input, ctx }) {
				const note = { id: input.id, title: input.title };
				return ctx.ok({
					output: note,
					sourceCommit: {
						async commit() {
							try {
								await database.add(params.workspaceId, note);
								return ctx.ok();
							} catch (cause) {
								return ctx.error(
									ctx.syncError('internal', cause instanceof Error ? cause.message : String(cause), {
										cause
									})
								);
							}
						}
					}
				});
			}
		}),
		mutate: method.mutate({
			query: noteQueryValidator,
			input: mutateNoteValidator,
			output: noteValidator,
			handler({ params, query, input, ctx }) {
				const existing = database.read(params.workspaceId, query.id);
				if (!existing) {
					return ctx.error(ctx.syncError('not_found', 'Note not found.'));
				}
				const note = { ...existing, title: input.title };
				return ctx.ok({
					output: note,
					sourceCommit: {
						async commit() {
							try {
								await database.mutate(params.workspaceId, note);
								return ctx.ok();
							} catch (cause) {
								return ctx.error(
									ctx.syncError('internal', cause instanceof Error ? cause.message : String(cause), {
										cause
									})
								);
							}
						}
					}
				});
			}
		}),
		delete: method.delete({
			query: noteQueryValidator,
			output: noteTombstoneValidator,
			handler({ params, query, ctx }) {
				const tombstone = { id: query.id, deleted: true as const };
				return ctx.ok({
					output: tombstone,
					sourceCommit: {
						async commit() {
							try {
								await database.delete(params.workspaceId, query.id);
								return ctx.ok();
							} catch (cause) {
								return ctx.error(
									ctx.syncError('internal', cause instanceof Error ? cause.message : String(cause), {
										cause
									})
								);
							}
						}
					}
				});
			}
		})
	}));

	return manager({
		key: 'notes',
		resource: notesResource,
		authorize: () => true,
		scope: (params) => params.workspaceId,
		persistence,
		replayLimit: 100
	});
}

const notesParamsValidator = validator<NotesParams>((value) => {
	const record = object(value);
	if (typeof record.workspaceId !== 'string') {
		throw new Error('workspaceId must be a string.');
	}
	return { workspaceId: record.workspaceId };
});

const notesListQueryValidator = validator<NotesListQuery>((value) => {
	const record = object(value);
	if (typeof record.limit !== 'number') {
		throw new Error('limit must be a number.');
	}
	return { limit: record.limit };
});

const addNoteValidator = validator<AddNoteInput>((value) => {
	const record = object(value);
	if (typeof record.id !== 'string' || typeof record.title !== 'string') {
		throw new Error('A note id and title are required.');
	}
	return { id: record.id, title: record.title };
});

const mutateNoteValidator = validator<MutateNoteInput>((value) => {
	const record = object(value);
	if (typeof record.title !== 'string') {
		throw new Error('A note title is required.');
	}
	return { title: record.title };
});

const noteQueryValidator = validator<NoteQuery>((value) => {
	const record = object(value);
	if (typeof record.id !== 'string') {
		throw new Error('A note id is required.');
	}
	return { id: record.id };
});

const noteValidator = validator<Note>((value) => {
	const record = object(value);
	if (typeof record.id !== 'string' || typeof record.title !== 'string') {
		throw new Error('A valid note is required.');
	}
	return { id: record.id, title: record.title };
});

const notesPageValidator = validator<NotesPage>((value) => {
	const record = object(value);
	if (!Array.isArray(record.items)) {
		throw new Error('Page items are required.');
	}
	return {
		items: record.items.map((item) => noteValidator.parse(item))
	};
});

const noteTombstoneValidator = validator<NoteTombstone>((value) => {
	const record = object(value);
	if (typeof record.id !== 'string' || record.deleted !== true) {
		throw new Error('A valid note tombstone is required.');
	}
	return { id: record.id, deleted: true };
});

function validator<TValue>(parse: (value: unknown) => TValue): Validator<TValue> {
	return { parse };
}

function object(value: unknown): Record<string, unknown> {
	if (typeof value !== 'object' || value === null || Array.isArray(value)) {
		throw new Error('Expected an object.');
	}
	return value as Record<string, unknown>;
}
