import {
	httpSharedSyncStream,
	registerSharedStreamSubscription,
	type ManagerHttpHandlers,
	type ManagerSyncPersistence,
	type SyncEnvelope
} from '../../src/server/index.ts';
import { parseSseEvent } from '../../src/shared/sse.ts';

const decoder = new TextDecoder();

export const STREAM_SCOPE = 'notes:ws';

export function connectRequest(transportId: string): Request {
	return new Request('http://sync.test/connect', { method: 'POST', headers: { 'x-sync-transport-id': transportId } });
}

export function streamRequest(transportId: string): Request {
	return new Request('http://sync.test/stream', {
		method: 'POST',
		headers: { 'x-sync-transport-id': transportId, 'x-forwarded-for': '203.0.113.1' }
	});
}

export function streamEnvelope(cursor: string, scope = STREAM_SCOPE): SyncEnvelope {
	return {
		managerKey: 'notes',
		scope,
		cursor,
		changes: [{ type: 'itemAdded', id: cursor, value: { id: cursor } }]
	};
}

export function outboxReading(
	readAfter: ManagerSyncPersistence['readAfter'] = () => ({
		cursorFound: true,
		retainedEnvelopeCount: 0,
		envelopes: []
	})
): ManagerSyncPersistence {
	return {
		async append() {},
		readAfter,
		async readMutation() {
			return undefined;
		},
		async recordMutation() {}
	};
}

export function registerStreamSubscription(
	transportId: string,
	persistence: ManagerSyncPersistence,
	afterCursor?: string,
	scope = STREAM_SCOPE
) {
	return registerSharedStreamSubscription({
		request: connectRequest(transportId),
		managerKey: 'notes',
		scope,
		afterCursor,
		replayLimit: 100,
		persistence,
		nextCursor: () => 'reset'
	});
}

export async function openStream(transportId: string): Promise<ReadableStreamDefaultReader<Uint8Array>> {
	const reader = (await httpSharedSyncStream(streamRequest(transportId))).body!.getReader();
	const hello = parseSseEvent(decoder.decode((await reader.read()).value));
	if (hello.eventName !== 'hello') {
		throw new Error(`Expected a hello event, received ${hello.eventName}.`);
	}
	return reader;
}

export async function openSharedStream<TParams>(
	handlers: ManagerHttpHandlers<undefined, TParams>,
	params: TParams,
	options: { readonly afterCursor?: string; readonly signal?: AbortSignal; readonly transportId?: string } = {}
): Promise<ReadableStreamDefaultReader<Uint8Array>> {
	const headers = { 'x-sync-transport-id': options.transportId ?? 'fixture-transport' };
	const stream = await httpSharedSyncStream(
		new Request('http://sync.test/stream', { method: 'POST', headers, signal: options.signal })
	);
	const reader = stream.body!.getReader();
	const url = new URL('http://sync.test/connect');
	if (options.afterCursor) {
		url.searchParams.set('after', options.afterCursor);
	}
	const connected = await handlers.connect!({
		request: new Request(url, { method: 'POST', headers, signal: options.signal }),
		params
	});
	if (!connected.ok) {
		throw new Error(`Connect failed with ${connected.status}.`);
	}
	return reader;
}

export async function readSyncEnvelope(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<SyncEnvelope> {
	for (;;) {
		const next = await reader.read();
		if (next.done) {
			throw new Error('Shared stream closed before a sync envelope arrived.');
		}
		const event = parseSseEvent(decoder.decode(next.value));
		if (event.eventName === 'sync') {
			return JSON.parse(event.data) as SyncEnvelope;
		}
	}
}

// Stream order is delivery order, so publishing a final envelope and reading up to it also proves nothing else arrived before it.
export async function readCursorsUntil(
	reader: ReadableStreamDefaultReader<Uint8Array>,
	lastCursor: string
): Promise<string[]> {
	const cursors: string[] = [];
	while (cursors.at(-1) !== lastCursor) {
		cursors.push((await readSyncEnvelope(reader)).cursor);
	}
	return cursors;
}
