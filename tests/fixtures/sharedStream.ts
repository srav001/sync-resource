import { httpSharedSyncStream, type ManagerHttpHandlers, type SyncEnvelope } from '../../src/server/index.ts';
import { parseSseEvent } from '../../src/shared/sse.ts';

const decoder = new TextDecoder();

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
