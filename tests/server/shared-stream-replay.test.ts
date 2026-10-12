import { afterEach, describe, expect, it } from 'vite-plus/test';

import {
	httpSharedSyncStream,
	publishSharedStreamEnvelope,
	registerSharedStreamManager,
	registerSharedStreamSubscription,
	type ManagerOutboxRead,
	type ManagerSyncPersistence,
	type SyncEnvelope
} from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';
import { parseSseEvent } from '../../src/shared/sse.ts';
import { Deferred } from '../fixtures/syncSystem.ts';

afterEach(() => resetSharedStreamForTests());
const SCOPE = 'notes:ws';
const env = (cursor: string): SyncEnvelope => ({
	managerKey: 'notes',
	scope: SCOPE,
	cursor,
	changes: [{ type: 'itemAdded', id: cursor, value: { id: cursor } }]
});
const persistence = (
	readAfter: (s: string, c: string, l: number) => ManagerOutboxRead | Promise<ManagerOutboxRead>
): ManagerSyncPersistence => ({
	async append() {},
	readAfter,
	async readMutation() {
		return undefined;
	},
	async recordMutation() {}
});
const connect = (t: string) =>
	new Request('http://x/connect', { method: 'POST', headers: { 'x-sync-transport-id': t } });
const stream = (t: string) =>
	new Request('http://x/stream', {
		method: 'POST',
		headers: { 'x-sync-transport-id': t, 'x-forwarded-for': '1.1.1.1' }
	});
const reg = (t: string, p: ManagerSyncPersistence, afterCursor?: string) =>
	registerSharedStreamSubscription({
		request: connect(t),
		managerKey: 'notes',
		scope: SCOPE,
		afterCursor,
		replayLimit: 100,
		persistence: p,
		nextCursor: () => 'reset'
	});

const pending = new WeakMap<object, Promise<ReadableStreamReadResult<Uint8Array>>>();
async function drain(reader: ReadableStreamDefaultReader<Uint8Array>, ms: number): Promise<string[]> {
	const out: string[] = [];
	const end = Date.now() + ms;
	while (Date.now() < end) {
		let read = pending.get(reader);
		if (!read) {
			read = reader.read();
			pending.set(reader, read);
		}
		const r = await Promise.race([read, new Promise<undefined>((res) => setTimeout(res, 30))]);
		if (!r) {
			continue;
		}
		pending.delete(reader);
		if (r.done) {
			break;
		}
		const ev = parseSseEvent(new TextDecoder().decode(r.value));
		if (ev.eventName === 'sync') {
			const j = JSON.parse(ev.data);
			out.push(j.reset ? `reset:${j.reset.reason}` : j.cursor);
		}
	}
	return out;
}

describe('shared stream replay ordering', () => {
	it('delivers a replayed envelope before a live envelope published right after registration', async () => {
		registerSharedStreamManager({ key: 'notes' });
		const p = persistence(() => ({
			cursorFound: true,
			retainedEnvelopeCount: 2,
			envelopes: [env('c1'), env('c2')]
		}));
		const res = await httpSharedSyncStream(stream('t1'));
		const reader = res.body!.getReader();
		await reader.read(); // hello
		await reg('t1', p, 'c0');
		publishSharedStreamEnvelope(env('c2'));
		const got = await drain(reader, 200);
		expect(got).toEqual(['c1', 'c2']);
	});

	it('keeps buffering live envelopes until the last overlapping replay of a scope ends', async () => {
		registerSharedStreamManager({ key: 'notes' });
		const gates = [new Deferred<ManagerOutboxRead>(), new Deferred<ManagerOutboxRead>()];
		let n = 0;
		const p = persistence(() => gates[n++]!.promise);
		await reg('t2', p, 'c0');
		const res = await httpSharedSyncStream(stream('t2'));
		const reader = res.body!.getReader();
		await reader.read();
		await reg('t2', p, 'c0');
		await new Promise((r) => setTimeout(r, 20));
		gates[0]!.resolve({ cursorFound: true, retainedEnvelopeCount: 1, envelopes: [] });
		await new Promise((r) => setTimeout(r, 20));
		publishSharedStreamEnvelope(env('c9'));
		const before = await drain(reader, 100);
		gates[1]!.resolve({ cursorFound: true, retainedEnvelopeCount: 2, envelopes: [env('c5'), env('c9')] });
		const after = await drain(reader, 100);
		expect([...before, ...after]).toEqual(['c5', 'c9']);
	});
});

describe('shared stream replay gaps', () => {
	it('resets an unknown cursor even when the outbox retains nothing', async () => {
		registerSharedStreamManager({ key: 'notes' });
		const p = persistence(() => ({ cursorFound: false, retainedEnvelopeCount: 0, envelopes: [] }));
		await reg('t3', p, 'old-cursor');
		const res = await httpSharedSyncStream(stream('t3'));
		const reader = res.body!.getReader();
		await reader.read();
		expect(await drain(reader, 150)).toEqual(['reset:retention_gap']);
	});

	it('resets instead of sending a replay truncated at the replay limit', async () => {
		registerSharedStreamManager({ key: 'notes' });
		const retained = Array.from({ length: 150 }, (_, index) => env(`c${index + 1}`));
		const p = persistence((_s, _c, limit) => ({
			cursorFound: true,
			retainedEnvelopeCount: 150,
			envelopes: retained.slice(0, limit)
		}));
		await reg('t5', p, 'c0');
		const res = await httpSharedSyncStream(stream('t5'));
		const reader = res.body!.getReader();
		await reader.read();
		expect(await drain(reader, 150)).toEqual(['reset:retention_gap']);
	});

	it('replays without a reset when the pending envelopes exactly fill the replay limit', async () => {
		registerSharedStreamManager({ key: 'notes' });
		const retained = Array.from({ length: 100 }, (_, index) => env(`c${index + 1}`));
		const p = persistence((_s, _c, limit) => ({
			cursorFound: true,
			retainedEnvelopeCount: 100,
			envelopes: retained.slice(0, limit)
		}));
		await reg('t6', p, 'c0');
		const res = await httpSharedSyncStream(stream('t6'));
		const reader = res.body!.getReader();
		await reader.read();
		const frames = await drain(reader, 300);
		expect(frames).toHaveLength(100);
		expect(frames.at(-1)).toBe('c100');
	});
});
