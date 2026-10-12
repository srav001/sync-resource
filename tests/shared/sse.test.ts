import { describe, expect, it } from 'vite-plus/test';

import {
	MAX_CHUNKED_SYNC_BYTES,
	MAX_SSE_FRAME_BYTES,
	encodeSseFrame,
	encodeSyncEnvelopeSseFrames,
	parseSseEvent,
	parseSyncEnvelopeChunkJson,
	parseSyncEnvelopeJson
} from '../../src/shared/sse.ts';

const envelope = {
	managerKey: 'notes',
	scope: 'notes:workspace',
	cursor: 'c1',
	changes: [{ type: 'itemAdded' as const, id: 'n1', value: { id: 'n1', title: 'hello' } }]
};
const decode = new TextDecoder();
describe('SSE framing', () => {
	it('encodes and parses one event', () => {
		expect(parseSseEvent(decode.decode(encodeSseFrame('sync', { ok: true })))).toEqual({
			eventName: 'sync',
			data: '{"ok":true}'
		});
	});
	it('parses valid envelopes and rejects malformed shapes', () => {
		expect(parseSyncEnvelopeJson(JSON.stringify(envelope))).toEqual(envelope);
		expect(parseSyncEnvelopeJson('{bad')).toBeUndefined();
		const withSignal = { ...envelope, signals: [{ type: 'refresh', payload: undefined }] };
		expect(parseSyncEnvelopeJson(JSON.stringify(withSignal))).toEqual({
			...envelope,
			signals: [{ type: 'refresh' }]
		});
		expect(parseSyncEnvelopeJson(JSON.stringify({ ...envelope, changes: [{ type: 'bad' }] }))).toBeUndefined();
	});
	it('encodes small envelopes as one sync frame', () => {
		const frames = encodeSyncEnvelopeSseFrames(envelope, MAX_CHUNKED_SYNC_BYTES);
		expect(frames).toHaveLength(1);
		expect(parseSseEvent(decode.decode(frames![0])).eventName).toBe('sync');
	});
	it('chunks large envelopes and reassembles their JSON', () => {
		const large = {
			...envelope,
			changes: [{ type: 'itemAdded' as const, id: 'n1', value: { text: 'x'.repeat(300_000) } }]
		};
		const frames = encodeSyncEnvelopeSseFrames(large, MAX_CHUNKED_SYNC_BYTES);
		expect(frames).toBeDefined();
		expect(frames!.length).toBeGreaterThan(1);
		const chunks = frames!.map((frame) => parseSyncEnvelopeChunkJson(parseSseEvent(decode.decode(frame)).data));
		expect(chunks.every(Boolean)).toBe(true);
		const ordered = chunks.sort((a, b) => a!.index - b!.index);
		const serialized = ordered.map((chunk) => chunk!.data).join('');
		expect(JSON.parse(serialized)).toEqual(large);
		expect(ordered.every((chunk) => chunk!.totalBytes === new TextEncoder().encode(serialized).byteLength)).toBe(
			true
		);
	});
	it.each([
		['three-byte characters', '\u20ac'.repeat(150_000)],
		['escaped quotes', '"'.repeat(300_000)]
	])('keeps every chunk frame within the frame limit for %s', (_name, text) => {
		const large = { ...envelope, changes: [{ type: 'itemAdded' as const, id: 'n1', value: { text } }] };
		const frames = encodeSyncEnvelopeSseFrames(large, MAX_CHUNKED_SYNC_BYTES * 4);
		expect(frames!.length).toBeGreaterThan(1);
		expect(frames!.every((frame) => frame.byteLength <= MAX_SSE_FRAME_BYTES)).toBe(true);
	});
	it('rejects fractional and non-finite chunk indexes, counts, and byte lengths', () => {
		const base = { type: 'sync-chunk', id: 'chunk', index: 0, total: 1, totalBytes: 1, data: 'x' };
		expect(parseSyncEnvelopeChunkJson(JSON.stringify(base))).toEqual(base);
		for (const malformed of [
			{ ...base, index: 0.5 },
			{ ...base, total: 1.5 },
			{ ...base, totalBytes: 1.5 },
			{ ...base, index: Number.NaN },
			{ ...base, total: Number.POSITIVE_INFINITY }
		]) {
			expect(parseSyncEnvelopeChunkJson(JSON.stringify(malformed))).toBeUndefined();
		}
	});
	it('rejects envelopes above the logical limit', () => {
		const large = {
			...envelope,
			changes: [{ type: 'itemAdded' as const, id: 'n1', value: { text: 'x'.repeat(4_000) } }]
		};
		expect(encodeSyncEnvelopeSseFrames(large, 100)).toBeUndefined();
	});
});
