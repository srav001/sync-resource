import { isFiniteNumber, isRecord, isString } from './guards.ts';
import { isSyncEnvelope, type SyncEnvelope } from './protocol.ts';

const textEncoder = new TextEncoder();

export const MAX_SSE_FRAME_BYTES = 256 * 1024;
export const MAX_CHUNKED_SYNC_BYTES = 1024 * 1024;

// A chunk of at most 64K JSON characters encodes to at most 3 bytes each, which stays under MAX_SSE_FRAME_BYTES.
const SYNC_CHUNK_CHARS = 64 * 1024;
const SYNC_FRAME_OVERHEAD_BYTES = 'event: sync\ndata: \n\n'.length;

export interface ParsedSseEvent {
	readonly eventName: string;
	readonly data: string;
}

export interface SyncEnvelopeChunk {
	readonly type: 'sync-chunk';
	readonly id: string;
	readonly index: number;
	readonly total: number;
	readonly totalBytes: number;
	readonly data: string;
}

export function parseSyncEnvelopeChunkJson(value: string): SyncEnvelopeChunk | undefined {
	try {
		const chunk = JSON.parse(value);
		return isSyncEnvelopeChunk(chunk) ? chunk : undefined;
	} catch {
		return undefined;
	}
}

function isSyncEnvelopeChunk<TValue>(value: TValue): value is TValue & SyncEnvelopeChunk {
	return (
		isRecord(value) &&
		value.type === 'sync-chunk' &&
		isString(value.id) &&
		isFiniteNumber(value.index) &&
		Number.isInteger(value.index) &&
		isFiniteNumber(value.total) &&
		Number.isInteger(value.total) &&
		isFiniteNumber(value.totalBytes) &&
		Number.isInteger(value.totalBytes) &&
		isString(value.data)
	);
}

export function encodeSseFrame<TPayload>(eventName: string, payload: TPayload): Uint8Array {
	return encodeSseFrameJson(eventName, JSON.stringify(payload));
}

export function encodeSyncEnvelopeSseFrames(
	envelope: SyncEnvelope,
	maxEnvelopeBytes: number
): readonly Uint8Array[] | undefined {
	const serialized = JSON.stringify(envelope);
	const fullFrame = encodeSseFrameJson('sync', serialized);
	if (fullFrame.byteLength > maxEnvelopeBytes) {
		return undefined;
	}
	if (fullFrame.byteLength <= MAX_SSE_FRAME_BYTES) {
		return [fullFrame];
	}

	const id = `${envelope.managerKey}:${envelope.scope}:${envelope.cursor}`;
	const totalBytes = fullFrame.byteLength - SYNC_FRAME_OVERHEAD_BYTES;
	const total = Math.ceil(serialized.length / SYNC_CHUNK_CHARS);
	return Array.from({ length: total }, (_, index) =>
		encodeSseFrame('sync-chunk', {
			type: 'sync-chunk',
			id,
			index,
			total,
			totalBytes,
			data: serialized.slice(index * SYNC_CHUNK_CHARS, (index + 1) * SYNC_CHUNK_CHARS)
		})
	);
}

export function parseSseEvent(eventText: string): ParsedSseEvent {
	const lines = eventText.split('\n');
	let eventName = '';
	let data = '';
	for (const line of lines) {
		if (line.startsWith('event:')) {
			eventName = line.slice('event:'.length).trim();
		}
		if (line.startsWith('data:')) {
			data += line.slice('data:'.length).trim();
		}
	}
	return { eventName, data };
}

export function parseSyncEnvelopeJson(value: string): SyncEnvelope | undefined {
	try {
		const parsed = JSON.parse(value);
		return isSyncEnvelope(parsed) ? parsed : undefined;
	} catch {
		return undefined;
	}
}

function encodeSseFrameJson(eventName: string, json: string): Uint8Array {
	return textEncoder.encode(`event: ${eventName}\ndata: ${json}\n\n`);
}
