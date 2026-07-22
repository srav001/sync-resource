import { isSyncEnvelope, type SyncEnvelope } from './protocol.ts';

const textEncoder = new TextEncoder();

export const MAX_SSE_FRAME_BYTES = 256 * 1024;
export const MAX_CHUNKED_SYNC_BYTES = 1024 * 1024;

const SYNC_CHUNK_CHARS = 64 * 1024;
const MIN_SYNC_CHUNK_CHARS = 1024;

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
		return JSON.parse(value) as SyncEnvelopeChunk;
	} catch {
		return undefined;
	}
}

export function encodeSseFrame(eventName: string, payload: unknown): Uint8Array {
	return encodeSseFrameJson(eventName, JSON.stringify(payload));
}

export function encodeSyncEnvelopeSseFrames(
	envelope: SyncEnvelope,
	options: {
		readonly maxEnvelopeBytes: number;
		readonly maxFrameBytes?: number;
	}
): readonly Uint8Array[] | undefined {
	const maxFrameBytes = options.maxFrameBytes ?? MAX_SSE_FRAME_BYTES;
	const serialized = JSON.stringify(envelope);
	const fullFrame = encodeSseFrameJson('sync', serialized);
	if (fullFrame.byteLength > options.maxEnvelopeBytes) {
		return undefined;
	}
	if (fullFrame.byteLength <= maxFrameBytes) {
		return [fullFrame];
	}

	const serializedBytes = textEncoder.encode(serialized).byteLength;
	return encodeChunkedSyncEnvelopeFrames(envelope, serialized, serializedBytes, maxFrameBytes);
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
		const parsed = JSON.parse(value) as unknown;
		return isSyncEnvelope(parsed) ? parsed : undefined;
	} catch {
		return undefined;
	}
}

function encodeChunkedSyncEnvelopeFrames(
	envelope: SyncEnvelope,
	serialized: string,
	totalBytes: number,
	maxFrameBytes: number
): readonly Uint8Array[] | undefined {
	let chunkChars = Math.min(SYNC_CHUNK_CHARS, Math.max(serialized.length, 1));
	while (chunkChars >= MIN_SYNC_CHUNK_CHARS) {
		const chunks = splitString(serialized, chunkChars);
		const frames: Uint8Array[] = [];
		let oversized = false;
		for (let index = 0; index < chunks.length; index += 1) {
			const chunk = chunks[index];
			if (chunk === undefined) {
				oversized = true;
				break;
			}
			const frame = encodeSseFrame('sync-chunk', {
				type: 'sync-chunk',
				id: syncChunkId(envelope),
				index,
				total: chunks.length,
				totalBytes,
				data: chunk
			});
			if (frame.byteLength > maxFrameBytes) {
				oversized = true;
				break;
			}
			frames.push(frame);
		}
		if (!oversized) {
			return frames;
		}
		chunkChars = Math.floor(chunkChars / 2);
	}
	return undefined;
}

function encodeSseFrameJson(eventName: string, json: string): Uint8Array {
	return textEncoder.encode(`event: ${eventName}\ndata: ${json}\n\n`);
}

function splitString(value: string, chunkChars: number): string[] {
	const chunks: string[] = [];
	for (let index = 0; index < value.length; index += chunkChars) {
		chunks.push(value.slice(index, index + chunkChars));
	}
	return chunks;
}

function syncChunkId(envelope: SyncEnvelope): string {
	return `${envelope.managerKey}:${envelope.scope}:${envelope.cursor}`;
}
