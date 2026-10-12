import type { SyncEnvelope } from '../../src/client/core.ts';

export function syncResponse(value: unknown, envelope?: SyncEnvelope): Response {
	return Response.json({ isOk: true, isError: false, value, ...(envelope ? { envelope } : {}) });
}

export function syncErrorResponse(message: string): Response {
	return Response.json({ isOk: false, isError: true, error: { code: 'internal', message } });
}
