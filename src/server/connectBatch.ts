import { isRecord } from '../shared/guards.ts';
import { syncError, type SyncErrorCode } from '../shared/index.ts';
import { syncHttpError } from '../shared/protocol.ts';
import { MAX_BATCH_CONNECTS } from '../shared/protocol.ts';

const SKIPPED_HEADERS = ['content-length', 'content-type', 'transfer-encoding'];

function errorBody(code: SyncErrorCode, message: string) {
	return syncHttpError(syncError(code, message));
}

function isConnectPath(path: unknown, origin: string): path is string {
	if (typeof path !== 'string' || !path.startsWith('/') || path.startsWith('//')) {
		return false;
	}
	const url = new URL(path, origin);
	return url.origin === origin && url.pathname.endsWith('/connect');
}

export async function httpSharedSyncConnect(
	request: Request,
	dispatch: (request: Request) => Promise<Response>
): Promise<Response> {
	const body: unknown = await request.json().catch(() => undefined);
	const connects = isRecord(body) ? body.connects : undefined;
	const origin = new URL(request.url).origin;
	if (
		!Array.isArray(connects) ||
		connects.length === 0 ||
		connects.length > MAX_BATCH_CONNECTS ||
		!connects.every((path) => isConnectPath(path, origin))
	) {
		return Response.json(
			errorBody(
				'bad_request',
				`Body must be { connects: [...] } with 1 to ${MAX_BATCH_CONNECTS} relative connect paths.`
			),
			{ status: 400 }
		);
	}
	const headers = new Headers(request.headers);
	for (const name of SKIPPED_HEADERS) {
		headers.delete(name);
	}
	const results = await Promise.all(
		connects.map(async (path: string) => {
			try {
				const response = await dispatch(
					new Request(new URL(path, origin), { method: 'POST', headers, signal: request.signal })
				);
				return { status: response.status, body: await response.json().catch(() => null) };
			} catch {
				return { status: 500, body: errorBody('internal', 'Sync connect failed.') };
			}
		})
	);
	return Response.json({ results });
}
