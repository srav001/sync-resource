import { isSyncEnvelope } from '../shared/index.ts';
import { err, ok, type SyncResult } from '../shared/result.js';
import { normalizeSyncError, syncError, type SyncError } from './errors.js';
import type { Awaitable, ManagerRealtimeBus, SyncEnvelope } from './types.js';

export interface ManagerRealtimePubSubTransport {
	publish(channel: string, payload: string): Awaitable<void>;
	subscribe(channel: string, onPayload: (payload: string) => Awaitable<void>): Awaitable<() => Awaitable<void>>;
}

export interface ManagerRealtimeBusErrorContext {
	readonly phase: 'publish' | 'subscribe' | 'receive' | 'unsubscribe';
	readonly channel: string;
	readonly scope?: string;
}

export interface PubSubManagerRealtimeBusOptions {
	readonly transport: ManagerRealtimePubSubTransport;
	readonly channelPrefix?: string;
	serialize?(this: void, envelope: SyncEnvelope): string;
	deserialize?(this: void, payload: string): SyncEnvelope;
	handleError?(error: SyncError, context: ManagerRealtimeBusErrorContext): Awaitable<void>;
}

export function createPubSubManagerRealtimeBus(options: PubSubManagerRealtimeBusOptions): ManagerRealtimeBus {
	const channelPrefix = options.channelPrefix ?? 'sync-resource';
	const serialize = options.serialize ?? defaultSerializeEnvelope;
	const deserialize = options.deserialize ?? defaultDeserializeEnvelope;

	return {
		async publish(envelope) {
			const channel = buildRealtimeChannel(channelPrefix, envelope.scope);
			try {
				await options.transport.publish(channel, serialize(envelope));
			} catch (cause) {
				const errorValue = normalizeSyncError(cause);
				await reportRealtimeBusError(options, errorValue, {
					phase: 'publish',
					channel,
					scope: envelope.scope
				});
				throw cause;
			}
		},

		async subscribe(scope, onEnvelope) {
			const channel = buildRealtimeChannel(channelPrefix, scope);
			try {
				const unsubscribe = await options.transport.subscribe(channel, async (payload) => {
					const envelope = parseInboundEnvelope(payload, deserialize);
					if (envelope.isErr()) {
						await reportRealtimeBusError(options, envelope.error, {
							phase: 'receive',
							channel,
							scope
						});
						return;
					}
					if (envelope.value.scope !== scope) {
						await reportRealtimeBusError(
							options,
							syncError('validation', 'Realtime bus envelope scope did not match subscription.', {
								details: {
									expectedScope: scope,
									actualScope: envelope.value.scope
								}
							}),
							{
								phase: 'receive',
								channel,
								scope
							}
						);
						return;
					}
					onEnvelope(envelope.value);
				});

				return async () => {
					try {
						await unsubscribe();
					} catch (cause) {
						const errorValue = normalizeSyncError(cause);
						await reportRealtimeBusError(options, errorValue, {
							phase: 'unsubscribe',
							channel,
							scope
						});
					}
				};
			} catch (cause) {
				const errorValue = normalizeSyncError(cause);
				await reportRealtimeBusError(options, errorValue, {
					phase: 'subscribe',
					channel,
					scope
				});
				throw cause;
			}
		}
	};
}

function buildRealtimeChannel(prefix: string, scope: string): string {
	return `${prefix}:${encodeURIComponent(scope)}`;
}

function defaultSerializeEnvelope(envelope: SyncEnvelope): string {
	return JSON.stringify(envelope);
}

function defaultDeserializeEnvelope(payload: string): SyncEnvelope {
	return JSON.parse(payload) as SyncEnvelope;
}

function parseInboundEnvelope(
	payload: string,
	deserialize: (payload: string) => SyncEnvelope
): SyncResult<SyncEnvelope, SyncError> {
	try {
		const envelope = deserialize(payload);
		if (!isSyncEnvelope(envelope)) {
			return err(
				syncError('validation', 'Realtime bus payload was not a valid sync envelope.', {
					details: envelope
				})
			);
		}
		return ok(envelope);
	} catch (cause) {
		return err(syncError('validation', 'Realtime bus payload could not be decoded.', { cause }));
	}
}

async function reportRealtimeBusError(
	options: PubSubManagerRealtimeBusOptions,
	errorValue: SyncError,
	context: ManagerRealtimeBusErrorContext
): Promise<SyncResult<void, SyncError>> {
	if (!options.handleError) {
		return ok();
	}
	try {
		await options.handleError(errorValue, context);
		return ok();
	} catch (cause) {
		return err(normalizeSyncError(cause));
	}
}
