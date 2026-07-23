import { describe, expect, it } from 'vite-plus/test';

import {
	createPubSubManagerRealtimeBus,
	type ManagerRealtimeBusErrorContext,
	type ManagerRealtimePubSubTransport,
	type SyncEnvelope
} from '../../src/server/index.ts';

const validEnvelope: SyncEnvelope = {
	managerKey: 'notes',
	scope: 'notes:workspace one',
	cursor: 'c1',
	changes: [{ type: 'itemAdded', id: 'n1', value: { id: 'n1' } }]
};

describe('pub-sub manager realtime bus', () => {
	it('uses the encoded scoped channel and round-trips a valid envelope', async () => {
		const published: { channel: string; payload: string }[] = [];
		let receiver: ((payload: string) => Promise<void> | void) | undefined;
		const transport: ManagerRealtimePubSubTransport = {
			publish(channel, payload) {
				published.push({ channel, payload });
			},
			subscribe(channel, onPayload) {
				expect(channel).toBe('custom:notes%3Aworkspace%20one');
				receiver = onPayload;
				return () => {};
			}
		};
		const bus = createPubSubManagerRealtimeBus({
			transport,
			channelPrefix: 'custom',
			serialize: (envelope) => `wrapped:${JSON.stringify(envelope)}`,
			deserialize: (payload) => JSON.parse(payload.slice('wrapped:'.length)) as SyncEnvelope
		});
		const received: SyncEnvelope[] = [];

		await bus.publish(validEnvelope);
		const unsubscribe = await bus.subscribe(validEnvelope.scope, (envelope) => received.push(envelope));
		await receiver?.(published[0]!.payload);

		expect(published).toEqual([
			{
				channel: 'custom:notes%3Aworkspace%20one',
				payload: `wrapped:${JSON.stringify(validEnvelope)}`
			}
		]);
		expect(received).toEqual([validEnvelope]);
		await unsubscribe();
	});

	it('reports undecodable, invalid, and wrong-scope payloads without delivering them', async () => {
		const reported: { code: string; context: ManagerRealtimeBusErrorContext }[] = [];
		let receiver: ((payload: string) => Promise<void> | void) | undefined;
		const bus = createPubSubManagerRealtimeBus({
			transport: {
				publish() {},
				subscribe(_channel, onPayload) {
					receiver = onPayload;
					return () => {};
				}
			},
			handleError(error, context) {
				reported.push({ code: error.code, context });
			}
		});
		const received: SyncEnvelope[] = [];
		await bus.subscribe('notes:w1', (envelope) => received.push(envelope));

		await receiver?.('{bad json');
		await receiver?.(JSON.stringify({ managerKey: 'notes' }));
		await receiver?.(
			JSON.stringify({
				managerKey: 'notes',
				scope: 'notes:w2',
				cursor: 'c1',
				changes: []
			})
		);

		expect(received).toEqual([]);
		expect(reported).toHaveLength(3);
		expect(reported.map((entry) => entry.code)).toEqual(['validation', 'validation', 'validation']);
		expect(reported.map((entry) => entry.context)).toEqual([
			{ phase: 'receive', channel: 'sync-resource:notes%3Aw1', scope: 'notes:w1' },
			{ phase: 'receive', channel: 'sync-resource:notes%3Aw1', scope: 'notes:w1' },
			{ phase: 'receive', channel: 'sync-resource:notes%3Aw1', scope: 'notes:w1' }
		]);
	});

	it('reports publish and subscribe failures and preserves the transport error', async () => {
		const publishCause = new Error('publish unavailable');
		const subscribeCause = new Error('subscribe unavailable');
		const reported: ManagerRealtimeBusErrorContext[] = [];
		let errorHookCalls = 0;
		const publishBus = createPubSubManagerRealtimeBus({
			transport: {
				publish() {
					throw publishCause;
				},
				subscribe() {
					return () => {};
				}
			},
			handleError(_error, context) {
				reported.push(context);
				errorHookCalls += 1;
				throw new Error('reporting unavailable');
			}
		});
		const subscribeBus = createPubSubManagerRealtimeBus({
			transport: {
				publish() {},
				subscribe() {
					throw subscribeCause;
				}
			},
			handleError(_error, context) {
				reported.push(context);
			}
		});

		await expect(publishBus.publish(validEnvelope)).rejects.toBe(publishCause);
		await expect(subscribeBus.subscribe(validEnvelope.scope, () => {})).rejects.toBe(subscribeCause);
		expect(errorHookCalls).toBe(1);
		expect(reported).toEqual([
			{
				phase: 'publish',
				channel: 'sync-resource:notes%3Aworkspace%20one',
				scope: 'notes:workspace one'
			},
			{
				phase: 'subscribe',
				channel: 'sync-resource:notes%3Aworkspace%20one',
				scope: 'notes:workspace one'
			}
		]);
	});

	it('reports unsubscribe failures without making cleanup throw', async () => {
		const reported: ManagerRealtimeBusErrorContext[] = [];
		const bus = createPubSubManagerRealtimeBus({
			transport: {
				publish() {},
				subscribe() {
					return () => {
						throw new Error('unsubscribe failed');
					};
				}
			},
			handleError(_error, context) {
				reported.push(context);
			}
		});

		const unsubscribe = await bus.subscribe(validEnvelope.scope, () => {});
		await expect(unsubscribe()).resolves.toBeUndefined();
		expect(reported).toEqual([
			{
				phase: 'unsubscribe',
				channel: 'sync-resource:notes%3Aworkspace%20one',
				scope: 'notes:workspace one'
			}
		]);
	});
});
