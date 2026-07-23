import { describe, expect, it } from 'vite-plus/test';

import { MemoryManagerRealtimeBus, syncError } from '../../src/server/index.ts';

describe('manager realtime bus', () => {
	it('publishes only to subscribers for the envelope scope', () => {
		const bus = new MemoryManagerRealtimeBus();
		const first: unknown[] = [];
		const second: unknown[] = [];
		bus.subscribe('notes:workspace-1', (envelope) => first.push(envelope));
		bus.subscribe('notes:workspace-2', (envelope) => second.push(envelope));
		bus.publish({ managerKey: 'notes', scope: 'notes:workspace-1', cursor: 'c1', changes: [] });
		expect(first).toHaveLength(1);
		expect(second).toHaveLength(0);
	});

	it('supports idempotent unsubscribe and removes an empty scope', () => {
		const bus = new MemoryManagerRealtimeBus();
		const received: unknown[] = [];
		const unsubscribe = bus.subscribe('scope', (envelope) => received.push(envelope));
		unsubscribe();
		unsubscribe();
		bus.publish({ managerKey: 'notes', scope: 'scope', cursor: 'c1', changes: [] });
		expect(received).toEqual([]);
	});

	it('propagates realtime transport failures to the caller', async () => {
		const bus = new MemoryManagerRealtimeBus();
		bus.subscribe('scope', () => {
			throw syncError('internal', 'subscriber failed');
		});
		expect(() => bus.publish({ managerKey: 'notes', scope: 'scope', cursor: 'c1', changes: [] })).toThrow(
			'subscriber failed'
		);
	});
});
