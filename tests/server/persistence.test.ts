import { describe, expect, it } from 'vite-plus/test';

import { MemoryManagerOutbox } from '../../src/server/index.ts';

const envelope = (cursor: string) => ({
	managerKey: 'notes',
	scope: 'notes:s',
	cursor,
	changes: [{ type: 'itemAdded' as const, id: cursor, value: { id: cursor } }]
});

describe('memory manager persistence', () => {
	it('retains newest envelopes and reports cursor gaps', () => {
		const outbox = new MemoryManagerOutbox(2);
		outbox.append(envelope('c1'));
		outbox.append(envelope('c2'));
		outbox.append(envelope('c3'));
		expect(outbox.readAfter('notes:s', 'c1', 10)).toMatchObject({ cursorFound: false, retainedEnvelopeCount: 2 });
		expect(outbox.readAfter('notes:s', 'c2', 10)).toMatchObject({ cursorFound: true, envelopes: [envelope('c3')] });
		expect(outbox.readAfter('notes:s', 'c3', 10)).toMatchObject({ cursorFound: true, envelopes: [] });
	});

	it('records mutation envelopes and supports lookup/pruning', () => {
		const outbox = new MemoryManagerOutbox();
		const value = envelope('c1');
		const record = {
			scope: 'notes:s',
			mutationId: 'm1',
			method: 'add',
			argsHash: 'h',
			output: { id: 'c1' },
			envelope: value,
			status: 'finalized' as const,
			createdAt: Date.now(),
			finalizedAt: Date.now()
		};
		outbox.recordMutation(record);
		expect(outbox.readMutation('notes:s', 'm1')).toEqual(record);
		expect(outbox.readAfter('notes:s', 'c1', 10).cursorFound).toBe(true);
		outbox.pruneScope('notes:s');
		expect(outbox.readMutation('notes:s', 'm1')).toBeUndefined();
	});
});
