import { afterEach, describe, expect, it } from 'vite-plus/test';

import { manager, resource, type ManagerTelemetry } from '../../src/server/index.ts';
import { resetSharedStreamForTests } from '../../src/server/streamMultiplexer.ts';

const schema = { parse: (value: unknown) => value as { id: string } };

afterEach(() => resetSharedStreamForTests());

function notes(telemetry: ManagerTelemetry) {
	const repository = resource({ parse: (value: unknown) => value as { workspaceId: string } }, (method) => ({
		add: method.add({ input: schema, output: schema, handler: ({ input, ctx }) => ctx.ok(input) })
	}));
	return manager({
		key: 'hooks',
		resource: repository,
		authorize: () => true,
		scope: (params) => params.workspaceId,
		telemetry
	}).bind({ workspaceId: 'w1' });
}

describe('manager hooks', () => {
	it('does not fail a write when a hook throws or rejects', async () => {
		const called: string[] = [];
		const result = await notes({
			start() {
				called.push('start');
				throw new Error('sync hook failure');
			},
			success() {
				called.push('success');
				return Promise.reject(new Error('async hook failure'));
			}
		}).add({ input: { id: 'n1' } });
		expect(result.isOk() && result.value).toEqual({ id: 'n1' });
		expect(called).toEqual(['start', 'success']);
	});

	it('waits for an asynchronous hook before continuing', async () => {
		const order: string[] = [];
		let finish!: () => void;
		const write = notes({
			start: () =>
				new Promise<void>((resolve) => {
					finish = () => {
						order.push('hook');
						resolve();
					};
				}),
			success() {
				order.push('success');
			}
		}).add({ input: { id: 'n1' } });
		await new Promise((resolve) => setTimeout(resolve, 5));
		expect(order).toEqual([]);
		finish();
		await write;
		expect(order).toEqual(['hook', 'success']);
	});
});
