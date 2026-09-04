import { describe, expect, it, vi } from 'vite-plus/test';

import { stableStringify } from '../../src/shared/stableJson.ts';

describe('stableStringify', () => {
	it('sorts nested object keys and preserves array order', () => {
		expect(stableStringify({ b: 1, a: { d: 2, c: 3 }, list: [{ z: 1, y: 2 }] })).toBe(
			'{"a":{"c":3,"d":2},"b":1,"list":[{"y":2,"z":1}]}'
		);
		expect(stableStringify({ list: [2, 1] })).toBe('{"list":[2,1]}');
	});
	it('normalizes equivalent objects, dates, and undefined object properties', () => {
		expect(stableStringify({ a: 1, b: 2 })).toBe(stableStringify({ b: 2, a: 1 }));
		expect(stableStringify({ when: new Date('2020-01-01T00:00:00.000Z') })).toBe(
			'{"when":"2020-01-01T00:00:00.000Z"}'
		);
		expect(stableStringify({ a: undefined, b: 1 })).toBe('{"b":1}');
	});
	it('characterizes primitive and top-level undefined output', () => {
		expect(stableStringify('text')).toBe('"text"');
		expect(stableStringify(undefined)).toBe('undefined');
	});
	it('uses one native JSON serialization on the hot path', () => {
		const stringify = vi.spyOn(JSON, 'stringify');
		expect(stableStringify({ z: { b: 2, a: 1 }, a: [3, 2, 1] })).toBe('{"a":[3,2,1],"z":{"a":1,"b":2}}');
		expect(stringify).toHaveBeenCalledTimes(1);
		stringify.mockRestore();
	});
});
