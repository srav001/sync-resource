import { describe, expect, it } from 'vite-plus/test';

import { normalizeValidator } from '../../src/shared/index.ts';

describe('schema normalization', () => {
	it('accepts function-style validators', () => {
		const validator = normalizeValidator<{ id: string }>({
			parse(value) {
				if (typeof value !== 'object' || value === null || !('id' in value)) {
					throw new Error('invalid');
				}
				return value as { id: string };
			}
		});
		expect(validator.parse({ id: 'a' })).toEqual({ id: 'a' });
		expect(() => validator.parse({})).toThrow('invalid');
	});
	it('accepts Standard Schema and preserves transformed output', () => {
		const schema = {
			'~standard': {
				version: 1 as const,
				vendor: 'test',
				validate(value: unknown) {
					return typeof value === 'string' ? { value: value.trim().toUpperCase() } : { issues: ['string'] };
				}
			}
		};
		const validator = normalizeValidator<string>(schema);
		expect(validator.parse(' hi ')).toBe('HI');
		expect(() => validator.parse(3)).toThrow('Validation failed');
	});
	it('rejects asynchronous Standard Schema validation', () => {
		const schema = {
			'~standard': { version: 1 as const, vendor: 'test', validate: async () => ({ value: 'ok' }) }
		};
		expect(() => normalizeValidator<string>(schema).parse('x')).toThrow(
			'Async validation is not supported by Sync Resource core validators.'
		);
	});
});
