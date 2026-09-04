import { isCallable, isRecord } from './guards.ts';

type StablePrimitive = string | number | bigint | boolean | symbol | null | undefined;
type StableJson = StablePrimitive | Function | readonly StableJson[] | { readonly [key: string]: StableJson };

function isStablePrimitive<TValue>(value: TValue): value is TValue & StablePrimitive {
	return value === null || (typeof value !== 'object' && typeof value !== 'function');
}

export function stableStringify<TValue>(value: TValue): string {
	return JSON.stringify(toStableJson(value)) ?? 'undefined';
}

export function toStableJson<TValue>(value: TValue): StableJson {
	if (Array.isArray(value)) {
		return value.map(toStableJson);
	}

	if (value instanceof Date) {
		return value.toISOString();
	}

	if (isStablePrimitive(value) || isCallable(value)) {
		return value;
	}

	if (!isRecord(value)) {
		return undefined;
	}

	const output: Record<string, StableJson> = {};
	for (const key of Object.keys(value).sort()) {
		const nextValue = value[key];
		if (nextValue !== undefined) {
			output[key] = toStableJson(nextValue);
		}
	}
	return output;
}
