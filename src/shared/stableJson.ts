export function stableStringify(value: unknown): string {
	return JSON.stringify(toStableJson(value)) ?? 'undefined';
}

export function toStableJson(value: unknown): unknown {
	if (Array.isArray(value)) {
		return value.map(toStableJson);
	}

	if (value instanceof Date) {
		return value.toISOString();
	}

	if (typeof value !== 'object' || value === null) {
		return value;
	}

	const output: Record<string, unknown> = {};
	for (const key of Object.keys(value).sort()) {
		const nextValue = (value as Record<string, unknown>)[key];
		if (nextValue !== undefined) {
			output[key] = toStableJson(nextValue);
		}
	}
	return output;
}
