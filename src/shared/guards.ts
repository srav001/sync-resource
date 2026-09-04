export interface ValueRecord<TValue = unknown> {
	readonly [key: string]: TValue;
}
export interface UnrefableTimer {
	unref(): void;
}
export function isRecord(value: unknown): value is ValueRecord {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function isString<TValue>(value: TValue): value is TValue & string {
	return typeof value === 'string';
}

export function isFiniteNumber<TValue>(value: TValue): value is TValue & number {
	return typeof value === 'number' && Number.isFinite(value);
}

export function isCallable<TValue>(value: TValue): value is TValue & Function {
	return typeof value === 'function';
}

export function isUnrefableTimer(value: unknown): value is UnrefableTimer {
	return isRecord(value) && isCallable(value.unref);
}
