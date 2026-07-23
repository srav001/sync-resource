export interface ExternalStoreSource<TValue> {
	readonly getSnapshot: () => TValue;
	readonly subscribe: (callback: () => void) => () => void;
}

type StoreEventSubscribe<TValue> = (callback: (value: TValue) => void) => () => void;

export function createExternalStoreSource<TValue>(
	read: () => TValue,
	subscribeToStore: StoreEventSubscribe<TValue>
): ExternalStoreSource<TValue> {
	let current = read();
	let unsubscribeFromStore: (() => void) | undefined;
	const subscribers = new Set<() => void>();

	function update(value: TValue): void {
		if (Object.is(current, value)) {
			return;
		}
		current = value;
		for (const subscriber of subscribers) {
			subscriber();
		}
	}

	return {
		getSnapshot: () => current,
		subscribe: (callback) => {
			const shouldSubscribe = subscribers.size === 0;
			subscribers.add(callback);
			if (shouldSubscribe) {
				unsubscribeFromStore = subscribeToStore(update);
				current = read();
			}

			let active = true;
			return () => {
				if (!active) {
					return;
				}
				active = false;
				subscribers.delete(callback);
				if (subscribers.size === 0) {
					unsubscribeFromStore?.();
					unsubscribeFromStore = undefined;
				}
			};
		}
	};
}
