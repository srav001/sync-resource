// One macrotask turn: every pending promise continuation has run once it resolves. Not for use under fake timers.
export function nextMacrotask(): Promise<void> {
	return new Promise((resolve) => setImmediate(resolve));
}

// Yields microtasks only, so it also works under fake timers.
export async function waitFor(condition: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (condition()) {
			return;
		}
		await Promise.resolve();
	}
	throw new Error('Condition was not reached.');
}
