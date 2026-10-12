// One macrotask turn: every pending promise continuation has run once it resolves. Not for use under fake timers.
export function nextMacrotask(): Promise<void> {
	return new Promise((resolve) => setImmediate(resolve));
}
