import { err, ok, type SyncResult } from '../shared/result.ts';
import type { PageState, ResetManifest, SyncError } from './types.ts';

export type RepairKind = 'none' | 'page' | 'scope';

export interface RepairPlan {
	readonly kind: RepairKind;
	readonly stalePageCount: number;
	readonly reason?: string;
}

export interface RepairScheduler {
	plan(pages: readonly PageState[], reset?: ResetManifest): RepairPlan;
	request(run: () => Promise<SyncResult<void, SyncError>>): Promise<SyncResult<void, SyncError>>;
	dispose(): void;
}

export function createRepairScheduler(delayMs = 0): RepairScheduler {
	let active: Promise<SyncResult<void, SyncError>> | undefined;
	let queued: QueuedRepair | undefined;
	let timeout: ReturnType<typeof setTimeout> | undefined;
	let disposed = false;

	function startQueued(): void {
		const next = queued;
		if (!next || active || disposed) {
			return;
		}
		queued = undefined;
		const current = Promise.resolve()
			.then(next.run)
			.catch((cause) => err('internal', cause instanceof Error ? cause.message : String(cause), { cause }));
		active = current;
		current.then(next.resolve);
		current.finally(() => {
			if (active === current) {
				active = undefined;
			}
			if (queued && !disposed) {
				scheduleQueued();
			}
		});
	}

	function scheduleQueued(): void {
		if (timeout || disposed) {
			return;
		}
		timeout = setTimeout(() => {
			timeout = undefined;
			startQueued();
		}, delayMs);
	}

	return {
		plan: planRepair,
		request(run) {
			if (disposed) {
				return Promise.resolve(err('disposed', 'Repair scheduler was disposed.'));
			}
			if (queued) {
				return queued.promise;
			}

			let resolveQueued: (result: SyncResult<void, SyncError>) => void = () => {};
			const promise = new Promise<SyncResult<void, SyncError>>((resolve) => {
				resolveQueued = resolve;
			});
			queued = {
				run,
				promise,
				resolve: resolveQueued
			};
			if (!active) {
				scheduleQueued();
			}
			return promise;
		},
		dispose() {
			disposed = true;
			if (timeout) {
				clearTimeout(timeout);
				timeout = undefined;
			}
			queued?.resolve(err('disposed', 'Repair scheduler was disposed.'));
			active = undefined;
			queued = undefined;
		}
	};
}

interface QueuedRepair {
	readonly run: () => Promise<SyncResult<void, SyncError>>;
	readonly promise: Promise<SyncResult<void, SyncError>>;
	resolve(this: void, result: SyncResult<void, SyncError>): void;
}

export function planRepair(pages: readonly PageState[], reset?: ResetManifest): RepairPlan {
	const stalePageCount = countStalePages(pages);
	if (reset) {
		return {
			kind: reset.reason === 'retention_gap' ? 'scope' : 'page',
			stalePageCount,
			reason: reset.reason
		};
	}

	if (stalePageCount === 0) {
		return {
			kind: 'none',
			stalePageCount
		};
	}

	return {
		kind: 'page',
		stalePageCount
	};
}

export async function noRepairNeeded(): Promise<SyncResult<void, SyncError>> {
	return ok(undefined);
}

function countStalePages(pages: readonly PageState[]): number {
	let count = 0;
	for (const page of pages) {
		if (page.repairNeeded || page.stale || page.coverage === 'stale') {
			count += 1;
		}
	}
	return count;
}
