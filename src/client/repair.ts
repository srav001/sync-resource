import type { ResetManifest, SyncError } from '../shared/index.ts';
import { err, ok, type SyncResult } from '../shared/result.ts';
import type { PageState } from './types.ts';

type RepairResult = SyncResult<void, SyncError>;

type PageHealth = Pick<PageState, 'repairNeeded' | 'stale' | 'coverage'>;

export type RepairKind = 'none' | 'page' | 'scope';

export interface RepairPlan {
	readonly kind: RepairKind;
	readonly stalePageCount: number;
	readonly reason?: string;
}

export interface RepairScheduler {
	plan(pages: readonly PageHealth[], reset?: ResetManifest): RepairPlan;
	request(run: () => Promise<RepairResult>): Promise<RepairResult>;
	dispose(): void;
}

export function createRepairScheduler(delayMs = 0): RepairScheduler {
	let active: Promise<RepairResult> | undefined;
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

			let resolveQueued: QueuedRepair['resolve'] = () => {};
			const promise = new Promise<RepairResult>((resolve) => {
				resolveQueued = resolve;
			});
			queued = { run, promise, resolve: resolveQueued };
			if (!active) {
				scheduleQueued();
			}
			return promise;
		},
		dispose() {
			disposed = true;
			clearTimeout(timeout);
			timeout = undefined;
			queued?.resolve(err('disposed', 'Repair scheduler was disposed.'));
			active = undefined;
			queued = undefined;
		}
	};
}

interface QueuedRepair {
	readonly run: () => Promise<RepairResult>;
	readonly promise: Promise<RepairResult>;
	resolve(this: void, result: RepairResult): void;
}

export function planRepair(pages: readonly PageHealth[], reset?: ResetManifest): RepairPlan {
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

function countStalePages(pages: readonly PageHealth[]): number {
	let count = 0;
	for (const page of pages) {
		if (page.repairNeeded || page.stale || page.coverage === 'stale') {
			count += 1;
		}
	}
	return count;
}
