import type { ResetManifest, SyncError } from '../shared/index.ts';
import { err, type SyncResult } from '../shared/result.ts';
import type { PageState } from './types.ts';

type RepairResult = SyncResult<void, SyncError>;

export interface RepairScheduler {
	request(run: () => Promise<RepairResult>): Promise<RepairResult>;
	dispose(): void;
}

export function needsRepair(pages: readonly Pick<PageState, 'stale'>[], reset?: ResetManifest): boolean {
	return reset !== undefined || pages.some((page) => page.stale);
}

export function createRepairScheduler(): RepairScheduler {
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
		}, 0);
	}

	return {
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
