import { ok, type CacheAdapter, type RuntimeTransport, type SyncEnvelope } from '../../src/client/core.ts';

export const noopCache: CacheAdapter = {
	async get() {
		return undefined;
	},
	async set() {},
	async del() {}
};

export const noopTransport: RuntimeTransport = {
	async subscribe() {
		return ok(() => {});
	}
};

export class ManualTransport implements RuntimeTransport {
	options: Parameters<RuntimeTransport['subscribe']>[0] | undefined;
	capturedOptions: Parameters<RuntimeTransport['subscribe']>[0] | undefined;
	unsubscribeCount = 0;

	async subscribe(options: Parameters<RuntimeTransport['subscribe']>[0]) {
		this.options = options;
		this.capturedOptions = options;
		return ok(() => {
			this.unsubscribeCount += 1;
			this.options = undefined;
		});
	}

	emit(envelope: SyncEnvelope): void {
		this.options?.onEnvelope(envelope);
	}

	emitCaptured(envelope: SyncEnvelope): void {
		this.capturedOptions?.onEnvelope(envelope);
	}
}
