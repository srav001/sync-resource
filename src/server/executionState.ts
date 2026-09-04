import type { OperationExecution, ResourceCommitContext } from './types.js';

const executionByCommitContext = new WeakMap<ResourceCommitContext, OperationExecution>();

export function withCommitExecution<TContext extends ResourceCommitContext>(
	context: TContext,
	execution: OperationExecution
): TContext {
	executionByCommitContext.set(context, execution);
	// Preserve cancellation for legacy plain commit implementations without adding it to the public commit record shape.
	Object.defineProperty(context, 'signal', { enumerable: false, value: execution.signal });
	return context;
}

export function executionForCommit(context: ResourceCommitContext): OperationExecution | undefined {
	return executionByCommitContext.get(context);
}
