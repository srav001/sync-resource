export const workspaceParams = {
	parse(value: unknown): { workspaceId: string } {
		if (
			!value ||
			typeof value !== 'object' ||
			typeof (value as { workspaceId?: unknown }).workspaceId !== 'string'
		) {
			throw new Error('invalid params');
		}
		return value as { workspaceId: string };
	}
};

export const idSchema = {
	parse(value: unknown): { id: string } {
		if (!value || typeof value !== 'object' || typeof (value as { id?: unknown }).id !== 'string') {
			throw new Error('invalid id record');
		}
		return value as { id: string };
	}
};
