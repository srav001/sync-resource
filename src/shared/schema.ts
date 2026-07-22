export interface Validator<TValue> {
	parse(value: unknown): TValue;
}

export type StandardSchemaValidationResult<TOutput> =
	| {
			readonly value: TOutput;
			readonly issues?: undefined;
	  }
	| {
			readonly value?: undefined;
			readonly issues: readonly unknown[];
	  };

export interface StandardSchema<TInput = unknown, TOutput = TInput> {
	readonly '~standard': {
		readonly version: 1;
		readonly vendor: string;
		readonly types?:
			| {
					readonly input: TInput;
					readonly output: TOutput;
			  }
			| undefined;
		validate(
			value: TInput
		): StandardSchemaValidationResult<TOutput> | Promise<StandardSchemaValidationResult<TOutput>>;
	};
}

export type AnySchema<TValue> = StandardSchema<unknown, TValue> | Validator<TValue>;

export type InferSchemaOutput<TSchema> =
	TSchema extends StandardSchema<unknown, infer TOutput>
		? TOutput
		: TSchema extends Validator<infer TOutput>
			? TOutput
			: never;

function isStandardSchema<TValue>(schema: AnySchema<TValue>): schema is StandardSchema<unknown, TValue> {
	return (
		typeof schema === 'object' &&
		schema !== null &&
		'~standard' in schema &&
		typeof schema['~standard']?.validate === 'function'
	);
}

export function normalizeValidator<TValue>(schema: AnySchema<TValue>): Validator<TValue> {
	if (isStandardSchema(schema)) {
		return {
			parse(value: unknown): TValue {
				const result = schema['~standard'].validate(value);
				if (result instanceof Promise) {
					throw new Error('Async validation is not supported by Live Resource core validators.');
				}
				if ('issues' in result && result.issues) {
					throw new Error(`Validation failed: ${JSON.stringify(result.issues)}`);
				}
				return result.value;
			}
		};
	}

	return schema;
}
