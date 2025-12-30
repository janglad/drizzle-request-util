import postgres from "postgres";
import { drizzle } from "drizzle-orm/postgres-js";
import { AsyncLocalStorage } from "node:async_hooks";
import { DrizzleQueryError } from "drizzle-orm/errors";
import { z } from "zod";
import type { Cache } from "drizzle-orm/cache/core";

//#region stubs or implementations that should probably live somewhere else in your app

// Import your env variables
const env: Record<string, string> = {};

interface TaggedErrorArgs {
  message: string;
  cause?: unknown;
}
abstract class TaggedError extends Error {
  abstract readonly _tag: string;
  constructor(args: TaggedErrorArgs) {
    super(args.message, { cause: args.cause });
  }
}
class InvariantError extends TaggedError {
  readonly _tag = "InvariantError";
  constructor(args: TaggedErrorArgs) {
    super(args);
  }
}
class NoSuchElementError extends TaggedError {
  readonly _tag = "NoSuchElementError";
  constructor(args: TaggedErrorArgs) {
    super(args);
  }
}
class MultipleElementsFoundError extends TaggedError {
  readonly _tag = "MultipleElementsFoundError";
  constructor(args: TaggedErrorArgs) {
    super(args);
  }
}

interface Ok<D> {
  ok: true;
  value: D;
}
export const ok = <D>(value: D): Ok<D> => ({ ok: true, value });
interface Err<E> {
  ok: false;
  error: E;
}
export const err = <E>(error: E): Err<E> => ({ ok: false, error });
type Result<D, E> = Ok<D> | Err<E>;
type InferOk<T> = T extends Result<infer D, any> ? D : never;

class EncodeError extends TaggedError {
  readonly _tag = "EncodeError";
  readonly cause: z.ZodError;
  constructor(args: TaggedErrorArgs & { cause: z.ZodError }) {
    super(args);
    this.cause = args.cause;
  }
}
class DecodeError extends TaggedError {
  readonly _tag = "DecodeError";
  readonly cause: z.ZodError;
  constructor(args: TaggedErrorArgs & { cause: z.ZodError }) {
    super(args);
    this.cause = args.cause;
  }
}

const encode =
  <In, Out>(schema: z.ZodType<Out, In>) =>
  (output: Out): Result<In, EncodeError> => {
    const res = schema.safeEncode(output);
    if (res.success) {
      return ok(res.data);
    }
    return err(
      new EncodeError({
        message: `Failed to encode output`,
        cause: res.error,
      })
    );
  };

const decodeUnknown =
  <In, Out>(schema: z.ZodType<Out, In>) =>
  (input: unknown): Result<Out, DecodeError> => {
    const res = schema.safeDecode(input as any);
    if (res.success) {
      return ok(res.data);
    }
    return err(
      new DecodeError({
        message: `Failed to decode input`,
        cause: res.error,
      })
    );
  };

const tracedFn =
  (id: string) =>
  <T extends (...args: any[]) => any>(fn: T): T => {
    return fn;
  };

function annotateCurrentSpan(key: string, value: string): void;
function annotateCurrentSpan(annotation: Record<string, string>): void;
function annotateCurrentSpan() {
  return;
}

type Prettify<T> = {
  [K in keyof T]: T[K];
} & {};
//#endregion

//#region db client
// Singleton so we don't create a new client during HMR. TODO: add clean up for old client

// Done as such for easier testing
export const getDb = () => ({} as Db);

export type Db = ReturnType<typeof drizzle>;
export type TransactionClient = Parameters<Parameters<Db["transaction"]>[0]>[0];
export type CommonClient = Db | TransactionClient;

type DbContext =
  | {
      _tag: "db";
      client: Db;
    }
  | {
      _tag: "transaction";
      client: TransactionClient;
      onCommitFinalizers: (() => unknown)[];
    };

const dbContext = new AsyncLocalStorage<DbContext>();

/** 
@internal should only be used for testing purposes
*/
export const dangerouslySetDbContext = (context: DbContext) =>
  dbContext.enterWith(context);

type TransactionMode = "require" | "inherit" | "none";

/**
 * Get the current db client based on the transaction mode.
 * * "none" -> return the db client to run outside of a transaction
 * * "inherit" -> if called from within a transaction, return the transaction client, otherwise use the db client
 * * "require" -> throw an error if no transaction context is set, else return the transaction client
 */
const getDbClient = (transactionMode: TransactionMode): DbContext => {
  if (transactionMode === "none") {
    return {
      _tag: "db",
      client: getDb(),
    };
  }
  const context = dbContext.getStore();
  if (transactionMode === "require") {
    if (context === undefined || context._tag !== "transaction") {
      throw new InvariantError({
        message:
          "[drizzleRequest.getDbClient] Transaction mode is 'require' but there is no transaction",
        cause: {
          context: {
            _tag: context?._tag,
          },
        },
      });
    }

    return context satisfies { _tag: "transaction" };
  }
  if (context === undefined) {
    return {
      _tag: "db",
      client: getDb(),
    };
  }
  return context;
};

//#endregion

/** An error coming out of Drizzle that we can't map to a more specific erro */
class CommonDrizzleError extends TaggedError {
  readonly _tag = "CommonDrizzleError";
}

class UniqueViolationError extends TaggedError {
  readonly _tag = "UniqueViolationError";
  readonly cause: DrizzleQueryError;
  constructor(args: TaggedErrorArgs & { cause: DrizzleQueryError }) {
    super(args);
    this.cause = args.cause;
  }
}

const pgErrorCodeToErrorClass = {
  "23505": UniqueViolationError,
};
type PgErrorCodeToErrorClass = typeof pgErrorCodeToErrorClass;
type PgError = InstanceType<
  PgErrorCodeToErrorClass[keyof PgErrorCodeToErrorClass]
>;
type PgErrorTag = PgError["_tag"];
type PgErrorForTag<T> = Extract<PgError, { _tag: T }>;

type RequestError =
  | EncodeError
  | DecodeError
  | MultipleElementsFoundError
  | NoSuchElementError;
type RequestErrorTag = RequestError["_tag"];
type RequestErrorForTag<T> = Extract<RequestError, { _tag: T }>;

/** Try to map an error to a specific PG error, otherwise return a common Drizzle error */
const mapDrizzleError =
  (id: string) =>
  (error: unknown): PgError | CommonDrizzleError => {
    if (
      error instanceof DrizzleQueryError &&
      typeof error.cause === "object" &&
      "code" in error.cause &&
      typeof error.cause.code === "string"
    ) {
      const matchingClass:
        | (typeof pgErrorCodeToErrorClass)[keyof typeof pgErrorCodeToErrorClass]
        | undefined =
        // @ts-expect-error - keyof things
        pgErrorCodeToErrorClass[error.cause.code];
      if (matchingClass !== undefined) {
        return new matchingClass({
          message: `[${id}] Failed to execute request`,
          cause: error,
        });
      }
    }

    return new CommonDrizzleError({
      message: `[${id}] Failed to execute request`,
      cause: error,
    });
  };

type RequestMode = "unique" | "many" | "void";

type RequestSchemaInput<T extends readonly z.ZodType[] | z.ZodType> =
  T extends readonly z.ZodType[]
    ? {
        [K in keyof T]: z.input<T[K]>;
      }
    : z.input<T>;
type RequestSchemaOutput<T extends readonly z.ZodType[] | z.ZodType> =
  T extends readonly z.ZodType[]
    ? {
        [K in keyof T]: z.output<T[K]>;
      }
    : z.output<T>;

export const drizzleRequest = <
  TRequestMode extends RequestMode,
  ResultSchema extends TRequestMode extends "void" ? never : z.ZodType,
  const RequestSchema extends z.ZodType | readonly z.ZodType[] = never,
  ExpectedErrorTags extends readonly (PgErrorTag | RequestErrorTag)[] = []
>(
  id: string,
  options: {
    Result?: ResultSchema;
    Request?: RequestSchema;
    mode: TRequestMode;
    expectedErrorTags?: ExpectedErrorTags;
    /**@default "inherit" */
    transactionMode?: TransactionMode;
    execute: (
      db: CommonClient,
      request: RequestSchemaInput<RequestSchema>,
      decodedRequest: RequestSchemaOutput<RequestSchema>
    ) => Promise<
      TRequestMode extends "many"
        ? DrizzleIFy.DrizzleIFy<z.input<ResultSchema>>
        : readonly DrizzleIFy.DrizzleIFy<z.input<ResultSchema>>[]
    >;
  } & (TRequestMode extends "void"
    ? { Result?: never }
    : { Result: ResultSchema })
): ((
  ...args: [RequestSchema] extends [never]
    ? [request?: undefined]
    : RequestSchema extends readonly z.ZodType[]
    ? {
        [K in keyof RequestSchema]: z.output<RequestSchema[K]>;
      }
    : [request: z.output<RequestSchema>]
) => Promise<
  Result<
    TRequestMode extends "void" ? void : z.output<ResultSchema>,
    | CommonDrizzleError
    | PgErrorForTag<ExpectedErrorTags[number]>
    | RequestErrorForTag<ExpectedErrorTags[number]>
  >
>) => {
  const requestSchema: z.ZodType | undefined = Array.isArray(options.Request)
    ? z.tuple(options.Request as any)
    : (options.Request as z.ZodType | undefined);

  const encodeRequest =
    requestSchema === undefined
      ? () => ok(undefined as never)
      : encode(requestSchema);

  const mapExecuteError = mapDrizzleError(id);

  const decodeResult = (
    val: unknown
  ): Result<
    TRequestMode extends "void" ? void : z.output<ResultSchema>,
    MultipleElementsFoundError | NoSuchElementError | DecodeError
  > => {
    if (options.mode === "void") {
      return ok(undefined as void) as any;
    }
    if (options.Result === undefined) {
      throw new InvariantError({
        message: `[${id}] Schema is required for non void requests, this should have been caught by the type checker?`,
        cause: undefined,
      });
    }
    if (options.mode === "many") {
      return decodeUnknown(options.Result)(val) as any;
    }
    options.mode satisfies "unique";
    if (!Array.isArray(val)) {
      return decodeUnknown(options.Result)(val) as any;
    }
    if (val.length > 1) {
      return err(
        new MultipleElementsFoundError({
          message: `[${id}] Expected a single element, but got ${val.length}`,
          cause: val,
        })
      );
    }
    const first = val[0] as unknown;
    if (first === undefined) {
      return err(
        new NoSuchElementError({
          message: `[${id}] Expected a single element, but got 0`,
          cause: val,
        })
      );
    }
    return decodeUnknown(options.Result)(first) as any;
  };

  const mapToExpectedError = (
    error: RequestError | PgError | CommonDrizzleError
  ): CommonDrizzleError | PgErrorForTag<ExpectedErrorTags[number]> => {
    if (error._tag === "CommonDrizzleError") {
      return error;
    }
    if (options.expectedErrorTags?.includes(error._tag)) {
      return error as unknown as PgErrorForTag<ExpectedErrorTags[number]>;
    }
    return new CommonDrizzleError({
      message: `[${id}] Failed to execute request`,
      cause: error,
    });
  };

  return tracedFn(`drizzleRequest.${id}`)(async (...args) => {
    const decodedRequest = (
      Array.isArray(options.Request) ? args : args[0]
    ) as any;

    const encodedRequestRes = encodeRequest(decodedRequest);
    if (encodedRequestRes.ok === false) {
      return err(mapToExpectedError(encodedRequestRes.error));
    }

    const executeRes = await (async () => {
      try {
        const context = getDbClient(options.transactionMode ?? "inherit");
        annotateCurrentSpan("db.transaction.mode", context._tag);
        return ok(
          await options.execute(
            context.client,
            encodedRequestRes.value as any,
            decodedRequest
          )
        );
      } catch (e) {
        return err(mapExecuteError(e));
      }
    })();

    if (executeRes.ok === false) {
      return err(mapToExpectedError(executeRes.error));
    }

    const decodedResult = decodeResult(executeRes.value);
    if (decodedResult.ok === false) {
      return err(mapToExpectedError(decodedResult.error));
    }

    return decodedResult;
  });
};

namespace DrizzleIFy {
  export type DrizzleIFy<T> = [T] extends [Primitive]
    ? MapPrimitive<T>
    : [T] extends [object]
    ? MergeUnion<{
        [K in keyof T]: DrizzleIFy<T[K]>;
      }>
    : T extends readonly any[]
    ? readonly DrizzleIFy<T[number]>[]
    : MapPrimitive<T>;

  type AllKeys<T> = T extends any ? keyof T : never;

  type CommonKeys<T> = keyof (T extends any ? T : never);

  type NonCommonKeys<T> = Exclude<AllKeys<T>, CommonKeys<T>>;
  type MergeUnion<T> = Prettify<
    {
      [K in CommonKeys<T>]: T extends any
        ? K extends keyof T
          ? T[K]
          : never
        : never;
    } & {
      [K in NonCommonKeys<T>]?: T extends any
        ? K extends keyof T
          ? T[K]
          : never
        : never;
    }
  >;

  type Primitive = string | number | boolean | null | undefined | Date;
  type MapPrimitive<T> = T extends string
    ? string
    : T extends number
    ? number
    : T extends boolean
    ? boolean
    : T extends Date
    ? Date
    : T extends null
    ? null
    : T extends undefined
    ? undefined
    : T;
}

namespace InternalRollbackError {
  const sym = Symbol();
  interface InternalRollbackError<E> {
    [sym]: null;
    cause: E;
  }
  export const make = <E>(cause: E): InternalRollbackError<E> => ({
    [sym]: null,
    cause,
  });
  export const is = (e: unknown): e is InternalRollbackError<unknown> =>
    typeof e === "object" && e !== null && sym in e;
}

export const withTransaction = async <D, E>(
  fn: () => Promise<Result<D, E>>
): Promise<Result<D, E>> => {
  try {
    const client = getDbClient("inherit");

    const finalizersInScope: (() => unknown)[] = [];
    const res = await client.client.transaction((ctx) =>
      dbContext.run(
        { _tag: "transaction", client: ctx, onCommitFinalizers: [] },
        async () => {
          const result = await fn();

          if (result.ok === false) {
            // eslint-disable-next-line @typescript-eslint/only-throw-error -- no need for stack here
            throw InternalRollbackError.make(result.error);
          }

          const hasParentTransaction = client._tag === "transaction";
          const currentTransactionContext = dbContext.getStore();
          if (currentTransactionContext === undefined) {
            throw new InvariantError({
              message:
                "[drizzleRequest.withTransaction] Transaction context got lost within transaction",
              cause: undefined,
            });
          }
          if (currentTransactionContext._tag !== "transaction") {
            throw new InvariantError({
              message:
                "[drizzleRequest.withTransaction] Transaction context is not a transaction context",
              cause: currentTransactionContext,
            });
          }
          const currentOnCommitFinalizers =
            currentTransactionContext.onCommitFinalizers;

          if (!hasParentTransaction) {
            currentOnCommitFinalizers.forEach((finalizer) =>
              finalizersInScope.push(finalizer)
            );
          } else {
            currentOnCommitFinalizers.forEach((finalizer) =>
              client.onCommitFinalizers.push(finalizer)
            );
          }

          return result.value;
        }
      )
    );
    for (let i = finalizersInScope.length - 1; i >= 0; i--) {
      // wrapped wth try catch when added
      await finalizersInScope[i]!();
    }
    return ok(res);
  } catch (error) {
    if (InternalRollbackError.is(error)) {
      return err(error.cause as E);
    }
    throw error;
  }
};

/**
 * Add a function that runs after the current transaction has committed. If there's no active transaction, the function will run immediately.
 * All outcomes of the function will be ignored. Functions will be run in reverse order of addition.
 */
export const onTransactionCommit =
  <D1 extends Result<any, any>>(
    fn: (d: InferOk<D1>, invalidateDbCache: Cache["onMutate"]) => unknown
  ): ((result: Promise<D1>) => Promise<D1>) =>
  async (result): Promise<D1> => {
    const res = await result;
    if (res.ok === false) {
      return res;
    }
    const currentTransactionContext = dbContext.getStore();
    if (
      currentTransactionContext === undefined ||
      currentTransactionContext._tag !== "transaction"
    ) {
      try {
        await fn(res.value, (...args) => getDb().$cache.invalidate(...args));
      } catch (e) {
        if (env.NODE_ENV === "development") {
          console.warn("[onTransactionCommit] Error in onCommitFinalizer", e);
        }
      }
      return res;
    }
    currentTransactionContext.onCommitFinalizers.push(async () => {
      try {
        await fn(res.value, (...args) => getDb().$cache.invalidate(...args));
      } catch (e) {
        //
        if (env.NODE_ENV === "development") {
          console.warn("[onTransactionCommit] Error in onCommitFinalizer", e);
        }
      }
    });
    return res;
  };
