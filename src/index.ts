import postgres from "postgres";
import { drizzle } from "drizzle-orm/postgres-js";
import { AsyncLocalStorage } from "node:async_hooks";
import { DrizzleQueryError } from "drizzle-orm/errors";
import { z } from "zod";
import type { Cache } from "drizzle-orm/cache/core";
import { eq, Placeholder, sql } from "drizzle-orm";
import { toCamelCase, toSnakeCase } from "drizzle-orm/casing";
import { PostgreSqlContainer } from "@testcontainers/postgresql";

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

type CamelToSnakeCase<S extends string> = S extends `${infer T}${infer U}`
  ? `${T extends Capitalize<T>
      ? Lowercase<T> extends Capitalize<T>
        ? ""
        : "_"
      : ""}${Lowercase<T>}${CamelToSnakeCase<U>}`
  : S;

//#endregion

//#region db client
// Implemented this way for testing

const container = await new PostgreSqlContainer("postgres:17").start();

const client = postgres(container.getConnectionUri(), {
  debug: console.log,
});
export const db = drizzle({ client });

export type Db = typeof db;
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
      client: db,
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
      client: db,
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

type PreparedRequestInput<T extends readonly z.ZodType[] | z.ZodType> =
  MapKeysToPlaceholder<RequestSchemaInput<T>>;

type MapKeysToPlaceholder<T, Key extends string = ""> = T extends object
  ? {
      [K in keyof T]: MapKeysToPlaceholder<
        T[K],
        `${Key extends "" ? "" : `${Key}.`}${CamelToSnakeCase<K & string>}`
      >;
    } & PlaceHolderWrap<CamelToSnakeCase<Key>, T>
  : PlaceHolderWrap<CamelToSnakeCase<Key>, T>;

type UnwrapPlaceholderFn = <
  WrapKey extends string,
  Value,
  Key extends string = WrapKey
>(
  placeholder: PlaceHolderWrap<WrapKey, Value>,
  key?: Key
) => Placeholder<Key, Value>;

const createPlaceholderProxy = (path: string[] = []) =>
  new Proxy(
    {},
    {
      get(_target, prop) {
        // allow coercion to string
        if (prop === Symbol.toPrimitive || prop === "toString") {
          return () => path.join(".");
        }

        const key = typeof prop === "string" ? toSnakeCase(prop) : prop;

        return createPlaceholderProxy([...path, String(key)]);
      },
    }
  );

const placeholderWrapSym = Symbol.for("drizzleRequest.placeholderWrap");

interface PlaceHolderWrap<Key extends string, Value> {
  [placeholderWrapSym]: {
    key: Key;
    value: Value;
  };
}

interface ExecuteFn<
  TRequestMode extends RequestMode,
  RequestSchema extends z.ZodType | readonly z.ZodType[],
  ResultSchema extends z.ZodType | readonly z.ZodType[]
> {
  (
    db: CommonClient,

    request: RequestSchemaInput<RequestSchema>,
    decodedRequest: RequestSchemaOutput<RequestSchema>
  ): Promise<
    TRequestMode extends "many"
      ? DrizzleIFy.DrizzleIFy<z.input<ResultSchema>>
      : readonly DrizzleIFy.DrizzleIFy<z.input<ResultSchema>>[]
  >;
}
interface PreparedExecuteFn<
  TRequestMode extends RequestMode,
  RequestSchema extends z.ZodType | readonly z.ZodType[],
  ResultSchema extends z.ZodType | readonly z.ZodType[]
> {
  (
    $: UnwrapPlaceholderFn,
    db: CommonClient,
    request: PreparedRequestInput<RequestSchema>
  ): Preparable<TRequestMode, ResultSchema>;
}

interface Preparable<
  TRequestMode extends RequestMode,
  ResultSchema extends z.ZodType | readonly z.ZodType[]
> {
  prepare(name: string): {
    execute: (
      placeholderValues?: Record<string, unknown>
    ) => Promise<
      TRequestMode extends "many"
        ? DrizzleIFy.DrizzleIFy<z.input<ResultSchema>>
        : readonly DrizzleIFy.DrizzleIFy<z.input<ResultSchema>>[]
    >;
  };
}

interface DrizzleRequest<
  TRequestMode extends RequestMode,
  ResultSchema extends TRequestMode extends "void" ? never : z.ZodType,
  RequestSchema extends z.ZodType | readonly z.ZodType[] = never,
  ExpectedErrorTags extends readonly (PgErrorTag | RequestErrorTag)[] = []
> {
  (
    ...args: [RequestSchema] extends [never]
      ? [request?: undefined]
      : RequestSchema extends readonly z.ZodType[]
      ? {
          [K in keyof RequestSchema]: z.output<RequestSchema[K]>;
        }
      : [request: z.output<RequestSchema>]
  ): Promise<
    Result<
      TRequestMode extends "void" ? void : z.output<ResultSchema>,
      | CommonDrizzleError
      | PgErrorForTag<ExpectedErrorTags[number]>
      | RequestErrorForTag<ExpectedErrorTags[number]>
    >
  >;
}

export const drizzleRequest = <
  TRequestMode extends RequestMode,
  ResultSchema extends TRequestMode extends "void" ? never : z.ZodType,
  const RequestSchema extends z.ZodType | readonly z.ZodType[] = never,
  ExpectedErrorTags extends readonly (PgErrorTag | RequestErrorTag)[] = [],
  Prepared extends boolean = false
>(
  id: string,
  options: {
    Result?: ResultSchema;
    Request?: RequestSchema;
    mode: TRequestMode;
    prepared?: Prepared;
    expectedErrorTags?: ExpectedErrorTags;
    /**
     * @default "inherit" for non prepared requests, "none" for prepared requests.
     * See `https://github.com/drizzle-team/drizzle-orm/issues/2826`
     *
     */
    transactionMode?: Prepared extends true ? "none" : TransactionMode;
    execute: Prepared extends true
      ? PreparedExecuteFn<TRequestMode, RequestSchema, ResultSchema>
      : ExecuteFn<TRequestMode, RequestSchema, ResultSchema>;
  } & (TRequestMode extends "void"
    ? { Result?: never }
    : { Result: ResultSchema })
): DrizzleRequest<
  TRequestMode,
  ResultSchema,
  RequestSchema,
  ExpectedErrorTags
> => {
  const requestSchema: z.ZodType | undefined = Array.isArray(options.Request)
    ? z.tuple(options.Request as any)
    : (options.Request as z.ZodType | undefined);

  const encodeRequest =
    requestSchema === undefined
      ? () => ok(undefined as void)
      : encode(requestSchema);

  const execute =
    options.prepared === true
      ? (() => {
          const proxy = createPlaceholderProxy();
          const aliasMap = new Map<string, string>();
          const placeholderKeys = new Set<string>();

          const unwrapPlaceholderFn: UnwrapPlaceholderFn = (
            placeholder,
            alias
          ) => {
            const placeholderKey = String(placeholder);
            placeholderKeys.add(placeholderKey);
            if (alias === undefined) {
              return sql.placeholder(placeholderKey);
            }
            aliasMap.set(placeholderKey, alias);
            return sql.placeholder(alias) as any;
          };

          const prepared = (
            options.execute as PreparedExecuteFn<
              TRequestMode,
              RequestSchema,
              ResultSchema
            >
          )(
            unwrapPlaceholderFn,
            getDbClient("none").client,
            proxy as any
          ).prepare(toCamelCase(id));

          const getValueForPlaceholderKey = (
            key: string,
            encodedRequest: RequestSchemaInput<RequestSchema>
          ) => {
            const segments = key.split(".").map(toCamelCase);

            if (segments[0] === "") {
              return encodedRequest;
            }

            let value: any = encodedRequest;
            for (const segment of segments) {
              if (typeof value !== "object" || value === null) {
                throw new InvariantError({
                  message: `[${id}:getValueForPlaceholderKey] expected a non null object at ${key} but got a ${typeof value}`,
                  cause: encodedRequest,
                });
              }
              value = value[segment];
            }
            return value;
          };

          return async (encodedRequest: RequestSchemaInput<RequestSchema>) => {
            annotateCurrentSpan("db.transaction.mode", "none");
            annotateCurrentSpan("drizzleRequest.prepared", "true");

            const placeholderObject = Object.fromEntries(
              Array.from(placeholderKeys).map((key) => [
                aliasMap.get(key) ?? key,
                getValueForPlaceholderKey(key, encodedRequest),
              ])
            );

            try {
              return ok(await prepared.execute(placeholderObject));
            } catch (error) {
              return err(mapExecuteError(error));
            }
          };
        })()
      : async (
          encodedRequest: RequestSchemaInput<RequestSchema>,
          decodedRequest: RequestSchemaOutput<RequestSchema>
        ) => {
          try {
            const context = getDbClient(
              options.prepared ? "none" : options.transactionMode ?? "inherit"
            );
            annotateCurrentSpan("db.transaction.mode", context._tag);
            annotateCurrentSpan("drizzleRequest.prepared", "false");
            return ok(
              await (
                options.execute as ExecuteFn<
                  TRequestMode,
                  RequestSchema,
                  ResultSchema
                >
              )(context.client, encodedRequest as any, decodedRequest)
            );
          } catch (e) {
            return err(mapExecuteError(e));
          }
        };

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

    const executeRes = await execute(
      encodedRequestRes.value as any,
      decodedRequest
    );

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
        await fn(res.value, (...args) => db.$cache.invalidate(...args));
      } catch (e) {
        if (env.NODE_ENV === "development") {
          console.warn("[onTransactionCommit] Error in onCommitFinalizer", e);
        }
      }
      return res;
    }
    currentTransactionContext.onCommitFinalizers.push(async () => {
      try {
        await fn(res.value, (...args) => db.$cache.invalidate(...args));
      } catch (e) {
        //
        if (env.NODE_ENV === "development") {
          console.warn("[onTransactionCommit] Error in onCommitFinalizer", e);
        }
      }
    });
    return res;
  };
