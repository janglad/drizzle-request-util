Some utils I find handy when working with DB requests (in this case specifically drizzle with PG).

## Usage

Copy over [src/index.ts](src/index.ts) to your project. There's a region with stubs or implementations that should probably live somewhere else in your app, implement/edit those as you see fit.

I've adopted the implementation to use a pretty generic `Result` type, but you should be able to easily move this over to your own (Neverthrow etc).

Similarly the current implementation uses Zod (instead of Standard Schema since we're using codecs), you can also quite easily move this over to say Effect Schema.

## Features

### `drizzleRequest`

Build a function to make a drizzle request. Will encode inputs and decode outputs, convert Postgres errors to specific JS error objects, validate correct response size, can automatically use transaction client from context if the returned function is called from within one. Supports prepared statements.

```ts
const UserId = z.uuid().brand("UserId");
const User = z.object({ ...yourSchema });

const getUserById = drizzleRequest("getUserById", {
  mode: "unique",
  Request: UserId,
  Result: User,
  expectedErrorTags: ["NoSuchElementError"],
  execute: (db, userId) => db.query.user.findMany({ where: { id: userId } }),
});

const user = await getUserById(userId);
// ^ Result<User, NoSuchElementError | CommonDrizzleError>

const insertUser = drizzleRequest("insertUser", {
  //...,
  expectedErrorTags: ["UniqueViolationError"],
  execute: (db, userId) => db.insert(users).values({ id: userId }).returning(),
});

const insertedUser = withTransaction(
  () =>
    // Will automatically use the transaction client
    await insertUser(userId)
);
// ^ Result<User, UniqueViolationError | CommonDrizzleError>

// Calls .prepare() under the hood and returns the .execute() function with encoding, decoding, error handling etc
const preparedGetUser = drizzleRequest("preparedGetUser", {
  mode: "unique",
  prepare: true,
  Request: z.object({
    id: UserId,
    ageBetween: z.tuple([z.number(), z.number()]),
    nested: z.object({
      name: z.string(),
    }),
  }),
  Result: User,
  expectedErrorTags: ["NoSuchElementError"],
  execute: (placeholder, db, request) =>
    db.query.user.findMany({
      where: {
        id: placeHolder(request.userId),
        // ^ Placeholder<'user_id', string>
        age: {
          gt: placeholder(request.ageBetween[0]),
          //  ^ Placeholder<'age_between.0', number>
          lt: placeholder(request.ageBetween[1], "aliased_max_age"),
          //  ^ Placeholder<'aliased_max_age', number>
        },
        name: placeHolder(request.nested.name),
        //     ^ Placeholder<'nested.name', string>
      },
    }),
});
```

Options:

- id: string
  - The id of the request, will be used for tracing and logs. When using `prepare` the id converted to snake case will be used as the prepared statement name.
- mode: `unique` | `many` | `void`
  - `unique` -> expect a single result, return `NoSuchElementError` if no result is found, return `MultipleElementsFoundError` if multiple results are found
  - `many` -> expect 0 to many results
  - `void` -> ignore the result
- Request: `ZodType` or `readonly ZodType[]`. Optional.
  - When an array, the built function can be called with a tuple of the values (e.g. `getPostForAuthorAndDate(authorId, date)`). This prevents passing multiple arguments as an object which breaks react cache.
  - You can access both the encoded and decoded request in the `execute` function.
- Result: `ZodType`. Not required for `void` requests.
  - The result schema, will be used to decode the result of the `execute` function.
- expectedErrorTags: `PgErrorTag` | `RequestErrorTag`[]
  - error tags that you want explicitly returned from the built function. Others will be wrapped in a `CommonDrizzleError`.
- prepare: `boolean`
  - Whether to use drizzle's `.prepare()` or not.
- transactionMode: `none` | `inherit` | `require`. Require also makes sure the transaction is rolled back when using 'unique' and you accidentally insert/update multiple rows. Always `none` when using `prepared`.
  - `none` -> don't use a transaction
  - `inherit` (default) -> run in a transaction if called within `withTransaction`, otherwise use regular db client.>
  - `require` -> throw an error if the function is not called within `withTransaction`. Can be handy in for update/insert/delete combination with `mode: 'unique'` as this'll make the db state gets rolled back if you accidentally touch more than 1 record (unless you handle the error yourself)
- execute:
  - The function that will be called when the built function is called.
  - Should always return an array (default for core queries in drizzle) so we can validate that the returned result is actually 1 for unique requests.
  - args:
    - when `prepare` is false (default)
      - db: either db client or transaction client
      - encoded request
      - decoded request
    - when `prepare` is true
      - placeholder: (requestProxy, optional alias override). Will create a `sql.placeHolder` coming from drizzle. Key names are separated by periods and transformed to camelCase. e.g. `placeHolder(user.ageRange[0])` calls `sql.placeHolder('user.age_range.0')` under the hood. These aliases are internally mapped for you when calling the function returned from `drizzleRequest`.
      - db: db client
      - encodedPayloadProxy: proxy object with the shape of the encoded payload. To be used with `placeholder()`

## `withTransaction`

Takes in an async function that returns a `Result` and makes sure all `drizzleRequest` functions within it run a transaction (unless `transactionMode` is `none`). If the function throws or returns an error, the transaction will be rolled back.

```ts
const res = await withTransaction(async () => {
  const user = await getUserById(userId);
  if (user.ok === false) return user;
  return ok(user.value);
});
```

## `onTransactionCommit`

Adds a function that will run after the current transaction has committed. If there's no active transaction, the function will run immediately. All outcomes of the function will be ignored. Functions will be run in reverse order of addition. Handy to make sure you only invalidate cache after the values have actually been committed to the database. Written in a way that's nice to use with pipes

```ts
const insertUser = flow(
  drizzleRequest(/*...*/),
  onTransactionCommit((userId) => invalidateUserCache(userId))
);

await withTransaction(async () => {
  await insertUser(userId);
  //   ...
});
```
