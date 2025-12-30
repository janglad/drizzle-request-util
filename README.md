Some utils I find handy when working with DB requests (in this case specifically drizzle with PG).

## Usage

Copy over [src/index.ts](src/index.ts) to your project. There's a region with stubs or implementations that should probably live somewhere else in your app, implement/edit those as you see fit.

I've adopted the implementation to use a pretty generic `Result` type, but you should be able to easily move this over to your own (Neverthrow etc).

Similarly the current implementation uses Zod (instead of Standard Schema since we're using codecs), you can also quite easily move this over to say Effect Schema.

## Features

### `drizzleRequest`

Build a function that can take in a request and return a result, both encoded/decoded with your provided schema.

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
```

Options:

- id: string
  - The id of the request, will be used for tracing and logs.
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
- transactionMode: `none` | `inherit` | `require`
  - `none` -> don't use a transaction
  - `inherit` -> use the transaction of the parent function
  - `require` -> create a new transaction
- execute: (db, request, decodedRequest) => Promise<Result<ResultSchema, Error>>
  - The function that will be called when the built function is called.
  - Should always return an array (default for core queries in drizzle) so we can validate that the returned result is actually 1 for unique requests.
  - args:
    - db: db client or transaction client
    - request: the encoded request
    - decodedRequest: the decoded request

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
