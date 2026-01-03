import { assert, describe, expect, it, vi } from "vitest";

import { and, between, eq } from "drizzle-orm";
import { integer, pgTable, text } from "drizzle-orm/pg-core";
import z from "zod";
import {
  db,
  drizzleRequest,
  err,
  ok,
  onTransactionCommit,
  withTransaction,
} from ".";

it("Should be live", async () => {
  const result = (await db.execute("SELECT 1")) as unknown;
  expect(result).toHaveLength(1);
});

it("Should properly map db errors", async () => {
  const properlyMappedErrors = pgTable("properly_mapped_errors", {
    id: integer("id").primaryKey(),
  });

  await db.execute(
    "create table properly_mapped_errors (id integer primary key)"
  );

  await db.insert(properlyMappedErrors).values({ id: 1 });

  const res = await drizzleRequest("test.properlyMappedErrors", {
    Result: z.object({ id: z.number() }),
    mode: "unique",
    execute: (db) =>
      db.insert(properlyMappedErrors).values({ id: 1 }).returning(),
    expectedErrorTags: ["UniqueViolationError"],
  })();

  assert(res.ok === false);
  expect(res.error._tag).toBe("UniqueViolationError");
});

it("Should properly map other errors", async () => {
  const res = await drizzleRequest("test.properlyMapOtherErrors", {
    Result: z.object({ id: z.number() }),

    mode: "unique",
    execute: () =>
      // @ts-expect-error - test
      Promise.resolve(undefined),
    expectedErrorTags: ["DecodeError"],
  })();

  assert(res.ok === false);
  expect(res.error._tag).toBe("DecodeError");
});

it("Should handle successful many/unique requests", async () => {
  await db.execute(
    "create table successful_many_unique_requests (id integer primary key)"
  );

  const successfulManyUniqueRequests = pgTable(
    "successful_many_unique_requests",
    {
      id: integer("id").primaryKey(),
    }
  );
  await db.insert(successfulManyUniqueRequests).values({ id: 1 });
  await db.insert(successfulManyUniqueRequests).values({ id: 2 });

  const manyRes = await drizzleRequest(
    "test.successfulManyUniqueRequests.many",
    {
      Result: z.array(z.object({ id: z.number() })),
      mode: "many",
      execute: (db) => db.select().from(successfulManyUniqueRequests),
    }
  )();
  assert(manyRes.ok === true, "Expected success");
  expect(manyRes.value).toEqual([{ id: 1 }, { id: 2 }]);

  const uniqueRes = await drizzleRequest(
    "test.successfulManyUniqueRequests.unique",
    {
      Result: z.object({ id: z.number() }),
      mode: "unique",
      execute: (db) => db.select().from(successfulManyUniqueRequests).limit(1),
    }
  )();
  assert(uniqueRes.ok === true, "Expected success");
  expect(uniqueRes.value).toEqual({ id: 1 });

  const uniqueNotFoundRes = await drizzleRequest(
    "test.successfulManyUniqueRequests.uniqueNotFound",
    {
      Result: z.object({ id: z.number() }),
      mode: "unique",
      execute: (db) =>
        db
          .select()
          .from(successfulManyUniqueRequests)
          .where(eq(successfulManyUniqueRequests.id, 3)),
      expectedErrorTags: ["NoSuchElementError"],
    }
  )();
  assert(uniqueNotFoundRes.ok === false, "Expected error");
  expect(uniqueNotFoundRes.error._tag).toBe("NoSuchElementError");

  const uniqueMultipleFoundRes = await drizzleRequest(
    "test.successfulManyUniqueRequests.uniqueMultipleFound",
    {
      Result: z.object({ id: z.number() }),
      mode: "unique",
      execute: (db) => db.select().from(successfulManyUniqueRequests).limit(2),
      expectedErrorTags: ["MultipleElementsFoundError"],
    }
  )();
  assert(uniqueMultipleFoundRes.ok === false, "Expected error");
  expect(uniqueMultipleFoundRes.error._tag).toBe("MultipleElementsFoundError");
});

it("Should handle successful transactions", async () => {
  await db.execute(
    "create table successful_transactions (id integer primary key)"
  );

  const successfulTransactions = pgTable("successful_transactions", {
    id: integer("id").primaryKey(),
  });

  const insert = drizzleRequest("test.successfulTransactions.insert", {
    mode: "void",
    Request: z.number(),
    execute: (db, request) =>
      db.insert(successfulTransactions).values({ id: request }),
  });

  const res = await withTransaction(async () => {
    const res1 = await insert(1);
    if (res1.ok === false) return res1;
    const res2 = await insert(2);
    if (res2.ok === false) return res2;
    return ok(undefined);
  });

  assert(res.ok === true, "Expected success");

  const count = await db.$count(successfulTransactions);
  expect(count).toBe(2);
});

it("Should rollback on error in transaction", async () => {
  await db.execute("create table rollback_on_error (id integer primary key)");

  const rollbackOnError = pgTable("rollback_on_error", {
    id: integer("id").primaryKey(),
  });

  const insert = drizzleRequest("test.rollbackOnError.insert", {
    mode: "void",
    Request: z.number(),
    execute: (db, request) =>
      db.insert(rollbackOnError).values({ id: request }),
  });

  const errorRes = await withTransaction(async () => {
    const res1 = await insert(1);
    if (res1.ok === false) return res1;
    return err(new Error("Test error"));
  });

  assert(errorRes.ok === false, "Expected error");
  expect(errorRes.error.message).toBe("Test error");

  const postErrorCount = await db.$count(rollbackOnError);
  expect(postErrorCount).toBe(0);

  const defectPromise = withTransaction(async () => {
    const res1 = await insert(1);
    if (res1.ok === false) return res1;
    throw new Error("Test error");
  });

  await expect(defectPromise).rejects.toThrow("Test error");

  const postDefectCount = await db.$count(rollbackOnError);
  expect(postDefectCount).toBe(0);
});

it("Should handle nested transactions", async () => {
  await db.execute("create table nested_transactions (id integer primary key)");

  const nestedTransactions = pgTable("nested_transactions", {
    id: integer("id").primaryKey(),
  });

  const insert = drizzleRequest("test.nestedTransactions.insert", {
    mode: "void",
    Request: z.number(),
    execute: (db, request) =>
      db.insert(nestedTransactions).values({ id: request }),
  });

  const res = await withTransaction(async () => {
    const res1 = await insert(1);
    if (res1.ok === false) return res1;
    const res2 = await withTransaction(async () => {
      const res2 = await insert(2);
      if (res2.ok === false) return res2;
      const res3 = await insert(1);
      if (res3.ok === false) return res3;
      return ok(undefined);
    });
    // ignore res2 error
    const res3 = await insert(3);
    if (res3.ok === false) return res3;
    return ok(undefined);
  });

  assert(res.ok === true, "Expected success");

  const dbState = await db.select().from(nestedTransactions);

  expect(dbState).toEqual([{ id: 1 }, { id: 3 }]);
});

it("Should decode requests", async () => {
  const mockExecute = vi.fn();
  const request = drizzleRequest("test.decodeRequests", {
    mode: "void",
    Request: z.codec(z.literal(1), z.literal(2), {
      encode: () => 1 as const,
      decode: () => 2 as const,
    }),
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return -- types
    execute: (_, request) => mockExecute(request),
  });

  await request(2);
  expect(mockExecute).toHaveBeenCalledWith(1);
});

it("Should decode requests with tuple", async () => {
  const mockExecute = vi.fn();
  const request = drizzleRequest("test.decodeRequestsWithTuple", {
    mode: "void",
    Request: [z.literal(1), z.literal(2)],
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return -- mock
    execute: (_, request) => mockExecute(request),
  });
  await request(1, 2);
  expect(mockExecute).toHaveBeenCalledWith([1, 2]);
});

describe("prepared", () => {
  it("something", async () => {
    const table = pgTable("prepared_test", {
      id: integer("id").primaryKey(),
      name: text("name").notNull(),
      age: integer("age").notNull(),
    });

    await db.execute(
      "create table prepared_test (id integer primary key, name text not null, age integer not null)"
    );
    await db.insert(table).values({ id: 1, name: "test", age: 1 });

    const request = drizzleRequest("test.prepared", {
      mode: "many",
      prepared: true,
      Request: z.object({
        id: z.string(),
        nested: z.object({
          name: z.string(),
        }),
        ageBetween: z.tuple([z.number(), z.number()]),
      }),
      Result: z.array(
        z.object({
          id: z.number(),
          name: z.string(),
          age: z.number(),
        })
      ),
      execute: ($, db, request) => {
        return db
          .select()
          .from(table)
          .where(
            and(
              eq(table.id, $(request.id)),
              eq(table.name, $(request.nested.name, "aliased_name")),
              between(
                table.age,
                $(request.ageBetween[0]),
                $(request.ageBetween[1])
              )
            )
          );
      },
    });
    const res = await request({
      id: "1",
      nested: { name: "test" },
      ageBetween: [1, 10],
    });

    const isPrepared = await db.execute(
      "select * from pg_prepared_statements "
    );
    console.log({ isPrepared });

    assert(res.ok === true, "Expected success");
    expect(res.value).toEqual([{ id: 1, name: "test", age: 1 }]);
  });
});

describe("onTransactionCommit", () => {
  it("Should immediately run when no transaction is active", async () => {
    const fn = vi.fn();

    await onTransactionCommit(fn)(Promise.resolve(ok(1)));

    expect(fn).toHaveBeenCalledWith(1, expect.any(Function));
  });

  it("Should not run for failures", async () => {
    const fn = vi.fn();
    await onTransactionCommit(fn)(
      Promise.resolve(err(new Error("Test error")))
    );
    expect(fn).not.toHaveBeenCalled();
  });

  it("Should not run for defects", async () => {
    const fn = vi.fn();
    try {
      await expect(
        onTransactionCommit(fn)(Promise.reject(new Error("Test error")))
      ).rejects.toThrow("Test error");
    } catch (_) {}
    expect(fn).not.toHaveBeenCalled();
  });

  it("Should wait with running until parent transaction commits", async () => {
    const fn = vi.fn();
    const { resolve: open, promise: latch } = Promise.withResolvers<void>();

    const promise = withTransaction(async () => {
      await onTransactionCommit(fn)(Promise.resolve(ok(1)));
      open();
      return ok(1);
    });

    await latch;
    expect(fn).not.toHaveBeenCalled();
    await promise;
    expect(fn).toHaveBeenCalledWith(1, expect.any(Function));
  });
  it("Should wait with running until parent transaction commits for nested transactions", async () => {
    const fn = vi.fn();
    const { resolve: open, promise: latch } = Promise.withResolvers<void>();

    const promise = withTransaction(async () => {
      await withTransaction(async () => {
        await onTransactionCommit(fn)(Promise.resolve(ok(1)));
        return ok(undefined);
      });
      open();
      return ok(1);
    });

    await latch;
    expect(fn).not.toHaveBeenCalled();
    await promise;
    expect(fn).toHaveBeenCalledWith(1, expect.any(Function));
  });
});
