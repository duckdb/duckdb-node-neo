import { expect, test } from 'vitest';
import {
  ANY,
  DATE,
  DuckDBConnection,
  DuckDBInstance,
  DuckDBTypeId,
  INTEGER,
  JSInputTypeForTypeId,
  JSTypeForTypeId,
  SMALLINT,
  STRUCT,
  TIMESTAMP,
  TIMESTAMPTZ,
  TIMESTAMP_MS,
  TIMESTAMP_NS,
  TIMESTAMP_S,
  UNION,
  VARCHAR,
  VARIANT,
  jsToDuckDBValue,
  structValue,
  variantValue,
} from '../src';

// Everything the read path produces, the write path must accept. VARIANT is the
// documented exception, and the only one: a VARIANT holds its own type, reading
// discards it, and JS alone is not enough to write it back.
// Collects the ids that FAIL, so the union means something: a passing id
// contributes `never` and disappears, while a failing one contributes itself
// and is named in the error. Collecting the passing ids instead would assert
// nothing, since `never | true` is just `true`.
type NotRoundTrippable = {
  [Id in Exclude<DuckDBTypeId, DuckDBTypeId.VARIANT>]: JSTypeForTypeId[Id] extends JSInputTypeForTypeId[Id]
    ? never
    : Id;
}[Exclude<DuckDBTypeId, DuckDBTypeId.VARIANT>];
type AssertNone<T extends never> = T;
export type _RoundTrip = AssertNone<NotRoundTrippable>;

async function connect(): Promise<DuckDBConnection> {
  return (await DuckDBInstance.create(':memory:')).connect();
}

test('jsToDuckDBValue rejects unwritable types', () => {
  expect(() => jsToDuckDBValue('x' as never, ANY)).toThrowError(
    'Cannot write values of type: ANY'
  );
  expect(jsToDuckDBValue(null, ANY)).toBeNull();
});

test('VARIANT accepts a wrapper, or a scalar it can infer', async () => {
  const connection = await connect();
  await connection.run(`create table v (id integer, payload variant)`);
  const appender = await connection.createAppender('v');
  const rows: [number, JSInputTypeForTypeId[DuckDBTypeId.VARIANT]][] = [
    [1, 42],
    [2, 'plain'],
    [3, true],
    [4, variantValue(7, INTEGER)],
    [5, variantValue(structValue({ k: 'n' }), STRUCT({ k: VARCHAR }))],
  ];
  for (const [id, payload] of rows) {
    appender.appendInteger(id);
    appender.appendValue(jsToDuckDBValue(payload, VARIANT), VARIANT);
    appender.endRow();
  }
  appender.closeSync();

  const reader = await connection.runAndReadAll(
    `select payload::varchar as p from v order by id`
  );
  expect(reader.getRowsJS().map((r) => r[0])).toEqual([
    '42',
    'plain',
    'true',
    '7',
    "{'k': n}",
  ]);
});

test('input is checked against the type when the type is known', () => {
  // Compile-time assertions; never executed, and checked by `tsc -b test`.
  void (() => {
    // @ts-expect-error a Date is not an INTEGER input
    jsToDuckDBValue(new Date(), INTEGER);
    // @ts-expect-error a string is not a TIMESTAMP input
    jsToDuckDBValue('2024-01-15', TIMESTAMP);
    // A Date could be DATE, TIMESTAMP or TIMESTAMP_MS, so it cannot be a bare
    // VARIANT input — wrap it to say which.
    // @ts-expect-error a bare Date is not an accepted VARIANT input
    jsToDuckDBValue(new Date(), VARIANT);
  });
  expect(jsToDuckDBValue(new Date(0), TIMESTAMP)).toBeDefined();
  expect(jsToDuckDBValue(null, TIMESTAMP)).toBeNull();
});

test('an unknown UNION tag is named, not dereferenced', () => {
  const union = UNION({ name: VARCHAR, age: SMALLINT });
  expect(() =>
    jsToDuckDBValue({ tag: 'nope', value: 'x' }, union)
  ).toThrowError('Tag "nope" is not a member of UNION("name" VARCHAR, "age" SMALLINT)');
  // Also refused with a null payload. Before the tag was checked, this case
  // returned NULL: the null short-circuited the dispatch before the undefined
  // member type could be dereferenced, so a bad tag went unreported.
  expect(() =>
    jsToDuckDBValue({ tag: 'nope', value: null }, union)
  ).toThrowError('is not a member of');
  // A null for the whole union value is still NULL.
  expect(jsToDuckDBValue(null, union)).toBeNull();
  expect(jsToDuckDBValue({ tag: 'name', value: 'x' }, union)).toBeDefined();
});

test('an Invalid Date is refused by every temporal converter', () => {
  const invalid = new Date('nonsense');
  for (const type of [
    DATE,
    TIMESTAMP,
    TIMESTAMP_S,
    TIMESTAMP_MS,
    TIMESTAMP_NS,
    TIMESTAMPTZ,
  ] as const) {
    expect(
      () => jsToDuckDBValue(invalid, type),
      `${type} should refuse an Invalid Date`
    ).toThrowError('Cannot convert an Invalid Date');
  }
  expect(jsToDuckDBValue(new Date(0), DATE)).toBeDefined();
});

