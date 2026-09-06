import { expect, test } from 'vitest';
import {
  DuckDBBigIntType,
  DuckDBBlobType,
  DuckDBDoubleType,
  DuckDBInstance,
  DuckDBIntegerType,
  DuckDBIntervalType,
  DuckDBListType,
  DuckDBSQLNullType,
  DuckDBTimestampTZType,
  DuckDBVarCharType,
  JS,
  JSTypeFor,
  Json,
  JsonTypeFor,
} from '../src';

type Eq<A, B> = (<G>() => G extends A ? 1 : 2) extends <G>() => G extends B
  ? 1
  : 2
  ? true
  : false;
type Assert<T extends true> = T;

// The mappings are checked against the converter maps at their definition; these
// assertions pin the resolutions callers actually see, through JSTypeFor.
export type _JS = [
  Assert<Eq<JSTypeFor<DuckDBIntegerType>, number>>,
  Assert<Eq<JSTypeFor<DuckDBBigIntType>, bigint>>,
  Assert<Eq<JSTypeFor<DuckDBVarCharType>, string>>,
  Assert<Eq<JSTypeFor<DuckDBTimestampTZType>, Date>>,
  Assert<Eq<JSTypeFor<DuckDBBlobType>, Uint8Array>>,
  Assert<Eq<JSTypeFor<DuckDBDoubleType>, number>>,
  Assert<Eq<JSTypeFor<DuckDBListType>, (JS | null)[]>>,
  Assert<Eq<JSTypeFor<DuckDBSQLNullType>, null>>,
];

// Json diverges from JS wherever a value has no lossless JSON form.
export type _Json = [
  Assert<Eq<JsonTypeFor<DuckDBIntegerType>, number>>,
  Assert<Eq<JsonTypeFor<DuckDBBigIntType>, string>>,
  Assert<Eq<JsonTypeFor<DuckDBTimestampTZType>, string>>,
  Assert<Eq<JsonTypeFor<DuckDBBlobType>, string>>,
  Assert<Eq<JsonTypeFor<DuckDBDoubleType>, number | string>>,
  Assert<Eq<JsonTypeFor<DuckDBListType>, (Json | null)[]>>,
  Assert<
    Eq<
      JsonTypeFor<DuckDBIntervalType>,
      { months: number; days: number; micros: string }
    >
  >,
];

test('declared JS and Json types match what the converters produce', async () => {
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  const reader = await connection.runAndReadAll(`select
    42::integer as i,
    42::bigint as bi,
    'x'::varchar as s,
    true as b,
    1.5::double as d,
    'abc'::blob as bl,
    timestamptz '2024-01-15 12:00:00Z' as tstz,
    [1, 2]::integer[] as l,
    null::integer as n`);

  const js = reader.getRowObjectsJS()[0];
  expect(typeof js['i']).toBe('number');
  expect(typeof js['bi']).toBe('bigint');
  expect(typeof js['s']).toBe('string');
  expect(typeof js['b']).toBe('boolean');
  expect(typeof js['d']).toBe('number');
  expect(js['bl']).toBeInstanceOf(Uint8Array);
  expect(js['tstz']).toBeInstanceOf(Date);
  expect(Array.isArray(js['l'])).toBe(true);
  expect(js['n']).toBeNull();

  const json = reader.getRowObjectsJson()[0];
  expect(typeof json['i']).toBe('number');
  expect(typeof json['bi']).toBe('string');
  expect(typeof json['d']).toBe('number');
  expect(typeof json['bl']).toBe('string');
  expect(typeof json['tstz']).toBe('string');
  expect(Array.isArray(json['l'])).toBe(true);
  expect(json['n']).toBeNull();
});
