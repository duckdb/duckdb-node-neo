import { expect, test } from 'vitest';
import {
  DuckDBDataChunk,
  DuckDBInstance,
  DuckDBLogicalType,
  INTEGER,
  VARCHAR,
} from '../src';

// An out-of-range column index used to reach DuckDB unchecked, which crashed
// the process for some accessors and returned wrong data for others.
test('a data chunk rejects an out-of-range column index', () => {
  const chunk = DuckDBDataChunk.create([INTEGER, VARCHAR], 1);
  expect(String(chunk.getColumnVector(1).type)).toBe('VARCHAR');
  expect(() => chunk.getColumnVector(2)).toThrowError(
    'column index 2 is out of range (count: 2)'
  );
  expect(() => chunk.getColumnValues(2)).toThrowError('out of range');
  expect(() => chunk.setColumnValues(2, [1])).toThrowError('out of range');
  // A negative index reaches the binding as a large unsigned one.
  expect(() => chunk.getColumnVector(-1)).toThrowError('out of range');
});

test('a result rejects an out-of-range column index', async () => {
  const connection = await (await DuckDBInstance.create(':memory:')).connect();
  const result = await connection.run(`select 1 as a, 'x' as b`);
  expect(result.columnName(1)).toBe('b');
  expect(() => result.columnName(2)).toThrowError(
    'column index 2 is out of range (count: 2)'
  );
  // This one returned type id 0 (INVALID) rather than raising.
  expect(() => result.columnTypeId(2)).toThrowError('out of range');
  expect(() => result.columnType(2)).toThrowError('out of range');
});

test('an appender rejects an out-of-range column index', async () => {
  const connection = await (await DuckDBInstance.create(':memory:')).connect();
  await connection.run(`create table t (a integer, b varchar)`);
  const appender = await connection.createAppender('t');
  expect(String(appender.columnType(1))).toBe('VARCHAR');
  expect(() => appender.columnType(2)).toThrowError(
    'column index 2 is out of range (count: 2)'
  );
  appender.closeSync();
});

test('a struct or union logical type rejects an out-of-range index', () => {
  const structType = DuckDBLogicalType.createStruct(
    ['a'],
    [INTEGER.toLogicalType()]
  );
  expect(structType.entryName(0)).toBe('a');
  expect(() => structType.entryName(1)).toThrowError(
    'struct entry index 1 is out of range (count: 1)'
  );
  expect(() => structType.entryType(1)).toThrowError('out of range');

  const unionType = DuckDBLogicalType.createUnion(
    ['a'],
    [INTEGER.toLogicalType()]
  );
  expect(unionType.memberTag(0)).toBe('a');
  expect(() => unionType.memberTag(1)).toThrowError(
    'union member index 1 is out of range (count: 1)'
  );
  expect(() => unionType.memberType(1)).toThrowError('out of range');
});

test('an enum logical type rejects an out-of-range value index', () => {
  // This one read out of bounds: index 2 on a 2-value enum segfaulted, and a
  // larger index returned whatever happened to be in memory.
  const enumType = DuckDBLogicalType.createEnum(['alpha', 'beta']);
  expect(enumType.value(1)).toBe('beta');
  expect(enumType.values()).toEqual(['alpha', 'beta']);
  expect(() => enumType.value(2)).toThrowError(
    'enum value index 2 is out of range (count: 2)'
  );
  expect(() => enumType.value(99)).toThrowError('out of range');
});

test('a prepared statement rejects an out-of-range index', async () => {
  const connection = await (await DuckDBInstance.create(':memory:')).connect();
  const prepared = await connection.prepare(`select ?::integer as a`);
  expect(prepared.parameterCount).toBe(1);
  // Parameter indexes are 1-based, so the count is the largest valid one.
  expect(() => prepared.parameterName(2)).toThrowError(
    'parameter index 2 is out of range (count: 1)'
  );
  // INVALID is also what an unbound parameter reports, so this one is checked
  // before the call rather than from the return value.
  expect(() => prepared.parameterTypeId(2)).toThrowError('out of range');
  expect(() => prepared.parameterTypeId(0)).toThrowError('out of range');
});
