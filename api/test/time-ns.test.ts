import { assert, expect, test } from 'vitest';
import { DuckDBTimeNSValue, TIME_NS } from '../src';
import { withConnection } from './util/testHelpers';

// TIME_NS decodes to DuckDBTimeNSValue, which carries `nanos` and is not a
// DuckDBTimeValue. Pointing its JS converter at `bigintFromTimeValue` made
// every TIME_NS column unreadable through the JS path, while the Json path —
// which goes through toString — worked. See JSTypeForTypeId, which has always
// declared TIME_NS as bigint.
test('TIME_NS reads through the JS converters as nanoseconds', async () => {
  await withConnection(async (connection) => {
    const reader = await connection.runAndReadAll(
      `select time_ns '12:34:56.123456789' as v`
    );
    assert.deepEqual(reader.getRowsJS(), [[45296123456789n]]);
    assert.deepEqual(reader.getRowObjectsJS(), [{ v: 45296123456789n }]);
    assert.deepEqual(reader.getColumnsJS(), [[45296123456789n]]);
    // The raw path was never broken, and the two must agree.
    assert.deepEqual(reader.getRows(), [
      [new DuckDBTimeNSValue(45296123456789n)],
    ]);
    // Nor was the Json path, which renders via toString.
    assert.deepEqual(reader.getRowsJson(), [['12:34:56.123456789']]);
  });
});

test('TIME_NS range ends and NULL survive the JS converters', async () => {
  await withConnection(async (connection) => {
    const reader = await connection.runAndReadAll(
      `select * from (values ('${TIME_NS.min}'::time_ns), ('${TIME_NS.max}'::time_ns), (null)) t(v)`
    );
    assert.deepEqual(reader.getColumnsJS(), [
      [TIME_NS.min.nanos, TIME_NS.max.nanos, null],
    ]);
  });
});

test('a TIME value is not accepted as TIME_NS', async () => {
  await withConnection(async (connection) => {
    const reader = await connection.runAndReadAll(`select time '12:34:56' as v`);
    // TIME still reads as micros, through its own converter.
    assert.deepEqual(reader.getRowsJS(), [[45296000000n]]);
  });
});

test('bigintFromTimeNSValue rejects a non-TIME_NS value', async () => {
  const { bigintFromTimeNSValue } = await import('../src');
  expect(() => bigintFromTimeNSValue(42n as never)).toThrowError(
    'Expected DuckDBTimeNSValue'
  );
});
