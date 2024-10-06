import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';

suite('values', () => {
  test('varchar', () => {
    const varchar_text = 'varchar_text';
    const varchar_value = duckdb.create_varchar(varchar_text);
    try {
      expect(duckdb.get_varchar(varchar_value)).toBe(varchar_text);
    } finally {
      duckdb.destroy_value(varchar_value);
    }
  });
  test('int64', () => {
    const int64_bigint = 12345n;
    const int64_value = duckdb.create_int64(int64_bigint);
    try {
      expect(duckdb.get_int64(int64_value)).toBe(int64_bigint);
    } finally {
      duckdb.destroy_value(int64_value);
    }
  });
});
