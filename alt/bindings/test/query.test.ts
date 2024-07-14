import duckdb from 'duckdb';
import { expect, suite, test } from 'vitest';

suite('query', () => {
  test('basic select', async () => {
    const db = await duckdb.open(':memory:');
    try {
      const con = await duckdb.connect(db);
      const res = await duckdb.query(con, 'select 17 as seventeen');
      expect(duckdb.column_count(res)).toBe(1);
      expect(duckdb.column_name(res, 0)).toBe('seventeen');
      expect(duckdb.column_type(res, 0)).toBe(duckdb.Type.INTEGER);
    } finally {
      await duckdb.close(db);
    }
  });
  test('test_all_types()', async () => {
    const db = await duckdb.open(':memory:');
    try {
      const con = await duckdb.connect(db);
      const res = await duckdb.query(con, 'from test_all_types()');
      expect(duckdb.column_count(res)).toBe(53);
      expect(duckdb.column_name(res, 0)).toBe('bool');
      expect(duckdb.column_type(res, 0)).toBe(duckdb.Type.BOOLEAN);
      expect(duckdb.column_name(res, 52)).toBe('list_of_fixed_int_array');
      expect(duckdb.column_type(res, 52)).toBe(duckdb.Type.LIST);
    } finally {
      await duckdb.close(db);
    }
  });
});
