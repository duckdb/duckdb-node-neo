import duckdb from 'duckdb';
import { expect, suite, test } from 'vitest';

suite('query', () => {
  test('basic select', async () => {
    const db = await duckdb.open();
    try {
      const con = await duckdb.connect(db);
      try {
        const res = await duckdb.query(con, 'select 17 as seventeen');
        try {
          expect(duckdb.result_statement_type(res)).toBe(duckdb.StatementType.SELECT);
          expect(duckdb.result_return_type(res)).toBe(duckdb.ResultType.QUERY_RESULT);
          expect(duckdb.rows_changed(res)).toBe(0);
          expect(duckdb.column_count(res)).toBe(1);
          expect(duckdb.column_name(res, 0)).toBe('seventeen');
          expect(duckdb.column_type(res, 0)).toBe(duckdb.Type.INTEGER);
        } finally {
          duckdb.destroy_result(res);
        }
      } finally {
        await duckdb.disconnect(con);
      }
    } finally {
      await duckdb.close(db);
    }
  });
  test('basic error', async () => {
    const db = await duckdb.open();
    try {
      const con = await duckdb.connect(db);
      try {
        await expect(duckdb.query(con, 'selct 1')).rejects.toThrow('Parser Error');
      } finally {
        await duckdb.disconnect(con);
      }
    } finally {
      await duckdb.close(db);
    }
  });
  test('test_all_types()', async () => {
    const db = await duckdb.open();
    try {
      const con = await duckdb.connect(db);
      try {
        const res = await duckdb.query(con, 'from test_all_types()');
        try {
          expect(duckdb.result_statement_type(res)).toBe(duckdb.StatementType.SELECT);
          expect(duckdb.result_return_type(res)).toBe(duckdb.ResultType.QUERY_RESULT);
          expect(duckdb.column_count(res)).toBe(53);
          expect(duckdb.column_name(res, 0)).toBe('bool');
          expect(duckdb.column_type(res, 0)).toBe(duckdb.Type.BOOLEAN);
          expect(duckdb.column_name(res, 52)).toBe('list_of_fixed_int_array');
          expect(duckdb.column_type(res, 52)).toBe(duckdb.Type.LIST);
        } finally {
          duckdb.destroy_result(res);
        }
      } finally {
        await duckdb.disconnect(con);
      }
    } finally {
      await duckdb.close(db);
    }
  });
  test('create and insert', async () => {
    const db = await duckdb.open();
    try {
      const con = await duckdb.connect(db);
      try {
        const res = await duckdb.query(con, 'create table test_create_and_insert(i integer)');
        try {
          expect(duckdb.result_statement_type(res)).toBe(duckdb.StatementType.CREATE);
          expect(duckdb.result_return_type(res)).toBe(duckdb.ResultType.NOTHING);
          expect(duckdb.rows_changed(res)).toBe(0);
          expect(duckdb.column_count(res)).toBe(1);
          expect(duckdb.column_name(res, 0)).toBe('Count');
          expect(duckdb.column_type(res, 0)).toBe(duckdb.Type.BIGINT);
        } finally {
          duckdb.destroy_result(res);
        }
        const res2 = await duckdb.query(con, 'insert into test_create_and_insert from range(17)');
        try {
          expect(duckdb.result_statement_type(res2)).toBe(duckdb.StatementType.INSERT);
          expect(duckdb.result_return_type(res2)).toBe(duckdb.ResultType.CHANGED_ROWS);
          expect(duckdb.rows_changed(res2)).toBe(17);
          expect(duckdb.column_count(res2)).toBe(1);
          expect(duckdb.column_name(res2, 0)).toBe('Count');
          expect(duckdb.column_type(res2, 0)).toBe(duckdb.Type.BIGINT);
        } finally {
          duckdb.destroy_result(res2);
        }
      } finally {
        await duckdb.disconnect(con);
      }
    } finally {
      await duckdb.close(db);
    }
  });
});
