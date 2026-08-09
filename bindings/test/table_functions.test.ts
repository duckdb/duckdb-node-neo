import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { withConnection } from './utils/withConnection';

suite('table functions', () => {
  test('create', () => {
    const table_function = duckdb.create_table_function();
    expect(table_function).toBeTruthy();
  });
  test('set name', () => {
    const table_function = duckdb.create_table_function();
    duckdb.table_function_set_name(table_function, 'my_func');
  });
  test('add parameters', () => {
    const table_function = duckdb.create_table_function();
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    duckdb.table_function_add_parameter(table_function, int_type);
    const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
    duckdb.table_function_add_named_parameter(
      table_function,
      'my_named_param',
      varchar_type,
    );
  });
  test('supports projection pushdown', () => {
    const table_function = duckdb.create_table_function();
    duckdb.table_function_supports_projection_pushdown(table_function, true);
    duckdb.table_function_supports_projection_pushdown(table_function, false);
  });
  test('set extra info', () => {
    const table_function = duckdb.create_table_function();
    duckdb.table_function_set_extra_info(table_function, {
      'my_extra_info_key': 'my_extra_info_value',
    });
  });
  test('destroy', () => {
    const table_function = duckdb.create_table_function();
    duckdb.destroy_table_function_sync(table_function);
    // Destroying twice is a no-op, not a crash.
    duckdb.destroy_table_function_sync(table_function);
  });
  test('register fails without bind, init and main functions', async () => {
    await withConnection(async (connection) => {
      const table_function = duckdb.create_table_function();
      duckdb.table_function_set_name(table_function, 'my_func');
      // DuckDB requires at least a name, a bind, an init and a main function.
      expect(() =>
        duckdb.register_table_function(connection, table_function),
      ).toThrowError('Failed to register table function');
    });
  });
  test('a scalar function external is not a table function', () => {
    const scalar_function = duckdb.create_scalar_function();
    expect(() =>
      duckdb.table_function_set_name(scalar_function as never, 'my_func'),
    ).toThrowError('Invalid table function argument');
  });
});
