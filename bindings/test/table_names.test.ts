import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { withConnection } from './utils/withConnection';

suite('table_names', () => {
  test('single table name, qualified', async () => {
    await withConnection(async (connection) => {
      const list_value = duckdb.get_table_names(connection, 'from memory.main.t1', true);
      const count = duckdb.get_list_size(list_value);
      expect(count).toBe(1);
      const child0 = duckdb.get_list_child(list_value, 0);
      const name0 = duckdb.get_varchar(child0);
      expect(name0).toBe('memory.main.t1');
    });
  });
  test('single table name, unqualified', async () => {
    await withConnection(async (connection) => {
      const list_value = duckdb.get_table_names(connection, 'from memory.main.t1', false);
      const count = duckdb.get_list_size(list_value);
      expect(count).toBe(1);
      const child0 = duckdb.get_list_child(list_value, 0);
      const name0 = duckdb.get_varchar(child0);
      expect(name0).toBe('t1');
    });
  });
  test('single table name, qualified, quoted', async () => {
    await withConnection(async (connection) => {
      const list_value = duckdb.get_table_names(connection, 'from memory.main."table one"', true);
      const count = duckdb.get_list_size(list_value);
      expect(count).toBe(1);
      const child0 = duckdb.get_list_child(list_value, 0);
      const name0 = duckdb.get_varchar(child0);
      expect(name0).toBe('memory.main."table one"');
    });
  });
  test('single table name, unqualified, quoted', async () => {
    await withConnection(async (connection) => {
      const list_value = duckdb.get_table_names(connection, 'from memory.main."table one"', false);
      const count = duckdb.get_list_size(list_value);
      expect(count).toBe(1);
      const child0 = duckdb.get_list_child(list_value, 0);
      const name0 = duckdb.get_varchar(child0);
      expect(name0).toBe('table one');
    });
  });
  test('multiple table names, qualified', async () => {
    await withConnection(async (connection) => {
      const list_value = duckdb.get_table_names(connection, 'from memory.main.t1, memory.main.t2, memory.main.t1', true);
      const count = duckdb.get_list_size(list_value);
      expect(count).toBe(2);
      const child0 = duckdb.get_list_child(list_value, 0);
      const name0 = duckdb.get_varchar(child0);
      expect(name0).toBe('memory.main.t2');
      const child1 = duckdb.get_list_child(list_value, 1);
      const name1 = duckdb.get_varchar(child1);
      expect(name1).toBe('memory.main.t1');
    });
  });
  test('multiple table names, qualified', async () => {
    await withConnection(async (connection) => {
      const list_value = duckdb.get_table_names(connection, 'from memory.main.t1, memory.main.t2, memory.main.t1', false);
      const count = duckdb.get_list_size(list_value);
      expect(count).toBe(2);
      const child0 = duckdb.get_list_child(list_value, 0);
      const name0 = duckdb.get_varchar(child0);
      expect(name0).toBe('t2');
      const child1 = duckdb.get_list_child(list_value, 1);
      const name1 = duckdb.get_varchar(child1);
      expect(name1).toBe('t1');
    });
  });
  test('no table names', async () => {
    await withConnection(async (connection) => {
      const list_value = duckdb.get_table_names(connection, 'select 1', true);
      const count = duckdb.get_list_size(list_value);
      expect(count).toBe(0);
    });
  });
});