import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { withConnection } from './utils/withConnection';

function sortedStringsFromList(list_value: duckdb.Value): readonly string[] {
  const strs: string[] = [];
  const count = duckdb.get_list_size(list_value);
  for (let i = 0; i < count; i++) {
    const str_value = duckdb.get_list_child(list_value, i);
    const str = duckdb.get_varchar(str_value);
    strs.push(str);
  }
  strs.sort();
  return strs;
}

suite('table_names', () => {
  test('single table name, qualified', async () => {
    await withConnection(async (connection) => {
      const list_value = duckdb.get_table_names(connection, 'from memory.main.t1', true);
      const strings = sortedStringsFromList(list_value);
      expect(strings).toStrictEqual(['memory.main.t1']);
    });
  });
  test('single table name, unqualified', async () => {
    await withConnection(async (connection) => {
      const list_value = duckdb.get_table_names(connection, 'from memory.main.t1', false);
      const strings = sortedStringsFromList(list_value);
      expect(strings).toStrictEqual(['t1']);
    });
  });
  test('single table name, qualified, quoted', async () => {
    await withConnection(async (connection) => {
      const list_value = duckdb.get_table_names(connection, 'from memory.main."table one"', true);
      const strings = sortedStringsFromList(list_value);
      expect(strings).toStrictEqual(['memory.main."table one"']);
    });
  });
  test('single table name, unqualified, quoted', async () => {
    await withConnection(async (connection) => {
      const list_value = duckdb.get_table_names(connection, 'from memory.main."table one"', false);
      const strings = sortedStringsFromList(list_value);
      expect(strings).toStrictEqual(['table one']);
    });
  });
  test('multiple table names, qualified', async () => {
    await withConnection(async (connection) => {
      const list_value = duckdb.get_table_names(connection, 'from memory.main.t1, memory.main.t2, memory.main.t1', true);
      const strings = sortedStringsFromList(list_value);
      expect(strings).toStrictEqual(['memory.main.t1', 'memory.main.t2']);
    });
  });
  test('multiple table names, qualified', async () => {
    await withConnection(async (connection) => {
      const list_value = duckdb.get_table_names(connection, 'from memory.main.t1, memory.main.t2, memory.main.t1', false);
      const strings = sortedStringsFromList(list_value);
      expect(strings).toStrictEqual(['t1', 't2']);
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