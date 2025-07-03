import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';

suite('instance_cache', () => {
  test('create', async () => {
    const cache = duckdb.create_instance_cache();
    expect(cache).toBeTruthy();
  });
  test('empty path', async () => {
    const cache = duckdb.create_instance_cache();
    const db1 = await duckdb.get_or_create_from_cache(cache, '');
    expect(db1).toBeTruthy();
    const db2 = await duckdb.get_or_create_from_cache(cache, '');
    expect(db2).toStrictEqual(db1);
  });
  test('memory path', async () => {
    const cache = duckdb.create_instance_cache();
    const db1 = await duckdb.get_or_create_from_cache(cache, ':memory:');
    expect(db1).toBeTruthy();
    const db2 = await duckdb.get_or_create_from_cache(cache, ':memory:');
    expect(db2).toStrictEqual(db1);
  });
});
