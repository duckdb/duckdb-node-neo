import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';

suite('open', () => {
  test('no args', async () => {
    const db = await duckdb.open();
    expect(db).toBeTruthy();
    duckdb.close_sync(db);
  });
  test('memory arg', async () => {
    const db = await duckdb.open(':memory:');
    expect(db).toBeTruthy();
    duckdb.close_sync(db);
  });
  test('with config', async () => {
    const config = duckdb.create_config();
    duckdb.set_config(config, 'custom_user_agent', 'my_user_agent');
    const db = await duckdb.open(':memory:', config);
    expect(db).toBeTruthy();
    duckdb.close_sync(db);
  });
  test('close', async () => {
    const db = await duckdb.open();
    expect(db).toBeTruthy();
    duckdb.close_sync(db);
    await expect(async () => await duckdb.connect(db)).rejects.toStrictEqual(
      new Error('Failed to connect: instance closed')
    );
    // double-close should be a no-op
    duckdb.close_sync(db);
  });
});
