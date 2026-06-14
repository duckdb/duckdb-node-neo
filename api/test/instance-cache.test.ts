import fs from 'fs';
import { assert, beforeAll, describe, test } from 'vitest';
import {
  DuckDBConnection,
  DuckDBInstance,
} from '../src';
import { DuckDBInstanceCache } from '../src/DuckDBInstanceCache';
import {
  setDefaultTimezone,
} from './util/testHelpers';

describe('instance cache', () => {
  beforeAll(setDefaultTimezone);

  test('instance cache - same instance', async () => {
    const cache = new DuckDBInstanceCache();
    const instance1 = await cache.getOrCreateInstance(':memory:m1');
    const connection1 = await instance1.connect();
    await connection1.run(`create table t1 as select 1`);

    const instance2 = await cache.getOrCreateInstance(':memory:m1');
    const connection2 = await instance2.connect();
    await connection2.run(`from t1`);
  });
  // Need to support explicitly destroying instance cache?
  test.skip('instance cache - different instances', async () => {
    let instance1: DuckDBInstance | undefined;
    let instance2: DuckDBInstance | undefined;
    try {
      const cache = new DuckDBInstanceCache();
      const instance1 = await cache.getOrCreateInstance(
        'instance_cache_test_a.db',
      );
      const connection1 = await instance1.connect();
      await connection1.run(`attach ':memory:' as mem1`);

      const instance2 = await cache.getOrCreateInstance(
        'instance_cache_test_b.db',
      );
      const connection2 = await instance2.connect();
      try {
        await connection2.run(`create table mem1.main.t1 as select 1`);
        assert.fail('should throw');
      } catch (err) {
        assert.deepEqual(
          err,
          new Error(`Catalog Error: Catalog with name mem1 does not exist!`),
        );
      }
    } finally {
      if (instance1) {
        instance1.closeSync();
        fs.rmSync('instance_cache_test_a.db');
      }
      if (instance2) {
        instance2.closeSync();
        fs.rmSync('instance_cache_test_b.db');
      }
    }
  });
  test('instance cache - different config', async () => {
    const cache = new DuckDBInstanceCache();
    const instance1 = await cache.getOrCreateInstance(':memory:m1');
    const connection1 = await instance1.connect();
    await connection1.run(`create table t1 as select 1`);
    try {
      await cache.getOrCreateInstance(':memory:m1', {
        accces_mode: 'READ_ONLY',
      });
      assert.fail('should throw');
    } catch (err) {
      assert.deepEqual(
        err,
        new Error(
          `Connection Error: Can't open a connection to same database file with a different configuration than existing connections`,
        ),
      );
    }
  });
  test('instance cache - singleton', async () => {
    const instance = await DuckDBInstance.fromCache();
    const connection = await instance.connect();
    await connection.run('select 1');
  });
  test('create connection using instance cache', async () => {
    const connection = await DuckDBConnection.create();
    await connection.run('select 1');
  });
});
