import { assert, beforeAll, describe, test } from 'vitest';
import {
  DuckDBInstance,
  DuckDBVarCharVector,
  VARCHAR,
} from '../src';
import {
  assertColumns,
  assertValues,
  setDefaultTimezone,
} from './util/testHelpers';

describe('config', () => {
  beforeAll(setDefaultTimezone);

  test('default duckdb_api without explicit options', async () => {
    const instance = await DuckDBInstance.create();
    const connection = await instance.connect();
    const result = await connection.run(
      `select current_setting('duckdb_api') as duckdb_api`,
    );
    assertColumns(result, [{ name: 'duckdb_api', type: VARCHAR }]);
    const chunk = await result.fetchChunk();
    assert.isDefined(chunk);
    if (chunk) {
      assert.strictEqual(chunk.columnCount, 1);
      assert.strictEqual(chunk.rowCount, 1);
      assertValues<string, DuckDBVarCharVector>(chunk, 0, DuckDBVarCharVector, [
        'node-neo-api',
      ]);
    }
  });
  test('default duckdb_api with explicit options', async () => {
    const instance = await DuckDBInstance.create(undefined, {});
    const connection = await instance.connect();
    const result = await connection.run(
      `select current_setting('duckdb_api') as duckdb_api`,
    );
    assertColumns(result, [{ name: 'duckdb_api', type: VARCHAR }]);
    const chunk = await result.fetchChunk();
    assert.isDefined(chunk);
    if (chunk) {
      assert.strictEqual(chunk.columnCount, 1);
      assert.strictEqual(chunk.rowCount, 1);
      assertValues<string, DuckDBVarCharVector>(chunk, 0, DuckDBVarCharVector, [
        'node-neo-api',
      ]);
    }
  });
  test('overriding duckdb_api', async () => {
    const instance = await DuckDBInstance.create(undefined, {
      'duckdb_api': 'custom-duckdb-api',
    });
    const connection = await instance.connect();
    const result = await connection.run(
      `select current_setting('duckdb_api') as duckdb_api`,
    );
    assertColumns(result, [{ name: 'duckdb_api', type: VARCHAR }]);
    const chunk = await result.fetchChunk();
    assert.isDefined(chunk);
    if (chunk) {
      assert.strictEqual(chunk.columnCount, 1);
      assert.strictEqual(chunk.rowCount, 1);
      assertValues<string, DuckDBVarCharVector>(chunk, 0, DuckDBVarCharVector, [
        'custom-duckdb-api',
      ]);
    }
  });
});
