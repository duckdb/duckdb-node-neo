import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { withConnection } from './utils/withConnection';

suite('connection', () => {
  test('disconnect', async () => {
    await withConnection(async (connection) => {
      const prepared1 = await duckdb.prepare(connection, 'select 1');
      expect(prepared1).toBeDefined();
      const { extracted_statements } = await duckdb.extract_statements(connection, 'select 10; select 20');
      duckdb.disconnect_sync(connection);
      await expect(async () => await duckdb.prepare(connection, 'select 2'))
        .rejects.toStrictEqual(new Error('Failed to prepare: connection disconnected'));
      await expect(async () => await duckdb.query(connection, 'select 3'))
        .rejects.toStrictEqual(new Error('Failed to query: connection disconnected'));
      await expect(async () => await duckdb.extract_statements(connection, 'select 4; select 5'))
        .rejects.toStrictEqual(new Error('Failed to extract statements: connection disconnected'));
      await expect(async () => await duckdb.prepare_extracted_statement(connection, extracted_statements, 0))
        .rejects.toStrictEqual(new Error('Failed to prepare extracted statement: connection disconnected'));
      await expect(async () => await duckdb.appender_create(connection, 'main', 'my_target'))
        .rejects.toStrictEqual(new Error('Failed to create appender: connection disconnected'));
    });
  });
  test('double disconnect', async () => {
    await withConnection(async (connection) => {
      const prepared1 = await duckdb.prepare(connection, 'select 1');
      expect(prepared1).toBeDefined();
      duckdb.disconnect_sync(connection);
      // ensure a second disconnect is a no-op
      duckdb.disconnect_sync(connection);
      await expect(async () => await duckdb.prepare(connection, 'select 3'))
        .rejects.toStrictEqual(new Error('Failed to prepare: connection disconnected'));
    });
  });
});
