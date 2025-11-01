import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { withConnection } from './utils/withConnection';

suite('client_context', () => {
  test('connection_id', async () => {
    await withConnection(async (connection) => {
      const client_context = duckdb.connection_get_client_context(connection);
      expect(client_context).toBeDefined();
      const connection_id = duckdb.client_context_get_connection_id(client_context);
      expect(connection_id).toBeDefined();
    });
  });
});
