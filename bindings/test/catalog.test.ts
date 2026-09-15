import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { data } from './utils/expectedVectors';
import { expectResult } from './utils/expectResult';
import { withConnection } from './utils/withConnection';

suite('catalog', () => {
  test('get backend type name during a transaction', async () => {
    await withConnection(async (connection) => {
      const context = duckdb.connection_get_client_context(connection);
      await duckdb.query(connection, 'BEGIN TRANSACTION');
      try {
        const catalog = duckdb.client_context_get_catalog(context, 'memory');
        expect(catalog).not.toBeNull();
        expect(duckdb.catalog_get_type_name(catalog!)).toBe('duckdb');
      } finally {
        await duckdb.query(connection, 'ROLLBACK');
      }
    });
  });

  test('return null outside a transaction', async () => {
    await withConnection(async (connection) => {
      const context = duckdb.connection_get_client_context(connection);
      expect(duckdb.client_context_get_catalog(context, 'memory')).toBeNull();
    });
  });

  test.each(['missing_catalog', ''])('return null for name %j', async (name) => {
    await withConnection(async (connection) => {
      const context = duckdb.connection_get_client_context(connection);
      await duckdb.query(connection, 'BEGIN TRANSACTION');
      try {
        expect(duckdb.client_context_get_catalog(context, name)).toBeNull();
      } finally {
        await duckdb.query(connection, 'ROLLBACK');
      }
    });
  });

  test('look up an attached catalog by alias', async () => {
    await withConnection(async (connection) => {
      await duckdb.query(connection, "ATTACH ':memory:' AS attached_catalog");
      const context = duckdb.connection_get_client_context(connection);
      await duckdb.query(connection, 'BEGIN TRANSACTION');
      try {
        const catalog = duckdb.client_context_get_catalog(context, 'attached_catalog');
        expect(catalog).not.toBeNull();
        expect(duckdb.catalog_get_type_name(catalog!)).toBe('duckdb');
      } finally {
        await duckdb.query(connection, 'ROLLBACK');
      }
    });
  });

  test('look up synchronously in a scalar bind callback', async () => {
    await withConnection(async (connection) => {
      const scalar_function = duckdb.create_scalar_function();
      duckdb.scalar_function_set_name(scalar_function, 'catalog_type');
      duckdb.scalar_function_set_return_type(
        scalar_function, duckdb.create_logical_type(duckdb.Type.VARCHAR),
      );
      duckdb.scalar_function_set_bind(scalar_function, (info) => {
        const context = duckdb.scalar_function_get_client_context(info);
        const catalog = duckdb.client_context_get_catalog(context, 'memory');
        duckdb.scalar_function_set_bind_data(info, {
          type_name: duckdb.catalog_get_type_name(catalog!),
        });
      });
      duckdb.scalar_function_set_function(scalar_function, (info, input, output) => {
        const { type_name } = duckdb.scalar_function_get_bind_data(info) as {
          type_name: string;
        };
        for (let row = 0; row < duckdb.data_chunk_get_size(input); row++) {
          duckdb.vector_assign_string_element(output, row, type_name);
        }
      });
      try {
        duckdb.register_scalar_function(connection, scalar_function);
      } finally {
        duckdb.destroy_scalar_function_sync(scalar_function);
      }
      const result = await duckdb.query(connection, 'SELECT catalog_type()');
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 1,
        columns: [{ name: 'catalog_type()', logicalType: { typeId: duckdb.Type.VARCHAR } }],
        chunks: [{ rowCount: 1, vectors: [data(16, [true], ['duckdb'])] }],
      });
    });
  });

  test('keep copied names after discarding handles and closing the database', async () => {
    const names: string[] = [];
    await withConnection(async (connection) => {
      const context = duckdb.connection_get_client_context(connection);
      await duckdb.query(connection, 'BEGIN TRANSACTION');
      try {
        for (let i = 0; i < 10; i++) {
          const catalog = duckdb.client_context_get_catalog(context, 'memory');
          names.push(duckdb.catalog_get_type_name(catalog!));
        }
      } finally {
        await duckdb.query(connection, 'ROLLBACK');
      }
    });
    expect(names).toEqual(Array(10).fill('duckdb'));
  });
});
