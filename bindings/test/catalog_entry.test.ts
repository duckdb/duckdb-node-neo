import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { data } from './utils/expectedVectors';
import { expectResult } from './utils/expectResult';
import { withConnection } from './utils/withConnection';

async function withCatalog(
  fn: (catalog: duckdb.Catalog, context: duckdb.ClientContext) => void,
) {
  await withConnection(async (connection) => {
    await duckdb.query(connection, 'BEGIN TRANSACTION');
    try {
      await duckdb.query(connection, 'CREATE TABLE test_table(i INTEGER)');
      await duckdb.query(connection, 'CREATE VIEW test_view AS SELECT * FROM test_table');
      const context = duckdb.connection_get_client_context(connection);
      const catalog = duckdb.client_context_get_catalog(context, 'memory');
      expect(catalog).not.toBeNull();
      fn(catalog!, context);
    } finally {
      await duckdb.query(connection, 'ROLLBACK');
    }
  });
}

suite('catalog entry', () => {
  test('read actual names and types in the shared table/view namespace during a transaction', async () => {
    await withCatalog((catalog, context) => {
      for (const requestedType of [duckdb.CatalogEntryType.TABLE, duckdb.CatalogEntryType.VIEW]) {
        for (const [name, actualType] of [
          ['test_table', duckdb.CatalogEntryType.TABLE],
          ['test_view', duckdb.CatalogEntryType.VIEW],
        ] as const) {
          const entry = duckdb.catalog_get_entry(catalog, context, requestedType, 'main', name.toUpperCase());
          expect(entry).not.toBeNull();
          expect(duckdb.catalog_entry_get_name(entry!)).toBe(name);
          expect(duckdb.catalog_entry_get_type(entry!)).toBe(actualType);
        }
      }
    });
  });

  test('qualify entries by catalog and non-default schema', async () => {
    await withConnection(async (connection) => {
      await duckdb.query(connection, "ATTACH ':memory:' AS attached_catalog");
      await duckdb.query(connection, 'CREATE SCHEMA memory.analytics');
      await duckdb.query(connection, 'CREATE TABLE memory.analytics."Sales_🦆"(i INTEGER)');
      await duckdb.query(connection, 'CREATE SCHEMA attached_catalog.analytics');
      await duckdb.query(connection, 'CREATE VIEW attached_catalog.analytics."Sales_🦆" AS SELECT 42 AS i');
      const context = duckdb.connection_get_client_context(connection);
      await duckdb.query(connection, 'BEGIN TRANSACTION');
      try {
        for (const [catalogName, actualType] of [
          ['memory', duckdb.CatalogEntryType.TABLE],
          ['attached_catalog', duckdb.CatalogEntryType.VIEW],
        ] as const) {
          const catalog = duckdb.client_context_get_catalog(context, catalogName)!;
          const entry = duckdb.catalog_get_entry(catalog, context, duckdb.CatalogEntryType.TABLE, 'analytics', 'sales_🦆');
          expect(entry).not.toBeNull();
          expect(duckdb.catalog_entry_get_name(entry!)).toBe('Sales_🦆');
          expect(duckdb.catalog_entry_get_type(entry!)).toBe(actualType);
          expect(duckdb.catalog_get_entry(catalog, context, duckdb.CatalogEntryType.TABLE, 'main', 'sales_🦆')).toBeNull();
          expect(duckdb.catalog_get_entry(catalog, context, duckdb.CatalogEntryType.TABLE, 'sales_🦆', 'analytics')).toBeNull();
        }
      } finally {
        await duckdb.query(connection, 'ROLLBACK');
      }
    });
  });

  test.each([
    duckdb.CatalogEntryType.TABLE,
    duckdb.CatalogEntryType.VIEW,
    duckdb.CatalogEntryType.INDEX,
    duckdb.CatalogEntryType.SEQUENCE,
    duckdb.CatalogEntryType.COLLATION,
    duckdb.CatalogEntryType.TYPE,
  ])('return null for missing schemas and entries of supported category %i', async (entryType) => {
    await withCatalog((catalog, context) => {
      expect(duckdb.catalog_get_entry(catalog, context, entryType, 'missing_schema', 'test_table')).toBeNull();
      expect(duckdb.catalog_get_entry(catalog, context, entryType, 'main', 'missing_entry')).toBeNull();
      expect(duckdb.catalog_get_entry(catalog, context, entryType, 'main', '')).toBeNull();
    });
  });

  test.each([
    ['INVALID', duckdb.CatalogEntryType.INVALID],
    ['SCHEMA', duckdb.CatalogEntryType.SCHEMA],
    ['PREPARED_STATEMENT', duckdb.CatalogEntryType.PREPARED_STATEMENT],
    ['DATABASE', duckdb.CatalogEntryType.DATABASE],
  ] as const)('describe unsupported lookup category %s in a DuckDB catalog', async (entryTypeName, entryType) => {
    await withCatalog((catalog, context) => {
      expect(() => duckdb.catalog_get_entry(catalog, context, entryType, 'main', 'test_table'))
        .toThrowError(new Error(`Catalog entry type ${entryTypeName} is not supported by duckdb catalog lookups`));
    });
  });

  test('reject non-integer and out-of-range catalog entry types', async () => {
    await withCatalog((catalog, context) => {
      for (const entryType of [
        -1, 10, 1.5, NaN, Infinity, -Infinity, 2 ** 32 + 1,
      ]) {
        expect(() => duckdb.catalog_get_entry(
          catalog, context, entryType, 'main', 'test_table',
        )).toThrowError();
      }
    });
  });

  test('reject null bytes instead of looking up a truncated name', async () => {
    await withCatalog((catalog, context) => {
      for (const [schemaName, entryName] of [
        ['main\0suffix', 'test_table'],
        ['main', 'test_table\0suffix'],
      ]) {
        expect(() => duckdb.catalog_get_entry(
          catalog, context, duckdb.CatalogEntryType.TABLE, schemaName, entryName,
        )).toThrowError('Catalog entry names must not contain null bytes');
      }
    });
  });

  test('copy metadata synchronously in a scalar bind callback before its transaction ends', async () => {
    await withConnection(async (connection) => {
      await duckdb.query(connection, 'CREATE TABLE callback_table(i INTEGER)');
      const scalarFunction = duckdb.create_scalar_function();
      duckdb.scalar_function_set_name(scalarFunction, 'entry_metadata');
      duckdb.scalar_function_set_return_type(scalarFunction, duckdb.create_logical_type(duckdb.Type.VARCHAR));
      duckdb.scalar_function_set_bind(scalarFunction, (info) => {
        const context = duckdb.scalar_function_get_client_context(info);
        const catalog = duckdb.client_context_get_catalog(context, 'memory')!;
        const entry = duckdb.catalog_get_entry(catalog, context, duckdb.CatalogEntryType.TABLE, 'main', 'callback_table')!;
        duckdb.scalar_function_set_bind_data(info, {
          name: duckdb.catalog_entry_get_name(entry),
          type: duckdb.catalog_entry_get_type(entry),
        });
      });
      duckdb.scalar_function_set_function(scalarFunction, (info, input, output) => {
        const metadata = duckdb.scalar_function_get_bind_data(info) as {
          name: string; type: duckdb.CatalogEntryType;
        };
        for (let row = 0; row < duckdb.data_chunk_get_size(input); row++) {
          duckdb.vector_assign_string_element(output, row, `${metadata.name}:${metadata.type}`);
        }
      });
      try {
        duckdb.register_scalar_function(connection, scalarFunction);
      } finally {
        duckdb.destroy_scalar_function_sync(scalarFunction);
      }
      const result = await duckdb.query(connection, 'SELECT entry_metadata()');
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 1,
        columns: [{ name: 'entry_metadata()', logicalType: { typeId: duckdb.Type.VARCHAR } }],
        chunks: [{ rowCount: 1, vectors: [data(16, [true], ['callback_table:1'])] }],
      });
    });
  });

  test('keep copied metadata after discarding entries, rolling back, and closing the database', async () => {
    const metadata: { name: string; type: duckdb.CatalogEntryType }[] = [];
    await withCatalog((catalog, context) => {
      for (let i = 0; i < 10; i++) {
        const entry = duckdb.catalog_get_entry(catalog, context, duckdb.CatalogEntryType.TABLE, 'main', 'test_table')!;
        metadata.push({ name: duckdb.catalog_entry_get_name(entry), type: duckdb.catalog_entry_get_type(entry) });
      }
    });
    expect(metadata).toEqual(Array(10).fill({ name: 'test_table', type: duckdb.CatalogEntryType.TABLE }));
  });
});
