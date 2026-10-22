import { describe, expect, test } from 'vitest';
import {
  CatalogEntryType,
  DuckDBCatalog,
  DuckDBCatalogEntry,
  DuckDBInstance,
  DuckDBScalarFunction,
  VARCHAR,
} from '../src';
import { withConnection } from './util/testHelpers';

describe('catalog', () => {
  test('look up qualified entries and preserve their actual names and types', async () => {
    await withConnection(async (connection) => {
      try {
        await connection.run("ATTACH ':memory:' AS attached_catalog");
        await connection.run('CREATE SCHEMA attached_catalog.analytics');
        await connection.run('CREATE VIEW attached_catalog.analytics."Sales_🦆" AS SELECT 42 AS i');
        await connection.run('BEGIN TRANSACTION');
        try {
          const catalog = connection.getClientContext().getCatalog('attached_catalog');
          expect(catalog).toBeInstanceOf(DuckDBCatalog);
          expect(catalog!.typeName).toBe('duckdb');
          const entry = catalog!.getEntry(CatalogEntryType.TABLE, 'analytics', 'sales_🦆');
          expect(entry).toBeInstanceOf(DuckDBCatalogEntry);
          expect(entry!.name).toBe('Sales_🦆');
          const type: CatalogEntryType = entry!.type;
          expect(type).toBe(CatalogEntryType.VIEW);
          expect(CatalogEntryType[type]).toBe('VIEW');
        } finally {
          await connection.run('ROLLBACK');
        }
      } finally {
        connection.disconnectSync();
      }
    });
  });

  test('propagate missing results and lookup errors', async () => {
    await withConnection(async (connection) => {
      try {
        const context = connection.clientContext;
        expect(context.getCatalog('memory')).toBeNull();
        await connection.run('BEGIN TRANSACTION');
        try {
          expect(context.getCatalog('missing_catalog')).toBeNull();
          expect(context.getCatalog('')).toBeNull();
          const catalog = context.getCatalog('memory')!;
          expect(catalog.getEntry(CatalogEntryType.TABLE, 'missing_schema', 'missing_entry')).toBeNull();
          expect(catalog.getEntry(CatalogEntryType.TABLE, 'main', 'missing_entry')).toBeNull();
          expect(() => catalog.getEntry(CatalogEntryType.SCHEMA, 'main', 'main'))
            .toThrowError('Catalog entry type SCHEMA is not supported by duckdb catalog lookups');
          expect(() => catalog.getEntry(CatalogEntryType.TABLE, 'main', 'name\0suffix'))
            .toThrowError('Catalog entry names must not contain null bytes');
        } finally {
          await connection.run('ROLLBACK');
        }
      } finally {
        connection.disconnectSync();
      }
    });
  });

  test('look up synchronously in a registered scalar function callback', async () => {
    await withConnection(async (connection) => {
      try {
        await connection.run('CREATE TABLE callback_table(i INTEGER)');
        connection.registerScalarFunction(DuckDBScalarFunction.create({
          name: 'entry_metadata',
          bindFunction: (info) => {
            const catalog = info.clientContext.getCatalog('memory')!;
            const entry = catalog.getEntry(CatalogEntryType.TABLE, 'main', 'callback_table')!;
            info.setBindData(entry);
          },
          mainFunction: (info, input, output) => {
            const entry = info.bindData as DuckDBCatalogEntry;
            for (let row = 0; row < input.rowCount; row++) {
              output.setItem(row, `${entry.name}:${CatalogEntryType[entry.type]}`);
            }
            output.flush();
          },
          returnType: VARCHAR,
        }));
        const result = await connection.runAndReadAll('SELECT entry_metadata()');
        expect(result.getRows()).toEqual([['callback_table:TABLE']]);
      } finally {
        connection.disconnectSync();
      }
    });
  });

  test.each(['COMMIT', 'ROLLBACK', 'disconnect'] as const)(
    'own entry metadata before its first read after %s', async (boundary) => {
      const instance = await DuckDBInstance.create();
      const connection = await instance.connect();
      try {
        await connection.run('BEGIN TRANSACTION');
        await connection.run('CREATE VIEW "Transient_🦆" AS SELECT 42 AS i');
        const entry = connection.clientContext.getCatalog('memory')!
          .getEntry(CatalogEntryType.TABLE, 'main', 'transient_🦆')!;
        expect(entry).toBeInstanceOf(DuckDBCatalogEntry);

        if (boundary === 'disconnect') {
          connection.disconnectSync();
        } else {
          await connection.run(boundary);
        }

        // Own data properties cannot invoke native accessors after the boundary.
        expect(Object.getOwnPropertyDescriptor(entry, 'name'))
          .toHaveProperty('value', 'Transient_🦆');
        expect(Object.getOwnPropertyDescriptor(entry, 'type'))
          .toHaveProperty('value', CatalogEntryType.VIEW);
        expect(entry.name).toBe('Transient_🦆');
        expect(entry.type).toBe(CatalogEntryType.VIEW);

        const verification = await instance.connect();
        try {
          await verification.run('BEGIN TRANSACTION');
          try {
            const current = verification.clientContext.getCatalog('memory')!
              .getEntry(CatalogEntryType.TABLE, 'main', 'transient_🦆');
            if (boundary === 'COMMIT') {
              expect(current!.name).toBe('Transient_🦆');
            } else {
              expect(current).toBeNull();
            }
          } finally {
            await verification.run('ROLLBACK');
          }
        } finally {
          verification.disconnectSync();
        }
      } finally {
        connection.disconnectSync();
        instance.closeSync();
      }
    }
  );
});
