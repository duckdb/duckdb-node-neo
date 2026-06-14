import { assert, beforeAll, describe, test } from 'vitest';
import {
  DuckDBIntegerVector,
  DuckDBValue,
  DuckDBVarCharVector,
  INTEGER,
  VARCHAR,
} from '../src';
import {
  createTestAllTypesColumnNameAndTypeObjectsJson,
  createTestAllTypesColumnNamesAndTypesJson,
  createTestAllTypesColumnsJson,
  createTestAllTypesColumnsObjectJson,
  createTestAllTypesRowObjectsJson,
  createTestAllTypesRowsJson,
} from './util/testAllTypes';
import {
  createTestJSColumnsJS,
  createTestJSColumnsObjectJS,
  createTestJSQuery,
  createTestJSRowObjectsJS,
  createTestJSRowsJS,
} from './util/testJS';
import {
  assertValues,
  setDefaultTimezone,
  withConnection,
} from './util/testHelpers';

describe('results', () => {
  beforeAll(setDefaultTimezone);

  test('result inspection conveniences', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(
        'select i::int as a, i::int + 10 as b from range(3) t(i)',
      );
      assert.deepEqual(result.columnNames(), ['a', 'b']);
      assert.deepEqual(result.columnTypes(), [INTEGER, INTEGER]);
      const chunks = await result.fetchAllChunks();
      const chunkColumns = chunks.map((chunk) => chunk.getColumns());
      assert.deepEqual(chunkColumns, [
        [
          [0, 1, 2],
          [10, 11, 12],
        ],
      ]);
      const chunkRows = chunks.map((chunk) => chunk.getRows());
      assert.deepEqual(chunkRows, [
        [
          [0, 10],
          [1, 11],
          [2, 12],
        ],
      ]);
    });
  });
  test('row and column objects', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        'select i::int as a, i::int + 10 as b, (i + 100)::varchar as a from range(3) t(i)',
      );
      assert.deepEqual(reader.columnNames(), ['a', 'b', 'a']);
      assert.deepEqual(reader.deduplicatedColumnNames(), ['a', 'b', 'a:1']);
      assert.deepEqual(reader.columnTypes(), [INTEGER, INTEGER, VARCHAR]);
      assert.deepEqual(reader.getRowObjects(), [
        { 'a': 0, 'b': 10, 'a:1': '100' },
        { 'a': 1, 'b': 11, 'a:1': '101' },
        { 'a': 2, 'b': 12, 'a:1': '102' },
      ]);
      assert.deepEqual(reader.getColumnsObject(), {
        'a': [0, 1, 2],
        'b': [10, 11, 12],
        'a:1': ['100', '101', '102'],
      });
    });
  });
  test('columns js', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(createTestJSQuery());
      const columnsJS = reader.getColumnsJS();
      assert.deepEqual(columnsJS, createTestJSColumnsJS());
    });
  });
  test('columns object js', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(createTestJSQuery());
      const columnsJS = reader.getColumnsObjectJS();
      assert.deepEqual(columnsJS, createTestJSColumnsObjectJS());
    });
  });
  test('rows js', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(createTestJSQuery());
      const columnsJS = reader.getRowsJS();
      assert.deepEqual(columnsJS, createTestJSRowsJS());
    });
  });
  test('row objects js', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(createTestJSQuery());
      const columnsJS = reader.getRowObjectsJS();
      assert.deepEqual(columnsJS, createTestJSRowObjectsJS());
    });
  });
  test('columns json', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `from test_all_types()`,
      );
      const columnsJson = reader.getColumnsJson();
      assert.deepEqual(columnsJson, createTestAllTypesColumnsJson());
    });
  });
  test('columns object json', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `from test_all_types()`,
      );
      const columnsJson = reader.getColumnsObjectJson();
      assert.deepEqual(columnsJson, createTestAllTypesColumnsObjectJson());
    });
  });
  test('rows json', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `from test_all_types()`,
      );
      const rowsJson = reader.getRowsJson();
      assert.deepEqual(rowsJson, createTestAllTypesRowsJson());
    });
  });
  test('row objects json', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `from test_all_types()`,
      );
      const rowObjectsJson = reader.getRowObjectsJson();
      assert.deepEqual(rowObjectsJson, createTestAllTypesRowObjectsJson());
    });
  });
  test('column names and types json', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `from test_all_types(use_large_enum=true)`,
      );
      const columnNamesAndTypesJson = reader.columnNamesAndTypesJson();
      assert.deepEqual(
        columnNamesAndTypesJson,
        createTestAllTypesColumnNamesAndTypesJson(),
      );
    });
  });
  test('column name and type objects json', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `from test_all_types(use_large_enum=true)`,
      );
      const columnNameAndTypeObjectsJson =
        reader.columnNameAndTypeObjectsJson();
      assert.deepEqual(
        columnNameAndTypeObjectsJson,
        createTestAllTypesColumnNameAndTypeObjectsJson(),
      );
    });
  });
  test('result reader', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        'select i::int as a, i::int + 10000 as b from range(5000) t(i)',
      );
      assert.deepEqual(reader.columnNames(), ['a', 'b']);
      assert.deepEqual(reader.columnTypes(), [INTEGER, INTEGER]);
      const columns = reader.getColumns();
      assert.equal(columns.length, 2);
      assert.equal(columns[0][0], 0);
      assert.equal(columns[0][4999], 4999);
      assert.equal(columns[1][0], 10000);
      assert.equal(columns[1][4999], 14999);
      const rows = reader.getRows();
      assert.equal(rows.length, 5000);
      assert.deepEqual(rows[0], [0, 10000]);
      assert.deepEqual(rows[4999], [4999, 14999]);
    });
  });

  test('iterate over DuckDBResult stream in chunks', async () => {
    await withConnection(async (connection) => {
      const result = await connection.stream(
        'select i::int, i::int + 10, (i + 100)::varchar from range(3) t(i)',
      );

      for await (const chunk of result) {
        assert.strictEqual(chunk.rowCount, 3);
        let i = 0;
        assertValues<number, DuckDBIntegerVector>(
          chunk,
          i++,
          DuckDBIntegerVector,
          [0, 1, 2],
        );
        assertValues<number, DuckDBIntegerVector>(
          chunk,
          i++,
          DuckDBIntegerVector,
          [10, 11, 12],
        );
        assertValues<string, DuckDBVarCharVector>(
          chunk,
          i++,
          DuckDBVarCharVector,
          ['100', '101', '102'],
        );
      }
    });
  });

  test('iterate over many DuckDBResult chunks', async () => {
    await withConnection(async (connection) => {
      const chunkSize = 2048;
      const totalExpectedCount = chunkSize * 3;
      const result = await connection.stream(
        `select i::int from range(${totalExpectedCount}) t(i)`,
      );

      let total = 0;
      for await (const chunk of result) {
        assert.equal(chunk.rowCount, chunkSize);
        total += chunk.rowCount;
      }

      assert.equal(total, totalExpectedCount);
    });
  });

  test('iterate stream of rows', async () => {
    await withConnection(async (connection) => {
      const result = await connection.stream(
        'select i::int, i::int + 10, (i + 100)::varchar from range(3) t(i)',
      );

      const expectedRows: DuckDBValue[][] = [
        [0, 10, '100'],
        [1, 11, '101'],
        [2, 12, '102'],
      ];

      for await (const rows of result.yieldRows()) {
        for (let i = 0; i < rows.length; i++) {
          assert.deepEqual(rows[i], expectedRows[i]);
        }
      }
    });
  });

  test('iterate stream of row objects', async () => {
    await withConnection(async (connection) => {
      const result = await connection.stream(
        'select i::int as a, i::int + 10 as b, (i + 100)::varchar as c from range(3) t(i)',
      );

      const expectedRows: Record<string, DuckDBValue>[] = [
        { a: 0, b: 10, c: '100' },
        { a: 1, b: 11, c: '101' },
        { a: 2, b: 12, c: '102' },
      ];

      for await (const rows of result.yieldRowObjects()) {
        for (let i = 0; i < rows.length; i++) {
          assert.deepEqual(rows[i], expectedRows[i]);
        }
      }
    });
  });

  test('iterate result stream rows js', async () => {
    await withConnection(async (connection) => {
      const result = await connection.stream(createTestJSQuery());
      for await (const row of result.yieldRowsJs()) {
        assert.deepEqual(row, createTestJSRowsJS());
      }
    });
  });

  test('iterate result stream object js', async () => {
    await withConnection(async (connection) => {
      const result = await connection.stream(createTestJSQuery());
      for await (const row of result.yieldRowObjectJs()) {
        assert.deepEqual(row, createTestJSRowObjectsJS());
      }
    });
  });

  test('iterate result stream rows json', async () => {
    await withConnection(async (connection) => {
      const result = await connection.stream(
        `from test_all_types()`,
      );
      for await (const row of result.yieldRowsJson()) {
        assert.deepEqual(row, createTestAllTypesRowsJson());
      }
    });
  });

  test('iterate result stream object json', async () => {
    await withConnection(async (connection) => {
      const result = await connection.stream(
        `from test_all_types()`,
      );
      for await (const row of result.yieldRowObjectJson()) {
        assert.deepEqual(row, createTestAllTypesRowObjectsJson());
      }
    });
  });
});
