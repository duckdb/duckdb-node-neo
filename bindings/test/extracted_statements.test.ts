import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { expectResult } from './utils/expectResult';
import { INTEGER } from './utils/expectedLogicalTypes';
import { data } from './utils/expectedVectors';
import { withConnection } from './utils/withConnection';

suite('extracted statements', () => {
  test('no statements', async () => {
    await withConnection(async (connection) => {
      const { statement_count } = await duckdb.extract_statements(connection, '');
      expect(statement_count).toBe(0);
    });
  });
  test('error', async () => {
    await withConnection(async (connection) => {
      const { extracted_statements, statement_count } = await duckdb.extract_statements(connection, 'x');
      expect(statement_count).toBe(0);
      expect(duckdb.extract_statements_error(extracted_statements)).toBe('Parser Error: syntax error at or near "x"');
    });
  });
  test('one statement', async () => {
    await withConnection(async (connection) => {
      const { extracted_statements, statement_count } = await duckdb.extract_statements(connection, 'select 11 as a');
      expect(statement_count).toBe(1);
      const prepared = await duckdb.prepare_extracted_statement(connection, extracted_statements, 0);
      const result = await duckdb.execute_prepared(prepared);
      await expectResult(result, {
        columns: [
          { name: 'a', logicalType: INTEGER },
        ],
        chunks: [
          { rowCount: 1, vectors: [data(4, [true], [11])]},
        ],
      });
    });
  });
  test('multiple statements', async () => {
    await withConnection(async (connection) => {
      const { extracted_statements, statement_count } = await duckdb.extract_statements(connection,
        'select 11 as a; select 22 as b; select 33 as c'
      );
      expect(statement_count).toBe(3);

      const prepared0 = await duckdb.prepare_extracted_statement(connection, extracted_statements, 0);
      const result0 = await duckdb.execute_prepared(prepared0);
      await expectResult(result0, {
        columns: [
          { name: 'a', logicalType: INTEGER },
        ],
        chunks: [
          { rowCount: 1, vectors: [data(4, [true], [11])]},
        ],
      });

      const prepared1 = await duckdb.prepare_extracted_statement(connection, extracted_statements, 1);
      const result1 = await duckdb.execute_prepared(prepared1);
      await expectResult(result1, {
        columns: [
          { name: 'b', logicalType: INTEGER },
        ],
        chunks: [
          { rowCount: 1, vectors: [data(4, [true], [22])]},
        ],
      });

      const prepared2 = await duckdb.prepare_extracted_statement(connection, extracted_statements, 2);
      const result2 = await duckdb.execute_prepared(prepared2);
      await expectResult(result2, {
        columns: [
          { name: 'c', logicalType: INTEGER },
        ],
        chunks: [
          { rowCount: 1, vectors: [data(4, [true], [33])]},
        ],
      });
    });
  });
});
