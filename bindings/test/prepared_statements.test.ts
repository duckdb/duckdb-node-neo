import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { data } from './utils/expectedVectors';
import { expectResult } from './utils/expectResult';
import { withConnection } from './utils/withConnection';

suite('prepared statements', () => {
  test('no parameters', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select 17 as seventeen');
      try {
        expect(duckdb.nparams(prepared)).toBe(0);
        expect(duckdb.prepared_statement_type(prepared)).toBe(duckdb.StatementType.SELECT);
        const result = await duckdb.execute_prepared(prepared);
        try {
          await expectResult(result, {
            columns: [
              { name: 'seventeen', logicalType: { typeId: duckdb.Type.INTEGER } },
            ],
            chunks: [
              { rowCount: 1, vectors: [data(4, [true], [17])]},
            ],
          });
        } finally {
          duckdb.destroy_result(result);
        }
      } finally {
        duckdb.destroy_prepare(prepared);
      }
    });
  });
});
