import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { expectResult } from './utils/expectResult';
import { withConnection } from './utils/withConnection';

suite('prepared statements', () => {
  test('no parameters', async () => {
    await withConnection(async (con) => {
      const prepared = await duckdb.prepare(con, 'select 17 as seventeen');
      try {
        expect(duckdb.nparams(prepared)).toBe(0);
        expect(duckdb.prepared_statement_type(prepared)).toBe(duckdb.StatementType.SELECT);
        const res = await duckdb.execute_prepared(prepared);
        try {
          await expectResult(res, {
            columns: [
              { name: 'seventeen', type: duckdb.Type.INTEGER },
            ],
            chunks: [
              { rowCount: 1, vectors: [{ byteCount: 4, validity: [true], values: [17] }]},
            ],
          });
        } finally {
          duckdb.destroy_result(res);
        }
      } finally {
        duckdb.destroy_prepare(prepared);
      }
    });
  });
});
