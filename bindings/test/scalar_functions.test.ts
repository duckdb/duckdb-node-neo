import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { data } from './utils/expectedVectors';
import { expectResult } from './utils/expectResult';
import { withConnection } from './utils/withConnection';

suite('scalar functions', () => {
  test('create', () => {
    const scalar_function = duckdb.create_scalar_function();
    expect(scalar_function).toBeTruthy();
  });
  test('set name', () => {
    const scalar_function = duckdb.create_scalar_function();
    duckdb.scalar_function_set_name(scalar_function, 'my_func');
  });
  test('set return type', () => {
    const scalar_function = duckdb.create_scalar_function();
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    duckdb.scalar_function_set_return_type(scalar_function, int_type);
  });
  test('register & run (no extra info)', async () => {
    await withConnection(async (connection) => {
      const scalar_function = duckdb.create_scalar_function();
      duckdb.scalar_function_set_name(scalar_function, 'my_func');
      const int_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
      duckdb.scalar_function_set_return_type(scalar_function, int_type);
      duckdb.scalar_function_set_function(scalar_function, (_info, input, output) => {
        const inputSize = duckdb.data_chunk_get_size(input);
        for (let i = 0; i < inputSize; i++) {
          duckdb.vector_assign_string_element(output, i, `output_${i}`);
        }
      });
      duckdb.register_scalar_function(connection, scalar_function);
      duckdb.destroy_scalar_function_sync(scalar_function);

      const result = await duckdb.query(connection, "select my_func()");
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 1,
        columns: [
          { name: 'my_func()', logicalType: { typeId: duckdb.Type.VARCHAR } }
        ],
        chunks: [
          { rowCount: 1, vectors: [data(16, [true], ['output_0'])]}
        ],
      });
    });
  });
  test('register & run (extra info)', async () => {
    await withConnection(async (connection) => {
      const scalar_function = duckdb.create_scalar_function();
      duckdb.scalar_function_set_name(scalar_function, 'my_func');
      const int_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
      duckdb.scalar_function_set_return_type(scalar_function, int_type);
      duckdb.scalar_function_set_function(scalar_function, (info, input, output) => {
        const extra_info = duckdb.scalar_function_get_extra_info(info);
        const inputSize = duckdb.data_chunk_get_size(input);
        for (let i = 0; i < inputSize; i++) {
          duckdb.vector_assign_string_element(output, i, `output_${i}_${JSON.stringify(extra_info)}`);
        }
      }, { 'my_extra_info_key': 'my_extra_info_value' });
      duckdb.register_scalar_function(connection, scalar_function);
      duckdb.destroy_scalar_function_sync(scalar_function);

      const result = await duckdb.query(connection, "select my_func()");
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 1,
        columns: [
          { name: 'my_func()', logicalType: { typeId: duckdb.Type.VARCHAR } }
        ],
        chunks: [
          { rowCount: 1, vectors: [data(16, [true], ['output_0_{"my_extra_info_key":"my_extra_info_value"}'])]}
        ],
      });
    });
  });
  test('error handling', async () => {
    await withConnection(async (connection) => {
      const scalar_function = duckdb.create_scalar_function();
      duckdb.scalar_function_set_name(scalar_function, 'my_func');
      const int_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
      duckdb.scalar_function_set_return_type(scalar_function, int_type);
      duckdb.scalar_function_set_function(scalar_function, (_info, _input, _output) => {
        throw new Error('my_error');
      });
      duckdb.register_scalar_function(connection, scalar_function);
      duckdb.destroy_scalar_function_sync(scalar_function);

      await expect(duckdb.query(connection, "select my_func()")).rejects.toThrow('Invalid Input Error: my_error');;
    });
  });
});
