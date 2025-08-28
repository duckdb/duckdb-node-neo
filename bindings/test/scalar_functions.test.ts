import duckdb from '@databrainhq/node-bindings';
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
      const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
      duckdb.scalar_function_set_return_type(scalar_function, varchar_type);
      duckdb.scalar_function_set_function(
        scalar_function,
        (_info, input, output) => {
          const rowCount = duckdb.data_chunk_get_size(input);
          for (let i = 0; i < rowCount; i++) {
            duckdb.vector_assign_string_element(output, i, `output_${i}`);
          }
        },
      );
      duckdb.register_scalar_function(connection, scalar_function);
      duckdb.destroy_scalar_function_sync(scalar_function);

      const result = await duckdb.query(connection, 'select my_func()');
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 1,
        columns: [
          { name: 'my_func()', logicalType: { typeId: duckdb.Type.VARCHAR } },
        ],
        chunks: [{ rowCount: 1, vectors: [data(16, [true], ['output_0'])] }],
      });
    });
  });
  test('register & run (extra info)', async () => {
    await withConnection(async (connection) => {
      const scalar_function = duckdb.create_scalar_function();
      duckdb.scalar_function_set_name(scalar_function, 'my_func');
      const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
      duckdb.scalar_function_set_return_type(scalar_function, varchar_type);
      duckdb.scalar_function_set_function(
        scalar_function,
        (info, input, output) => {
          const extra_info = duckdb.scalar_function_get_extra_info(info);
          const rowCount = duckdb.data_chunk_get_size(input);
          for (let i = 0; i < rowCount; i++) {
            duckdb.vector_assign_string_element(
              output,
              i,
              `output_${i}_${JSON.stringify(extra_info)}`,
            );
          }
        },
      );
      duckdb.scalar_function_set_extra_info(scalar_function, {
        my_extra_info_key: 'my_extra_info_value',
      });
      duckdb.register_scalar_function(connection, scalar_function);
      duckdb.destroy_scalar_function_sync(scalar_function);

      const result = await duckdb.query(connection, 'select my_func()');
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 1,
        columns: [
          { name: 'my_func()', logicalType: { typeId: duckdb.Type.VARCHAR } },
        ],
        chunks: [
          {
            rowCount: 1,
            vectors: [
              data(
                16,
                [true],
                ['output_0_{"my_extra_info_key":"my_extra_info_value"}'],
              ),
            ],
          },
        ],
      });
    });
  });
  test('error handling', async () => {
    await withConnection(async (connection) => {
      const scalar_function = duckdb.create_scalar_function();
      duckdb.scalar_function_set_name(scalar_function, 'my_func');
      const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
      duckdb.scalar_function_set_return_type(scalar_function, varchar_type);
      duckdb.scalar_function_set_function(
        scalar_function,
        (_info, _input, _output) => {
          throw new Error('my_error');
        },
      );
      duckdb.register_scalar_function(connection, scalar_function);
      duckdb.destroy_scalar_function_sync(scalar_function);

      await expect(
        duckdb.query(connection, 'select my_func()'),
      ).rejects.toThrow('Invalid Input Error: my_error');
    });
  });
  test('parameters (fixed, volatile)', async () => {
    await withConnection(async (connection) => {
      const scalar_function = duckdb.create_scalar_function();
      duckdb.scalar_function_set_name(scalar_function, 'my_func');
      const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
      const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
      duckdb.scalar_function_add_parameter(scalar_function, int_type);
      duckdb.scalar_function_add_parameter(scalar_function, varchar_type);
      duckdb.scalar_function_set_return_type(scalar_function, varchar_type);
      duckdb.scalar_function_set_volatile(scalar_function);
      duckdb.scalar_function_set_function(
        scalar_function,
        (_info, input, output) => {
          const rowCount = duckdb.data_chunk_get_size(input);
          const vec0 = duckdb.data_chunk_get_vector(input, 0);
          const data0 = duckdb.vector_get_data(vec0, rowCount * 4);
          const dv0 = new DataView(data0.buffer);
          const vec1 = duckdb.data_chunk_get_vector(input, 1);
          const data1 = duckdb.vector_get_data(vec1, rowCount * 16);
          const dv1 = new DataView(data1.buffer);
          for (let i = 0; i < rowCount; i++) {
            duckdb.vector_assign_string_element(
              output,
              i,
              `output_${i}_${dv0.getInt32(i * 4, true)}_${dv1.getUint32(
                i * 16,
                true,
              )}`,
            );
          }
        },
      );
      duckdb.register_scalar_function(connection, scalar_function);
      duckdb.destroy_scalar_function_sync(scalar_function);

      const result = await duckdb.query(
        connection,
        "select my_func(42, 'duck') as my_func_result from range(3)",
      );
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 3,
        columns: [
          {
            name: 'my_func_result',
            logicalType: { typeId: duckdb.Type.VARCHAR },
          },
        ],
        chunks: [
          {
            rowCount: 3,
            vectors: [
              data(
                16,
                [true, true, true],
                ['output_0_42_4', 'output_1_42_4', 'output_2_42_4'],
              ),
            ],
          },
        ],
      });
    });
  });
  test('parameters (varargs, volatile)', async () => {
    await withConnection(async (connection) => {
      const scalar_function = duckdb.create_scalar_function();
      duckdb.scalar_function_set_name(scalar_function, 'my_func');
      const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
      const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
      duckdb.scalar_function_set_varargs(scalar_function, int_type);
      duckdb.scalar_function_set_return_type(scalar_function, varchar_type);
      duckdb.scalar_function_set_volatile(scalar_function);
      duckdb.scalar_function_set_function(
        scalar_function,
        (_info, input, output) => {
          const rowCount = duckdb.data_chunk_get_size(input);
          const paramCount = duckdb.data_chunk_get_column_count(input);

          for (let r = 0; r < rowCount; r++) {
            const params: number[] = [];
            for (let p = 0; p < paramCount; p++) {
              const vec = duckdb.data_chunk_get_vector(input, p);
              const data = duckdb.vector_get_data(vec, rowCount * 4);
              const dv = new DataView(data.buffer);
              params.push(dv.getInt32(r * 4, true));
            }
            duckdb.vector_assign_string_element(
              output,
              r,
              `output_${r}_${params.join('_')}`,
            );
          }
        },
      );
      duckdb.register_scalar_function(connection, scalar_function);
      duckdb.destroy_scalar_function_sync(scalar_function);

      const result = await duckdb.query(
        connection,
        'select my_func(11, 13, 17) as my_func_result from range(3)',
      );
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 3,
        columns: [
          {
            name: 'my_func_result',
            logicalType: { typeId: duckdb.Type.VARCHAR },
          },
        ],
        chunks: [
          {
            rowCount: 3,
            vectors: [
              data(
                16,
                [true, true, true],
                ['output_0_11_13_17', 'output_1_11_13_17', 'output_2_11_13_17'],
              ),
            ],
          },
        ],
      });
    });
  });
  test('special handling', async () => {
    await withConnection(async (connection) => {
      const scalar_function = duckdb.create_scalar_function();
      duckdb.scalar_function_set_name(scalar_function, 'my_func');
      const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
      duckdb.scalar_function_add_parameter(scalar_function, int_type);
      const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
      duckdb.scalar_function_set_return_type(scalar_function, varchar_type);
      duckdb.scalar_function_set_special_handling(scalar_function);
      duckdb.scalar_function_set_function(
        scalar_function,
        (_info, input, output) => {
          const rowCount = duckdb.data_chunk_get_size(input);
          for (let i = 0; i < rowCount; i++) {
            duckdb.vector_assign_string_element(
              output,
              i,
              `output_is_not_null`,
            );
          }
        },
      );
      duckdb.register_scalar_function(connection, scalar_function);
      duckdb.destroy_scalar_function_sync(scalar_function);

      const result = await duckdb.query(connection, 'select my_func(NULL)');
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 1,
        columns: [
          {
            name: 'my_func(NULL)',
            logicalType: { typeId: duckdb.Type.VARCHAR },
          },
        ],
        // Without special handling, this would be NULL
        chunks: [
          { rowCount: 1, vectors: [data(16, [true], ['output_is_not_null'])] },
        ],
      });
    });
  });
});
