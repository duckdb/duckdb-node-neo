import { assert, beforeAll, describe, test } from 'vitest';
import {
  DuckDBValue,
  INTEGER,
  VARCHAR,
} from '../src';
import { DuckDBScalarFunction } from '../src/DuckDBScalarFunction';
import {
  setDefaultTimezone,
  withConnection,
} from './util/testHelpers';

describe('scalar functions', () => {
  beforeAll(setDefaultTimezone);

  test('scalar function (no params)', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'my_func',
          mainFunction: (_info, input, output) => {
            for (let rowIndex = 0; rowIndex < input.rowCount; rowIndex++) {
              output.setItem(rowIndex, `my_output_${rowIndex}`);
            }
            output.flush();
          },
          returnType: VARCHAR,
        }),
      );
      const reader = await connection.runAndReadAll('select my_func()');
      const columns = reader.getColumnsObject();
      assert.deepEqual(columns, { 'my_func()': ['my_output_0'] });
    });
  });

  test('scalar function (bind data)', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'my_func',
          bindFunction: (info) => {
            info.setBindData({ 'my_bind_data_key': 'my_bind_data_value' });
          },
          mainFunction: (info, input, output) => {
            const { bindData } = info;
            for (let rowIndex = 0; rowIndex < input.rowCount; rowIndex++) {
              output.setItem(
                rowIndex,
                `my_output_${rowIndex}_${JSON.stringify(bindData)}`,
              );
            }
            output.flush();
          },
          returnType: VARCHAR,
        }),
      );
      const reader = await connection.runAndReadAll('select my_func()');
      const columns = reader.getColumnsObject();
      assert.deepEqual(columns, {
        'my_func()': [
          `my_output_0_${JSON.stringify({ 'my_bind_data_key': 'my_bind_data_value' })}`,
        ],
      });
    });
  });

  test('scalar function (extra info)', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'my_func',
          mainFunction: (info, input, output) => {
            const { extraInfo } = info;
            for (let rowIndex = 0; rowIndex < input.rowCount; rowIndex++) {
              output.setItem(
                rowIndex,
                `my_output_${rowIndex}_${JSON.stringify(extraInfo)}`,
              );
            }
            output.flush();
          },
          returnType: VARCHAR,
          extraInfo: { 'my_extra_info_key': 'my_extra_info_value' },
        }),
      );
      const reader = await connection.runAndReadAll('select my_func()');
      const columns = reader.getColumnsObject();
      assert.deepEqual(columns, {
        'my_func()': [
          'my_output_0_{"my_extra_info_key":"my_extra_info_value"}',
        ],
      });
    });
  });

  test('scalar function (extra info & bind data)', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'my_func',
          bindFunction: (info) => {
            const { extraInfo, clientContext } = info;
            info.setBindData({
              'my_bind_data_key': 'my_bind_data_value',
              'extra_info': extraInfo,
              'valid_connection_id': clientContext.connectionId > 0,
            });
          },
          mainFunction: (info, input, output) => {
            const { bindData } = info;
            for (let rowIndex = 0; rowIndex < input.rowCount; rowIndex++) {
              output.setItem(
                rowIndex,
                `my_output_${rowIndex}_${JSON.stringify(bindData)}`,
              );
            }
            output.flush();
          },
          returnType: VARCHAR,
          extraInfo: { 'my_extra_info_key': 'my_extra_info_value' },
        }),
      );
      const reader = await connection.runAndReadAll('select my_func()');
      const columns = reader.getColumnsObject();
      assert.deepEqual(columns, {
        'my_func()': [
          `my_output_0_${JSON.stringify({
            'my_bind_data_key': 'my_bind_data_value',
            'extra_info': { 'my_extra_info_key': 'my_extra_info_value' },
            'valid_connection_id': true,
          })}`,
        ],
      });
    });
  });

  test('scalar function (error handling: exception in main func)', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'my_func',
          mainFunction: (_info, _input, _output) => {
            throw new Error('my_error');
          },
          returnType: VARCHAR,
        }),
      );
      try {
        await connection.run('select my_func()');
        assert.fail('should throw');
      } catch (err) {
        assert.deepEqual(err, new Error('Invalid Input Error: my_error'));
      }
    });
  });

  test('scalar function (error handling: setError in main func)', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'my_func',
          mainFunction: (info, _input, _output) => {
            info.setError('my_error');
          },
          returnType: VARCHAR,
        }),
      );
      try {
        await connection.run('select my_func()');
        assert.fail('should throw');
      } catch (err) {
        assert.deepEqual(err, new Error('Invalid Input Error: my_error'));
      }
    });
  });

  test('scalar function (error handling: exception in bind func)', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'my_func',
          bindFunction: (_info) => {
            throw new Error('my_bind_error');
          },
          mainFunction: (_info, _input, _output) => {
            throw new Error('my_error');
          },
          returnType: VARCHAR,
        }),
      );
      try {
        await connection.run('select my_func()');
        assert.fail('should throw');
      } catch (err) {
        assert.deepEqual(err, new Error('Binder Error: my_bind_error'));
      }
    });
  });

  test('scalar function (error handling: setError in bind func)', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'my_func',
          bindFunction: (info) => {
            info.setError('my_bind_error');
          },
          mainFunction: (info, _input, _output) => {
            info.setError('my_error');
          },
          returnType: VARCHAR,
        }),
      );
      try {
        await connection.run('select my_func()');
        assert.fail('should throw');
      } catch (err) {
        assert.deepEqual(err, new Error('Binder Error: my_bind_error'));
      }
    });
  });

  test('scalar function (params, volatile)', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'my_func',
          mainFunction: (_info, input, output) => {
            const v0 = input.getColumnVector(0);
            const v1 = input.getColumnVector(1);
            for (let rowIndex = 0; rowIndex < input.rowCount; rowIndex++) {
              output.setItem(
                rowIndex,
                `my_output_${rowIndex}_${v0.getItem(rowIndex)}_${v1.getItem(
                  rowIndex,
                )}`,
              );
            }
            output.flush();
          },
          returnType: VARCHAR,
          parameterTypes: [INTEGER, VARCHAR],
          volatile: true,
        }),
      );
      const reader = await connection.runAndReadAll(
        `select my_func(42, 'duck') as my_func_result from range(3)`,
      );
      const columns = reader.getColumnsObject();
      assert.deepEqual(columns, {
        'my_func_result': [
          'my_output_0_42_duck',
          'my_output_1_42_duck',
          'my_output_2_42_duck',
        ],
      });
    });
  });

  test('scalar function (varargs, volatile)', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'my_func',
          mainFunction: (_info, input, output) => {
            // const v0 = input.getColumnVector(0);
            // const v1 = input.getColumnVector(1);
            for (let rowIndex = 0; rowIndex < input.rowCount; rowIndex++) {
              const argValues: DuckDBValue[] = [];
              for (
                let columnIndex = 0;
                columnIndex < input.columnCount;
                columnIndex++
              ) {
                argValues.push(
                  input.getColumnVector(columnIndex).getItem(rowIndex),
                );
              }
              output.setItem(
                rowIndex,
                `my_output_${rowIndex}_${argValues.join('_')}`,
              );
            }
            output.flush();
          },
          returnType: VARCHAR,
          varArgsType: INTEGER,
          volatile: true,
        }),
      );
      const reader = await connection.runAndReadAll(
        `select my_func(11, 13, 17) as my_func_result from range(3)`,
      );
      const columns = reader.getColumnsObject();
      assert.deepEqual(columns, {
        'my_func_result': [
          'my_output_0_11_13_17',
          'my_output_1_11_13_17',
          'my_output_2_11_13_17',
        ],
      });
    });
  });

  test('scalar function (special handling)', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'my_func',
          mainFunction: (_info, input, output) => {
            for (let rowIndex = 0; rowIndex < input.rowCount; rowIndex++) {
              output.setItem(rowIndex, 'output_is_not_null');
            }
            output.flush();
          },
          returnType: VARCHAR,
          parameterTypes: [INTEGER],
          specialHandling: true,
        }),
      );
      const reader = await connection.runAndReadAll(`select my_func(NULL)`);
      const columns = reader.getColumnsObject();
      assert.deepEqual(columns, {
        'my_func(NULL)': ['output_is_not_null'],
      });
    });
  });
});
