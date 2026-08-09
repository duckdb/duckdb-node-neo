import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { data } from './utils/expectedVectors';
import { expectResult } from './utils/expectResult';
import { withConnection } from './utils/withConnection';

suite('table functions', () => {
  test('create', () => {
    const table_function = duckdb.create_table_function();
    expect(table_function).toBeTruthy();
  });
  test('set name', () => {
    const table_function = duckdb.create_table_function();
    duckdb.table_function_set_name(table_function, 'my_func');
  });
  test('add parameters', () => {
    const table_function = duckdb.create_table_function();
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    duckdb.table_function_add_parameter(table_function, int_type);
    const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
    duckdb.table_function_add_named_parameter(
      table_function,
      'my_named_param',
      varchar_type,
    );
  });
  test('supports projection pushdown', () => {
    const table_function = duckdb.create_table_function();
    duckdb.table_function_supports_projection_pushdown(table_function, true);
    duckdb.table_function_supports_projection_pushdown(table_function, false);
  });
  test('set extra info', () => {
    const table_function = duckdb.create_table_function();
    duckdb.table_function_set_extra_info(table_function, {
      'my_extra_info_key': 'my_extra_info_value',
    });
  });
  test('destroy', () => {
    const table_function = duckdb.create_table_function();
    duckdb.destroy_table_function_sync(table_function);
    // Destroying twice is a no-op, not a crash.
    duckdb.destroy_table_function_sync(table_function);
  });
  test('register and run', async () => {
    await withConnection(async (connection) => {
      const table_function = duckdb.create_table_function();
      duckdb.table_function_set_name(table_function, 'my_func');
      duckdb.table_function_set_bind(table_function, (info) => {
        const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
        duckdb.bind_add_result_column(info, 'my_column', varchar_type);
        duckdb.bind_set_bind_data(info, { 'row_count': 3 });
      });
      duckdb.table_function_set_init(table_function, (info) => {
        duckdb.init_set_init_data(info, { 'next_row': 0 });
      });
      duckdb.table_function_set_function(table_function, (info, output) => {
        const bind_data = duckdb.function_get_bind_data(info) as {
          row_count: number;
        };
        const init_data = duckdb.function_get_init_data(info) as {
          next_row: number;
        };
        const remaining = bind_data.row_count - init_data.next_row;
        const vector = duckdb.data_chunk_get_vector(output, 0);
        for (let i = 0; i < remaining; i++) {
          duckdb.vector_assign_string_element(
            vector,
            i,
            `row_${init_data.next_row + i}`,
          );
        }
        // A zero-size chunk is how a scan reports that it is finished.
        duckdb.data_chunk_set_size(output, remaining);
        init_data.next_row += remaining;
      });
      duckdb.register_table_function(connection, table_function);
      duckdb.destroy_table_function_sync(table_function);

      const result = await duckdb.query(connection, 'select * from my_func()');
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 3,
        columns: [
          { name: 'my_column', logicalType: { typeId: duckdb.Type.VARCHAR } },
        ],
        chunks: [
          {
            rowCount: 3,
            vectors: [
              data(16, [true, true, true], ['row_0', 'row_1', 'row_2']),
            ],
          },
        ],
      });
    });
  });
  test('register fails without bind, init and main functions', async () => {
    await withConnection(async (connection) => {
      const table_function = duckdb.create_table_function();
      duckdb.table_function_set_name(table_function, 'my_func');
      // DuckDB requires at least a name, a bind, an init and a main function.
      expect(() =>
        duckdb.register_table_function(connection, table_function),
      ).toThrowError('Failed to register table function');
    });
  });
  test('a scalar function external is not a table function', () => {
    const scalar_function = duckdb.create_scalar_function();
    expect(() =>
      duckdb.table_function_set_name(scalar_function as never, 'my_func'),
    ).toThrowError('Invalid table function argument');
  });

  // A minimal working function, so that each test below can break exactly one
  // part of it and leave the rest valid.
  function registerFunction(
    connection: duckdb.Connection,
    {
      rowCount = 3,
      bindFunction,
      initFunction,
      localInitFunction,
      mainFunction,
    }: {
      rowCount?: number;
      bindFunction?: duckdb.TableFunctionBindFunction;
      initFunction?: duckdb.TableFunctionInitFunction;
      localInitFunction?: duckdb.TableFunctionInitFunction;
      mainFunction?: duckdb.TableFunctionMainFunction;
    } = {},
  ) {
    const table_function = duckdb.create_table_function();
    duckdb.table_function_set_name(table_function, 'my_func');
    duckdb.table_function_set_bind(
      table_function,
      bindFunction ??
        ((info) => {
          const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
          duckdb.bind_add_result_column(info, 'my_column', varchar_type);
          duckdb.bind_set_bind_data(info, { 'row_count': rowCount });
        }),
    );
    duckdb.table_function_set_init(
      table_function,
      initFunction ??
        ((info) => {
          duckdb.init_set_init_data(info, { 'next_row': 0 });
        }),
    );
    if (localInitFunction) {
      duckdb.table_function_set_local_init(table_function, localInitFunction);
    }
    duckdb.table_function_set_function(
      table_function,
      mainFunction ??
        ((info, output) => {
          const bind_data = duckdb.function_get_bind_data(info) as {
            row_count: number;
          };
          const init_data = duckdb.function_get_init_data(info) as {
            next_row: number;
          };
          const chunkSize = Math.min(
            duckdb.vector_size(),
            bind_data.row_count - init_data.next_row,
          );
          const vector = duckdb.data_chunk_get_vector(output, 0);
          for (let i = 0; i < chunkSize; i++) {
            duckdb.vector_assign_string_element(
              vector,
              i,
              `row_${init_data.next_row + i}`,
            );
          }
          duckdb.data_chunk_set_size(output, chunkSize);
          init_data.next_row += chunkSize;
        }),
    );
    duckdb.register_table_function(connection, table_function);
    duckdb.destroy_table_function_sync(table_function);
  }

  test('scan spanning multiple chunks', async () => {
    await withConnection(async (connection) => {
      // More than one vector's worth of rows, so the main function runs several
      // times and has to be told where it left off by its init data.
      const rowCount = duckdb.vector_size() * 2 + 5;
      registerFunction(connection, { rowCount });
      const result = await duckdb.query(
        connection,
        'select count(*) as n, min(my_column) as lo, max(my_column) as hi from my_func()',
      );
      const chunk = await duckdb.fetch_chunk(result);
      expect(chunk).toBeTruthy();
      expect(duckdb.data_chunk_get_size(chunk!)).toBe(1);
      const countVector = duckdb.data_chunk_get_vector(chunk!, 0);
      const countData = duckdb.vector_get_data(countVector, 8);
      expect(new DataView(countData.buffer).getBigInt64(0, true)).toBe(
        BigInt(rowCount),
      );
    });
  });

  test('local init data is available to the main function', async () => {
    await withConnection(async (connection) => {
      registerFunction(connection, {
        localInitFunction: (info) => {
          duckdb.init_set_init_data(info, { 'token': 'local_init' });
        },
        mainFunction: (info, output) => {
          const local = duckdb.function_get_local_init_data(info) as {
            token: string;
          };
          const init_data = duckdb.function_get_init_data(info) as {
            next_row: number;
          };
          const chunkSize = init_data.next_row === 0 ? 1 : 0;
          if (chunkSize > 0) {
            const vector = duckdb.data_chunk_get_vector(output, 0);
            duckdb.vector_assign_string_element(vector, 0, local.token);
            init_data.next_row = 1;
          }
          duckdb.data_chunk_set_size(output, chunkSize);
        },
      });
      const result = await duckdb.query(connection, 'select * from my_func()');
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 1,
        columns: [
          { name: 'my_column', logicalType: { typeId: duckdb.Type.VARCHAR } },
        ],
        chunks: [{ rowCount: 1, vectors: [data(16, [true], ['local_init'])] }],
      });
    });
  });

  test('error handling (exception in bind func)', async () => {
    await withConnection(async (connection) => {
      registerFunction(connection, {
        bindFunction: () => {
          throw new Error('my_bind_error');
        },
      });
      await expect(
        duckdb.query(connection, 'select * from my_func()'),
      ).rejects.toThrow('my_bind_error');
    });
  });

  test('error handling (set error in bind func)', async () => {
    await withConnection(async (connection) => {
      registerFunction(connection, {
        bindFunction: (info) => {
          duckdb.bind_set_error(info, 'my_bind_error');
        },
      });
      await expect(
        duckdb.query(connection, 'select * from my_func()'),
      ).rejects.toThrow('my_bind_error');
    });
  });

  test('error handling (exception in init func)', async () => {
    await withConnection(async (connection) => {
      registerFunction(connection, {
        initFunction: () => {
          throw new Error('my_init_error');
        },
      });
      await expect(
        duckdb.query(connection, 'select * from my_func()'),
      ).rejects.toThrow('my_init_error');
    });
  });

  test('error handling (set error in init func)', async () => {
    await withConnection(async (connection) => {
      registerFunction(connection, {
        initFunction: (info) => {
          duckdb.init_set_error(info, 'my_init_error');
        },
      });
      await expect(
        duckdb.query(connection, 'select * from my_func()'),
      ).rejects.toThrow('my_init_error');
    });
  });

  test('error handling (exception in main func)', async () => {
    await withConnection(async (connection) => {
      registerFunction(connection, {
        mainFunction: () => {
          throw new Error('my_main_error');
        },
      });
      await expect(
        duckdb.query(connection, 'select * from my_func()'),
      ).rejects.toThrow('my_main_error');
    });
  });

  test('error handling (set error in main func)', async () => {
    await withConnection(async (connection) => {
      registerFunction(connection, {
        mainFunction: (info) => {
          duckdb.function_set_error(info, 'my_main_error');
        },
      });
      await expect(
        duckdb.query(connection, 'select * from my_func()'),
      ).rejects.toThrow('my_main_error');
    });
  });

  test('a table function info is not a scalar function info', async () => {
    await withConnection(async (connection) => {
      let caught: string | undefined;
      registerFunction(connection, {
        mainFunction: (info, output) => {
          try {
            // Same underlying C type, different family: this must throw rather
            // than reinterpret the handle as a scalar function's struct.
            duckdb.scalar_function_get_bind_data(info as never);
          } catch (e) {
            caught = String((e as Error).message);
          }
          duckdb.data_chunk_set_size(output, 0);
        },
      });
      await duckdb.query(connection, 'select * from my_func()');
      expect(caught).toBe('Invalid scalar function info argument');
    });
  });

  test('positional and named parameters', async () => {
    await withConnection(async (connection) => {
      const table_function = duckdb.create_table_function();
      duckdb.table_function_set_name(table_function, 'my_func');
      const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
      const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
      duckdb.table_function_add_parameter(table_function, int_type);
      duckdb.table_function_add_named_parameter(
        table_function,
        'prefix',
        varchar_type,
      );
      duckdb.table_function_set_bind(table_function, (info) => {
        const parameter_count = duckdb.bind_get_parameter_count(info);
        const count_value = duckdb.bind_get_parameter(info, 0);
        const count = duckdb.get_int32(count_value);
        const prefix_value = duckdb.bind_get_named_parameter(info, 'prefix');
        const prefix = prefix_value ? duckdb.get_varchar(prefix_value) : 'none';
        const missing = duckdb.bind_get_named_parameter(info, 'not_supplied');
        duckdb.bind_add_result_column(info, 'my_column', varchar_type);
        // Cardinality is only a hint to the optimizer, so nothing observable
        // depends on it; this just exercises the call.
        duckdb.bind_set_cardinality(info, count, true);
        duckdb.bind_set_bind_data(info, {
          count,
          prefix,
          parameter_count,
          'missing_is_null': missing === null,
        });
      });
      duckdb.table_function_set_init(table_function, (info) => {
        duckdb.init_set_init_data(info, { 'done': false });
      });
      duckdb.table_function_set_function(table_function, (info, output) => {
        const bind_data = duckdb.function_get_bind_data(info) as {
          count: number;
          prefix: string;
          parameter_count: number;
          missing_is_null: boolean;
        };
        const init_data = duckdb.function_get_init_data(info) as {
          done: boolean;
        };
        if (init_data.done) {
          duckdb.data_chunk_set_size(output, 0);
          return;
        }
        const vector = duckdb.data_chunk_get_vector(output, 0);
        for (let i = 0; i < bind_data.count; i++) {
          duckdb.vector_assign_string_element(
            vector,
            i,
            `${bind_data.prefix}_${i}_p${bind_data.parameter_count}_m${bind_data.missing_is_null}`,
          );
        }
        duckdb.data_chunk_set_size(output, bind_data.count);
        init_data.done = true;
      });
      duckdb.register_table_function(connection, table_function);
      duckdb.destroy_table_function_sync(table_function);

      const result = await duckdb.query(
        connection,
        "select * from my_func(2, prefix => 'row')",
      );
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 2,
        columns: [
          { name: 'my_column', logicalType: { typeId: duckdb.Type.VARCHAR } },
        ],
        chunks: [
          {
            rowCount: 2,
            vectors: [
              data(
                16,
                [true, true],
                ['row_0_p1_mtrue', 'row_1_p1_mtrue'],
              ),
            ],
          },
        ],
      });
    });
  });

  test('extra info is available to bind, init and the main function', async () => {
    await withConnection(async (connection) => {
      const seen: string[] = [];
      const table_function = duckdb.create_table_function();
      duckdb.table_function_set_name(table_function, 'my_func');
      duckdb.table_function_set_extra_info(table_function, {
        'token': 'extra_info',
      });
      duckdb.table_function_set_bind(table_function, (info) => {
        seen.push(
          `bind:${(duckdb.bind_get_extra_info(info) as { token: string }).token}`,
        );
        const client_context = duckdb.table_function_get_client_context(info);
        seen.push(
          `context:${duckdb.client_context_get_connection_id(client_context) > 0}`,
        );
        const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
        duckdb.bind_add_result_column(info, 'my_column', varchar_type);
        duckdb.bind_set_bind_data(info, { 'token': 'bind_data' });
      });
      duckdb.table_function_set_init(table_function, (info) => {
        seen.push(
          `init:${(duckdb.init_get_extra_info(info) as { token: string }).token}`,
        );
        seen.push(
          `init_bind_data:${(duckdb.init_get_bind_data(info) as { token: string }).token}`,
        );
        duckdb.init_set_init_data(info, { 'done': false });
      });
      duckdb.table_function_set_function(table_function, (info, output) => {
        seen.push(
          `main:${(duckdb.function_get_extra_info(info) as { token: string }).token}`,
        );
        duckdb.data_chunk_set_size(output, 0);
      });
      duckdb.register_table_function(connection, table_function);
      duckdb.destroy_table_function_sync(table_function);

      await duckdb.query(connection, 'select * from my_func()');
      expect(seen).toStrictEqual([
        'bind:extra_info',
        'context:true',
        'init:extra_info',
        'init_bind_data:bind_data',
        'main:extra_info',
      ]);
    });
  });

  test('projection pushdown reports the projected columns', async () => {
    await withConnection(async (connection) => {
      let projected: number[] | undefined;
      const table_function = duckdb.create_table_function();
      duckdb.table_function_set_name(table_function, 'my_func');
      duckdb.table_function_supports_projection_pushdown(table_function, true);
      duckdb.table_function_set_bind(table_function, (info) => {
        const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
        duckdb.bind_add_result_column(info, 'a', varchar_type);
        duckdb.bind_add_result_column(info, 'b', varchar_type);
        duckdb.bind_add_result_column(info, 'c', varchar_type);
        duckdb.bind_set_bind_data(info, {});
      });
      duckdb.table_function_set_init(table_function, (info) => {
        const column_count = duckdb.init_get_column_count(info);
        projected = [];
        for (let i = 0; i < column_count; i++) {
          projected.push(duckdb.init_get_column_index(info, i));
        }
        duckdb.init_set_init_data(info, { 'done': false });
      });
      duckdb.table_function_set_function(table_function, (_info, output) => {
        duckdb.data_chunk_set_size(output, 0);
      });
      duckdb.register_table_function(connection, table_function);
      duckdb.destroy_table_function_sync(table_function);

      // Only column b is selected, so that is all init should be told about.
      await duckdb.query(connection, 'select b from my_func()');
      expect(projected).toStrictEqual([1]);
    });
  });

  test('max threads can be set from init', async () => {
    await withConnection(async (connection) => {
      registerFunction(connection, {
        rowCount: duckdb.vector_size() * 2,
        initFunction: (info) => {
          duckdb.init_set_max_threads(info, 4);
          duckdb.init_set_init_data(info, { 'next_row': 0 });
        },
      });
      const result = await duckdb.query(
        connection,
        'select count(*) as n from my_func()',
      );
      const chunk = await duckdb.fetch_chunk(result);
      const vector = duckdb.data_chunk_get_vector(chunk!, 0);
      const countData = duckdb.vector_get_data(vector, 8);
      expect(new DataView(countData.buffer).getBigInt64(0, true)).toBe(
        BigInt(duckdb.vector_size() * 2),
      );
    });
  });
});
