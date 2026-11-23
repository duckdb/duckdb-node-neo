import duckdb from '@duckdb/node-bindings';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { expect, suite, test } from 'vitest';
import { data } from './utils/expectedVectors';
import { expectResult } from './utils/expectResult';
import { withConnection } from './utils/withConnection';

function readInt32s(vector: duckdb.Vector, count: number): number[] {
  const bytes = duckdb.vector_get_data(vector, 4 * count);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const values: number[] = [];
  for (let i = 0; i < count; i++) {
    values.push(view.getInt32(4 * i, true));
  }
  return values;
}

async function withTempDir(fn: (dir: string) => Promise<void>) {
  const dir = mkdtempSync(join(tmpdir(), 'duckdb-copy-'));
  try {
    await fn(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

suite('copy functions', () => {
  test('create', () => {
    const copy_function = duckdb.create_copy_function();
    expect(copy_function).toBeTruthy();
  });
  test('set name', () => {
    const copy_function = duckdb.create_copy_function();
    duckdb.copy_function_set_name(copy_function, 'my_format');
  });
  test('set extra info', () => {
    const copy_function = duckdb.create_copy_function();
    duckdb.copy_function_set_extra_info(copy_function, {
      'my_extra_info_key': 'my_extra_info_value',
    });
  });
  test('destroy', () => {
    const copy_function = duckdb.create_copy_function();
    duckdb.destroy_copy_function_sync(copy_function);
    // Destroying twice is a no-op, not a crash.
    duckdb.destroy_copy_function_sync(copy_function);
  });
  test('register needs a name and a sink', async () => {
    await withConnection(async (connection) => {
      const failure = 'Failed to register copy function';

      const unnamed = duckdb.create_copy_function();
      duckdb.copy_function_set_sink(unnamed, () => {});
      expect(() => duckdb.register_copy_function(connection, unnamed)).toThrowError(failure);

      const no_sink = duckdb.create_copy_function();
      duckdb.copy_function_set_name(no_sink, 'my_format');
      duckdb.copy_function_set_bind(no_sink, () => {});
      duckdb.copy_function_set_global_init(no_sink, () => {});
      duckdb.copy_function_set_finalize(no_sink, () => {});
      expect(() => duckdb.register_copy_function(connection, no_sink)).toThrowError(failure);

      // Bind, global init and finalize are optional.
      const minimal = duckdb.create_copy_function();
      duckdb.copy_function_set_name(minimal, 'my_format');
      duckdb.copy_function_set_sink(minimal, () => {});
      duckdb.register_copy_function(connection, minimal);
    });
  });
  test('a table function external is not a copy function', () => {
    const table_function = duckdb.create_table_function();
    expect(() =>
      duckdb.copy_function_set_name(table_function as never, 'my_format'),
    ).toThrowError('Invalid copy function argument');
  });

  // A working writer that collects the first column's INTEGER values and writes
  // them to the file in finalize, so that each test below can replace one
  // callback and leave the rest valid.
  function registerWriter(
    connection: duckdb.Connection,
    {
      bindFunction,
      globalInitFunction,
      sinkFunction,
      finalizeFunction,
    }: {
      bindFunction?: duckdb.CopyFunctionBindFunction;
      globalInitFunction?: duckdb.CopyFunctionGlobalInitFunction;
      sinkFunction?: duckdb.CopyFunctionSinkFunction;
      finalizeFunction?: duckdb.CopyFunctionFinalizeFunction;
    } = {},
  ) {
    const copy_function = duckdb.create_copy_function();
    duckdb.copy_function_set_name(copy_function, 'my_format');
    duckdb.copy_function_set_bind(
      copy_function,
      bindFunction ??
        ((info) => {
          duckdb.copy_function_bind_set_bind_data(info, {
            'column_count': duckdb.copy_function_bind_get_column_count(info),
          });
        }),
    );
    duckdb.copy_function_set_global_init(
      copy_function,
      globalInitFunction ??
        ((info) => {
          duckdb.copy_function_global_init_set_global_state(info, {
            'path': duckdb.copy_function_global_init_get_file_path(info),
            'values': [] as number[],
            'sink_calls': 0,
          });
        }),
    );
    duckdb.copy_function_set_sink(
      copy_function,
      sinkFunction ??
        ((info, input) => {
          const state = duckdb.copy_function_sink_get_global_state(info) as {
            values: number[];
            sink_calls: number;
          };
          const size = duckdb.data_chunk_get_size(input);
          state.values.push(...readInt32s(duckdb.data_chunk_get_vector(input, 0), size));
          state.sink_calls++;
        }),
    );
    duckdb.copy_function_set_finalize(
      copy_function,
      finalizeFunction ??
        ((info) => {
          const state = duckdb.copy_function_finalize_get_global_state(info) as {
            path: string;
            values: number[];
            sink_calls: number;
          };
          writeFileSync(state.path, `${state.sink_calls}:${state.values.join(',')}`);
        }),
    );
    duckdb.register_copy_function(connection, copy_function);
    duckdb.destroy_copy_function_sync(copy_function);
  }

  test('write a file with COPY TO', async () => {
    await withTempDir(async (dir) => {
      await withConnection(async (connection) => {
        registerWriter(connection);
        const path = join(dir, 'out.my_format');
        const result = await duckdb.query(
          connection,
          `COPY (SELECT range::INTEGER AS i FROM range(5)) TO '${path}' (FORMAT my_format)`,
        );
        // COPY reports the number of rows written.
        await expectResult(result, {
          statementType: duckdb.StatementType.COPY,
          resultType: duckdb.ResultType.CHANGED_ROWS,
          rowsChanged: 5,
          chunkCount: 1,
          rowCount: 1,
          columns: [{ name: 'Count', logicalType: { typeId: duckdb.Type.BIGINT } }],
          chunks: [{ rowCount: 1, vectors: [data(8, [true], [5n])] }],
        });
        expect(readFileSync(path, 'utf8')).toBe('1:0,1,2,3,4');
      });
    });
  });

  test('rows spanning several chunks reach the sink one chunk at a time', async () => {
    await withTempDir(async (dir) => {
      await withConnection(async (connection) => {
        registerWriter(connection);
        const rowCount = duckdb.vector_size() * 2 + 5;
        const path = join(dir, 'out.my_format');
        await duckdb.query(
          connection,
          `COPY (SELECT range::INTEGER AS i FROM range(${rowCount})) TO '${path}' (FORMAT my_format)`,
        );
        const [sinkCalls, values] = readFileSync(path, 'utf8').split(':');
        expect(Number(sinkCalls)).toBe(3);
        expect(values.split(',').map(Number)).toEqual(Array.from({ length: rowCount }, (_, i) => i));
      });
    });
  });

  test('no rows still runs global init and finalize', async () => {
    await withTempDir(async (dir) => {
      await withConnection(async (connection) => {
        registerWriter(connection);
        const path = join(dir, 'out.my_format');
        await duckdb.query(
          connection,
          `COPY (SELECT 1::INTEGER AS i WHERE false) TO '${path}' (FORMAT my_format)`,
        );
        expect(readFileSync(path, 'utf8')).toBe('0:');
      });
    });
  });

  test('bind sees the columns and the options', async () => {
    await withTempDir(async (dir) => {
      await withConnection(async (connection) => {
        const seen: {
          columns?: string[];
          options?: Record<string, string>;
        } = {};
        registerWriter(connection, {
          bindFunction: (info) => {
            const count = duckdb.copy_function_bind_get_column_count(info);
            seen.columns = [];
            for (let i = 0; i < count; i++) {
              const type = duckdb.copy_function_bind_get_column_type(info, i);
              seen.columns.push(duckdb.Type[duckdb.get_type_id(type)]);
            }
            // Options arrive as a STRUCT keyed by the upper-cased option name. A
            // value in parentheses is a LIST.
            const options = duckdb.copy_function_bind_get_options(info);
            const optionsType = duckdb.get_value_type(options);
            seen.options = {};
            for (let i = 0; i < duckdb.struct_type_child_count(optionsType); i++) {
              const name = duckdb.struct_type_child_name(optionsType, i);
              const value = duckdb.get_struct_child(options, i);
              switch (duckdb.get_type_id(duckdb.get_value_type(value))) {
                case duckdb.Type.VARCHAR:
                  seen.options[name] = duckdb.get_varchar(value);
                  break;
                case duckdb.Type.INTEGER:
                  seen.options[name] = String(duckdb.get_int32(value));
                  break;
                case duckdb.Type.LIST: {
                  const items: string[] = [];
                  for (let j = 0; j < duckdb.get_list_size(value); j++) {
                    items.push(duckdb.get_varchar(duckdb.get_list_child(value, j)));
                  }
                  seen.options[name] = items.join('|');
                  break;
                }
              }
            }
          },
        });
        const path = join(dir, 'out.my_format');
        await duckdb.query(
          connection,
          `COPY (SELECT 1::INTEGER AS i, 'a' AS s, true AS b) TO '${path}' (FORMAT my_format, token 'abc', count 7, flags ('x', 'y'))`,
        );
        expect(seen.columns).toEqual(['INTEGER', 'VARCHAR', 'BOOLEAN']);
        expect(seen.options).toEqual({ TOKEN: 'abc', COUNT: '7', FLAGS: 'x|y' });
      });
    });
  });

  test('the options value is NULL when no options are given', async () => {
    await withTempDir(async (dir) => {
      await withConnection(async (connection) => {
        let optionsType: duckdb.Type | undefined;
        registerWriter(connection, {
          bindFunction: (info) => {
            optionsType = duckdb.get_type_id(duckdb.get_value_type(duckdb.copy_function_bind_get_options(info)));
          },
        });
        const path = join(dir, 'out.my_format');
        await duckdb.query(connection, `COPY (SELECT 1::INTEGER AS i) TO '${path}' (FORMAT my_format)`);
        expect(optionsType).toBe(duckdb.Type.SQLNULL);
      });
    });
  });

  test('bind data, global state and extra info reach every callback', async () => {
    await withTempDir(async (dir) => {
      await withConnection(async (connection) => {
        const seen: Record<string, unknown[]> = {};
        const copy_function = duckdb.create_copy_function();
        duckdb.copy_function_set_name(copy_function, 'my_format');
        duckdb.copy_function_set_extra_info(copy_function, { 'extra': 'info' });
        duckdb.copy_function_set_bind(copy_function, (info) => {
          seen.bind = [duckdb.copy_function_bind_get_extra_info(info)];
          duckdb.copy_function_bind_set_bind_data(info, { 'bound': true });
        });
        duckdb.copy_function_set_global_init(copy_function, (info) => {
          seen.init = [
            duckdb.copy_function_global_init_get_extra_info(info),
            duckdb.copy_function_global_init_get_bind_data(info),
          ];
          duckdb.copy_function_global_init_set_global_state(info, { 'state': 'global' });
        });
        duckdb.copy_function_set_sink(copy_function, (info) => {
          seen.sink = [
            duckdb.copy_function_sink_get_extra_info(info),
            duckdb.copy_function_sink_get_bind_data(info),
            duckdb.copy_function_sink_get_global_state(info),
          ];
        });
        duckdb.copy_function_set_finalize(copy_function, (info) => {
          seen.finalize = [
            duckdb.copy_function_finalize_get_extra_info(info),
            duckdb.copy_function_finalize_get_bind_data(info),
            duckdb.copy_function_finalize_get_global_state(info),
          ];
        });
        duckdb.register_copy_function(connection, copy_function);
        const path = join(dir, 'out.my_format');
        await duckdb.query(connection, `COPY (SELECT 1::INTEGER AS i) TO '${path}' (FORMAT my_format)`);
        expect(seen.bind).toEqual([{ 'extra': 'info' }]);
        expect(seen.init).toEqual([{ 'extra': 'info' }, { 'bound': true }]);
        expect(seen.sink).toEqual([{ 'extra': 'info' }, { 'bound': true }, { 'state': 'global' }]);
        expect(seen.finalize).toEqual([{ 'extra': 'info' }, { 'bound': true }, { 'state': 'global' }]);
      });
    });
  });

  test('the client context in every callback is the copying connection', async () => {
    await withTempDir(async (dir) => {
      await withConnection(async (connection) => {
        const expected = duckdb.client_context_get_connection_id(duckdb.connection_get_client_context(connection));
        const ids: bigint[] = [];
        const copy_function = duckdb.create_copy_function();
        duckdb.copy_function_set_name(copy_function, 'my_format');
        duckdb.copy_function_set_bind(copy_function, (info) => {
          ids.push(duckdb.client_context_get_connection_id(duckdb.copy_function_bind_get_client_context(info)));
        });
        duckdb.copy_function_set_global_init(copy_function, (info) => {
          ids.push(duckdb.client_context_get_connection_id(duckdb.copy_function_global_init_get_client_context(info)));
        });
        duckdb.copy_function_set_sink(copy_function, (info) => {
          ids.push(duckdb.client_context_get_connection_id(duckdb.copy_function_sink_get_client_context(info)));
        });
        duckdb.copy_function_set_finalize(copy_function, (info) => {
          ids.push(duckdb.client_context_get_connection_id(duckdb.copy_function_finalize_get_client_context(info)));
        });
        duckdb.register_copy_function(connection, copy_function);
        const path = join(dir, 'out.my_format');
        await duckdb.query(connection, `COPY (SELECT 1::INTEGER AS i) TO '${path}' (FORMAT my_format)`);
        expect(ids).toEqual([expected, expected, expected, expected]);
      });
    });
  });

  for (const [stage, message] of [
    ['bind', 'my_bind_error'],
    ['global init', 'my_init_error'],
    ['sink', 'my_sink_error'],
    ['finalize', 'my_finalize_error'],
  ] as const) {
    test(`error handling (exception in ${stage} func)`, async () => {
      await withTempDir(async (dir) => {
        await withConnection(async (connection) => {
          const fail = () => {
            throw new Error(message);
          };
          registerWriter(connection, {
            bindFunction: stage === 'bind' ? fail : undefined,
            globalInitFunction: stage === 'global init' ? fail : undefined,
            sinkFunction: stage === 'sink' ? fail : undefined,
            finalizeFunction: stage === 'finalize' ? fail : undefined,
          });
          const path = join(dir, 'out.my_format');
          await expect(
            duckdb.query(connection, `COPY (SELECT 1::INTEGER AS i) TO '${path}' (FORMAT my_format)`),
          ).rejects.toThrow(message);
        });
      });
    });
    test(`error handling (set error in ${stage} func)`, async () => {
      await withTempDir(async (dir) => {
        await withConnection(async (connection) => {
          registerWriter(connection, {
            bindFunction: stage === 'bind' ? (info) => duckdb.copy_function_bind_set_error(info, message) : undefined,
            globalInitFunction:
              stage === 'global init' ? (info) => duckdb.copy_function_global_init_set_error(info, message) : undefined,
            sinkFunction: stage === 'sink' ? (info) => duckdb.copy_function_sink_set_error(info, message) : undefined,
            finalizeFunction:
              stage === 'finalize' ? (info) => duckdb.copy_function_finalize_set_error(info, message) : undefined,
          });
          const path = join(dir, 'out.my_format');
          await expect(
            duckdb.query(connection, `COPY (SELECT 1::INTEGER AS i) TO '${path}' (FORMAT my_format)`),
          ).rejects.toThrow(message);
        });
      });
    });
  }

  test('read with COPY FROM through a table function', async () => {
    await withConnection(async (connection) => {
      await duckdb.query(connection, 'CREATE TABLE target (a INTEGER, b VARCHAR)');
      const seen: { columns?: string[]; path?: string; rows?: number } = {};

      // The table function takes the file path and, as named parameters, the
      // COPY options. It reads the target table's columns instead of adding
      // result columns of its own.
      const table_function = duckdb.create_table_function();
      duckdb.table_function_set_name(table_function, 'my_format_reader');
      duckdb.table_function_add_parameter(table_function, duckdb.create_logical_type(duckdb.Type.VARCHAR));
      duckdb.table_function_add_named_parameter(table_function, 'rows', duckdb.create_logical_type(duckdb.Type.INTEGER));
      duckdb.table_function_set_bind(table_function, (info) => {
        const count = duckdb.table_function_bind_get_result_column_count(info);
        seen.columns = [];
        for (let i = 0; i < count; i++) {
          const type = duckdb.table_function_bind_get_result_column_type(info, i);
          seen.columns.push(
            `${duckdb.table_function_bind_get_result_column_name(info, i)} ${duckdb.Type[duckdb.get_type_id(type)]}`,
          );
        }
        seen.path = duckdb.get_varchar(duckdb.bind_get_parameter(info, 0));
        seen.rows = duckdb.get_int32(duckdb.bind_get_named_parameter(info, 'rows')!);
        duckdb.bind_set_bind_data(info, { 'rows': seen.rows });
      });
      duckdb.table_function_set_init(table_function, (info) => {
        duckdb.init_set_init_data(info, { 'done': false });
      });
      duckdb.table_function_set_function(table_function, (info, output) => {
        const bind_data = duckdb.function_get_bind_data(info) as { rows: number };
        const init_data = duckdb.function_get_init_data(info) as { done: boolean };
        if (init_data.done) {
          duckdb.data_chunk_set_size(output, 0);
          return;
        }
        const a = new ArrayBuffer(4 * bind_data.rows);
        const view = new DataView(a);
        const b = duckdb.data_chunk_get_vector(output, 1);
        for (let i = 0; i < bind_data.rows; i++) {
          view.setInt32(4 * i, 10 + i, true);
          duckdb.vector_assign_string_element(b, i, `row_${i}`);
        }
        duckdb.copy_data_to_vector(duckdb.data_chunk_get_vector(output, 0), 0, a, 0, a.byteLength);
        duckdb.data_chunk_set_size(output, bind_data.rows);
        init_data.done = true;
      });

      const copy_function = duckdb.create_copy_function();
      duckdb.copy_function_set_name(copy_function, 'my_format');
      duckdb.copy_function_set_copy_from_function(copy_function, table_function);
      duckdb.register_copy_function(connection, copy_function);
      duckdb.destroy_table_function_sync(table_function);
      duckdb.destroy_copy_function_sync(copy_function);

      const result = await duckdb.query(connection, "COPY target FROM 'input.my_format' (FORMAT my_format, rows 3)");
      await expectResult(result, {
        statementType: duckdb.StatementType.COPY,
        resultType: duckdb.ResultType.CHANGED_ROWS,
        rowsChanged: 3,
        chunkCount: 1,
        rowCount: 1,
        columns: [{ name: 'Count', logicalType: { typeId: duckdb.Type.BIGINT } }],
        chunks: [{ rowCount: 1, vectors: [data(8, [true], [3n])] }],
      });
      expect(seen).toEqual({ columns: ['a INTEGER', 'b VARCHAR'], path: 'input.my_format', rows: 3 });

      const rows = await duckdb.query(connection, 'SELECT a, b FROM target ORDER BY a');
      await expectResult(rows, {
        chunkCount: 1,
        rowCount: 3,
        columns: [
          { name: 'a', logicalType: { typeId: duckdb.Type.INTEGER } },
          { name: 'b', logicalType: { typeId: duckdb.Type.VARCHAR } },
        ],
        chunks: [
          {
            rowCount: 3,
            vectors: [
              data(4, [true, true, true], [10, 11, 12]),
              data(16, [true, true, true], ['row_0', 'row_1', 'row_2']),
            ],
          },
        ],
      });
    });
  });
});
