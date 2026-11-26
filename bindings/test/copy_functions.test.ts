import duckdb from '@duckdb/node-bindings';
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, relative } from 'node:path';
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

// A table function with everything a COPY FROM reader needs set.
function createReader(name: string): duckdb.TableFunction {
  const table_function = duckdb.create_table_function();
  duckdb.table_function_set_name(table_function, name);
  duckdb.table_function_add_parameter(table_function, duckdb.create_logical_type(duckdb.Type.VARCHAR));
  duckdb.table_function_set_bind(table_function, () => {});
  duckdb.table_function_set_init(table_function, () => {});
  duckdb.table_function_set_function(table_function, (_info, output) => {
    duckdb.data_chunk_set_size(output, 0);
  });
  return table_function;
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
    duckdb.copy_function_set_bind(copy_function, () => {});
    duckdb.destroy_copy_function_sync(copy_function);
    // Destroying twice is a no-op, not a crash.
    duckdb.destroy_copy_function_sync(copy_function);
    // Destroying freed the extra info along with the function, so the setters
    // refuse the handle instead of writing into freed memory.
    const destroyed = 'Copy function has been destroyed';
    expect(() => duckdb.copy_function_set_bind(copy_function, () => {})).toThrowError(destroyed);
    expect(() => duckdb.copy_function_set_sink(copy_function, () => {})).toThrowError(destroyed);
    expect(() => duckdb.copy_function_set_name(copy_function, 'my_format')).toThrowError(destroyed);
    expect(() => duckdb.copy_function_set_extra_info(copy_function, {})).toThrowError(destroyed);
  });
  test('register needs a name, and a sink or a COPY FROM table function', async () => {
    await withConnection(async (connection) => {
      const failure = 'Failed to register copy function';

      const unnamed = duckdb.create_copy_function();
      duckdb.copy_function_set_sink(unnamed, () => {});
      expect(() => duckdb.register_copy_function(connection, unnamed)).toThrowError(failure);

      // Bind, global init and finalize on their own are not enough.
      const neither = duckdb.create_copy_function();
      duckdb.copy_function_set_name(neither, 'my_format');
      duckdb.copy_function_set_bind(neither, () => {});
      duckdb.copy_function_set_global_init(neither, () => {});
      duckdb.copy_function_set_finalize(neither, () => {});
      expect(() => duckdb.register_copy_function(connection, neither)).toThrowError(failure);

      // A sink on its own is.
      const sink_only = duckdb.create_copy_function();
      duckdb.copy_function_set_name(sink_only, 'my_format');
      duckdb.copy_function_set_sink(sink_only, () => {});
      duckdb.register_copy_function(connection, sink_only);

      // So is a COPY FROM table function on its own.
      const from_only = duckdb.create_copy_function();
      duckdb.copy_function_set_name(from_only, 'my_format_from');
      duckdb.copy_function_set_copy_from_function(from_only, createReader('my_reader'));
      duckdb.register_copy_function(connection, from_only);
    });
  });
  test('a COPY FROM table function is copied as it is at the call', async () => {
    await withConnection(async (connection) => {
      const reader = duckdb.create_table_function();
      duckdb.table_function_set_name(reader, 'my_reader');
      duckdb.table_function_add_parameter(reader, duckdb.create_logical_type(duckdb.Type.VARCHAR));
      const copy_function = duckdb.create_copy_function();
      duckdb.copy_function_set_name(copy_function, 'my_format');

      // Without bind, init and main the call does nothing, and completing the
      // table function afterwards does not change that.
      duckdb.copy_function_set_copy_from_function(copy_function, reader);
      duckdb.table_function_set_bind(reader, () => {});
      duckdb.table_function_set_init(reader, () => {});
      duckdb.table_function_set_function(reader, (_info, output) => {
        duckdb.data_chunk_set_size(output, 0);
      });
      expect(() => duckdb.register_copy_function(connection, copy_function)).toThrowError(
        'Failed to register copy function',
      );

      // Set again now that it is complete.
      duckdb.copy_function_set_copy_from_function(copy_function, reader);
      duckdb.register_copy_function(connection, copy_function);
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

  test('an existing target file is replaced through a temporary path', async () => {
    await withTempDir(async (dir) => {
      await withConnection(async (connection) => {
        let initPath = '';
        registerWriter(connection, {
          globalInitFunction: (info) => {
            initPath = duckdb.copy_function_global_init_get_file_path(info);
            duckdb.copy_function_global_init_set_global_state(info, {
              'path': initPath,
              'values': [] as number[],
              'sink_calls': 0,
            });
          },
        });
        const path = join(dir, 'out.my_format');
        writeFileSync(path, 'old');
        await duckdb.query(
          connection,
          `COPY (SELECT range::INTEGER AS i FROM range(3)) TO '${path}' (FORMAT my_format)`,
        );
        // The writer got a temporary name next to the target, and DuckDB renamed
        // the file into place after finalize.
        expect(dirname(initPath)).toBe(dir);
        expect(basename(initPath)).not.toBe('out.my_format');
        expect(basename(initPath)).toMatch(/out\.my_format$/);
        expect(readdirSync(dir)).toEqual(['out.my_format']);
        expect(readFileSync(path, 'utf8')).toBe('1:0,1,2');
      });
    });
  });

  test('PARTITION_BY runs global init and finalize once per partition', async () => {
    await withTempDir(async (dir) => {
      await withConnection(async (connection) => {
        const events: string[] = [];
        registerWriter(connection, {
          bindFunction: (info) => {
            events.push('bind');
            duckdb.copy_function_bind_set_bind_data(info, {
              'column_count': duckdb.copy_function_bind_get_column_count(info),
            });
          },
          globalInitFunction: (info) => {
            const path = duckdb.copy_function_global_init_get_file_path(info);
            events.push(`init ${dirname(relative(dir, path))}`);
            duckdb.copy_function_global_init_set_global_state(info, {
              'path': path,
              'values': [] as number[],
              'sink_calls': 0,
            });
          },
          finalizeFunction: (info) => {
            const state = duckdb.copy_function_finalize_get_global_state(info) as {
              path: string;
              values: number[];
            };
            events.push(`finalize ${dirname(relative(dir, state.path))}`);
            writeFileSync(state.path, state.values.sort((a, b) => a - b).join(','));
          },
        });
        const root = join(dir, 'out');
        await duckdb.query(
          connection,
          `COPY (SELECT range % 2 AS k, range::INTEGER AS i FROM range(6)) TO '${root}' (FORMAT my_format, PARTITION_BY (k))`,
        );
        // One bind, then an init and a finalize for each partition's file. The
        // partition column itself is not written.
        expect(events.sort()).toEqual([
          'bind',
          'finalize out/k=0',
          'finalize out/k=1',
          'init out/k=0',
          'init out/k=1',
        ]);
        const [file0] = readdirSync(join(root, 'k=0'));
        const [file1] = readdirSync(join(root, 'k=1'));
        expect(readFileSync(join(root, 'k=0', file0), 'utf8')).toBe('0,2,4');
        expect(readFileSync(join(root, 'k=1', file1), 'utf8')).toBe('1,3,5');
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
                case duckdb.Type.STRUCT: {
                  // Values of different types come as an unnamed STRUCT.
                  const valueType = duckdb.get_value_type(value);
                  const members: string[] = [];
                  for (let j = 0; j < duckdb.struct_type_child_count(valueType); j++) {
                    members.push(`${duckdb.struct_type_child_name(valueType, j)}:${duckdb.Type[duckdb.get_type_id(duckdb.struct_type_child_type(valueType, j))]}`);
                  }
                  seen.options[name] = `struct(${members.join(',')})`;
                  break;
                }
              }
            }
          },
        });
        const path = join(dir, 'out.my_format');
        await duckdb.query(
          connection,
          `COPY (SELECT 1::INTEGER AS i, 'a' AS s, true AS b) TO '${path}' (FORMAT my_format, token 'abc', count 7, flags ('x', 'y'), single ('z'), mixed (1, 'a'))`,
        );
        expect(seen.columns).toEqual(['INTEGER', 'VARCHAR', 'BOOLEAN']);
        expect(seen.options).toEqual({
          TOKEN: 'abc',
          COUNT: '7',
          FLAGS: 'x|y',
          SINGLE: 'z',
          MIXED: 'struct(:INTEGER,:VARCHAR)',
        });
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
        const ids: number[] = [];
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
