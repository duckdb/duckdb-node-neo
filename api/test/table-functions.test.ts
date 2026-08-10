import { assert, beforeAll, describe, expect, test } from 'vitest';
import {
  DuckDBConnection,
  DuckDBTableFunction,
  DuckDBTableFunctionBindInfo,
  DuckDBTableFunctionInfo,
  DuckDBTableFunctionInitInfo,
  INTEGER,
  VARCHAR,
} from '../src';
import { setDefaultTimezone, withConnection } from './util/testHelpers';

// A counter-driven scan, which most tests below vary one part of.
function counterFunction({
  name = 'my_func',
  rowCount = 3,
  bindFunction,
  initFunction,
  localInitFunction,
  mainFunction,
  ...rest
}: {
  name?: string;
  rowCount?: number;
  bindFunction?: (info: DuckDBTableFunctionBindInfo) => void;
  initFunction?: (info: DuckDBTableFunctionInitInfo) => void;
  localInitFunction?: (info: DuckDBTableFunctionInitInfo) => void;
  mainFunction?: (
    info: DuckDBTableFunctionInfo,
    output: import('../src').DuckDBDataChunk,
  ) => void;
  parameterTypes?: readonly import('../src').DuckDBType[];
  namedParameterTypes?: Readonly<Record<string, import('../src').DuckDBType>>;
  supportsProjectionPushdown?: boolean;
  extraInfo?: object;
} = {}): DuckDBTableFunction {
  return DuckDBTableFunction.create({
    name,
    bindFunction:
      bindFunction ??
      ((info) => {
        info.addResultColumn('my_column', VARCHAR);
        info.setBindData({ rowCount });
      }),
    initFunction:
      initFunction ??
      ((info) => {
        info.setInitData({ nextRow: 0 });
      }),
    localInitFunction,
    mainFunction:
      mainFunction ??
      ((info, output) => {
        const { rowCount: total } = info.bindData as { rowCount: number };
        const initData = info.initData as { nextRow: number };
        const remaining = total - initData.nextRow;
        // Row count first: sizing the chunk is what gives its vectors their
        // length.
        output.rowCount = remaining;
        if (remaining > 0) {
          const column = output.getColumnVector(0);
          for (let i = 0; i < remaining; i++) {
            column.setItem(i, `row_${initData.nextRow + i}`);
          }
          column.flush();
          initData.nextRow += remaining;
        }
      }),
    ...rest,
  });
}

describe('table functions', () => {
  beforeAll(setDefaultTimezone);

  test('table function (no parameters)', async () => {
    await withConnection(async (connection) => {
      connection.registerTableFunction(counterFunction());
      const reader = await connection.runAndReadAll('select * from my_func()');
      assert.deepEqual(reader.getColumnsObject(), {
        my_column: ['row_0', 'row_1', 'row_2'],
      });
    });
  });

  test('table function (positional and named parameters)', async () => {
    await withConnection(async (connection) => {
      connection.registerTableFunction(
        counterFunction({
          parameterTypes: [INTEGER],
          namedParameterTypes: { prefix: VARCHAR },
          bindFunction: (info) => {
            expect(info.parameterCount).toBe(1);
            const count = info.getParameter(0) as number;
            const prefix = (info.getNamedParameter('prefix') as string) ?? 'x';
            expect(info.getNamedParameter('not_supplied')).toBeNull();
            info.addResultColumn('my_column', VARCHAR);
            info.setCardinality(count, true);
            info.setBindData({ count, prefix });
          },
          mainFunction: (info, output) => {
            const { count, prefix } = info.bindData as {
              count: number;
              prefix: string;
            };
            const initData = info.initData as { nextRow: number };
            const remaining = initData.nextRow === 0 ? count : 0;
            output.rowCount = remaining;
            if (remaining > 0) {
              const column = output.getColumnVector(0);
              for (let i = 0; i < remaining; i++) {
                column.setItem(i, `${prefix}_${i}`);
              }
              column.flush();
              initData.nextRow = count;
            }
          },
        }),
      );
      const reader = await connection.runAndReadAll(
        "select * from my_func(2, prefix => 'row')",
      );
      assert.deepEqual(reader.getColumnsObject(), {
        my_column: ['row_0', 'row_1'],
      });
    });
  });

  test('table function (extra info, bind data and init data)', async () => {
    await withConnection(async (connection) => {
      const seen: string[] = [];
      connection.registerTableFunction(
        counterFunction({
          extraInfo: { token: 'extra_info' },
          bindFunction: (info) => {
            seen.push(`bind:${(info.extraInfo as { token: string }).token}`);
            expect(info.clientContext.connectionId).toBeGreaterThan(0n);
            info.addResultColumn('my_column', VARCHAR);
            info.setBindData({ token: 'bind_data' });
          },
          initFunction: (info) => {
            seen.push(`init:${(info.extraInfo as { token: string }).token}`);
            seen.push(
              `init_bind:${(info.bindData as { token: string }).token}`,
            );
            info.setInitData({ done: false });
          },
          localInitFunction: (info) => {
            info.setInitData({ token: 'local_init' });
          },
          mainFunction: (info, output) => {
            seen.push(`main:${(info.extraInfo as { token: string }).token}`);
            seen.push(
              `main_local:${(info.localInitData as { token: string }).token}`,
            );
            output.rowCount = 0;
          },
        }),
      );
      await connection.runAndReadAll('select * from my_func()');
      assert.deepEqual(seen, [
        'bind:extra_info',
        'init:extra_info',
        'init_bind:bind_data',
        'main:extra_info',
        'main_local:local_init',
      ]);
    });
  });

  test('table function (projection pushdown)', async () => {
    await withConnection(async (connection) => {
      let projected: number[] | undefined;
      connection.registerTableFunction(
        counterFunction({
          supportsProjectionPushdown: true,
          bindFunction: (info) => {
            info.addResultColumn('a', VARCHAR);
            info.addResultColumn('b', VARCHAR);
            info.addResultColumn('c', VARCHAR);
            info.setBindData({});
          },
          initFunction: (info) => {
            projected = info.getColumnIndexes();
            info.setInitData({});
          },
          mainFunction: (_info, output) => {
            output.rowCount = 0;
          },
        }),
      );
      await connection.runAndReadAll('select b from my_func()');
      assert.deepEqual(projected, [1]);
    });
  });

  test('table function (multiple chunks)', async () => {
    await withConnection(async (connection) => {
      const rowCount = 5000;
      connection.registerTableFunction(
        counterFunction({
          rowCount,
          mainFunction: (info, output) => {
            const { rowCount: total } = info.bindData as { rowCount: number };
            const initData = info.initData as { nextRow: number };
            const chunkSize = Math.min(2048, total - initData.nextRow);
            output.rowCount = chunkSize;
            if (chunkSize > 0) {
              const column = output.getColumnVector(0);
              for (let i = 0; i < chunkSize; i++) {
                column.setItem(i, `row_${initData.nextRow + i}`);
              }
              column.flush();
              initData.nextRow += chunkSize;
            }
          },
        }),
      );
      const reader = await connection.runAndReadAll(
        'select count(*) as n from my_func()',
      );
      assert.deepEqual(reader.getRows(), [[BigInt(rowCount)]]);
    });
  });

  test('table function (error from bind)', async () => {
    await withConnection(async (connection) => {
      connection.registerTableFunction(
        counterFunction({
          bindFunction: () => {
            throw new Error('my_bind_error');
          },
        }),
      );
      await expect(
        connection.runAndReadAll('select * from my_func()'),
      ).rejects.toThrow('my_bind_error');
    });
  });

  test('table function (error set from main)', async () => {
    await withConnection(async (connection) => {
      connection.registerTableFunction(
        counterFunction({
          mainFunction: (info) => {
            info.setError('my_main_error');
          },
        }),
      );
      await expect(
        connection.runAndReadAll('select * from my_func()'),
      ).rejects.toThrow('my_main_error');
    });
  });

  test('DuckDBConnection.create registers on its own connection', async () => {
    const connection = await DuckDBConnection.create();
    try {
      connection.registerTableFunction(counterFunction({ rowCount: 1 }));
      const reader = await connection.runAndReadAll('select * from my_func()');
      assert.deepEqual(reader.getColumnsObject(), { my_column: ['row_0'] });
    } finally {
      connection.closeSync();
    }
  });
});
