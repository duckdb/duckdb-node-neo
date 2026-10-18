import { assert, describe, test } from 'vitest';
import {
  DuckDBDataChunk,
  DuckDBListValue,
  DuckDBStructType,
  DuckDBStructValue,
  DuckDBTupleValue,
  DuckDBTupleVector,
  DuckDBTypeId,
  INTEGER,
  JSDuckDBValueConverter,
  JSToDuckDBValueConverter,
  JsonDuckDBValueConverter,
  LIST,
  STRUCT,
  TUPLE,
  VARCHAR,
  jsToDuckDBValue,
  listValue,
  structValue,
  tupleValue,
} from '../src';
import { DuckDBScalarFunction } from '../src/DuckDBScalarFunction';
import { typeForValue } from '../src/typeForValue';
import { withConnection } from './util/testHelpers';

const INT_VARCHAR = TUPLE([INTEGER, VARCHAR]);

function chunkRowsJS(chunk: DuckDBDataChunk) {
  const types = Array.from({ length: chunk.columnCount }, (_, i) =>
    chunk.getColumnVector(i).type
  );
  return chunk
    .getRows()
    .map((row) =>
      row.map((value, i) =>
        value === null
          ? null
          : JSDuckDBValueConverter(value, types[i], JSDuckDBValueConverter)
      )
    );
}

describe('tuple type', () => {
  test('a STRUCT whose entry names are all empty is a tuple', () => {
    assert.isTrue(INT_VARCHAR.isTuple);
    assert.deepEqual(INT_VARCHAR.entryNames, ['', '']);
    assert.isFalse(STRUCT({ a: INTEGER, b: VARCHAR }).isTuple);
    assert.isFalse(STRUCT({}).isTuple);
  });

  test('toString', () => {
    assert.equal(INT_VARCHAR.toString(), 'TUPLE(INTEGER, VARCHAR)');
    assert.equal(
      LIST(TUPLE([INTEGER, TUPLE([VARCHAR])])).toString(),
      'TUPLE(INTEGER, TUPLE(VARCHAR))[]'
    );
    assert.equal(
      STRUCT({ a: INTEGER, b: VARCHAR }).toString(),
      'STRUCT("a" INTEGER, "b" VARCHAR)'
    );
  });

  test('toJson keeps the empty entry names', () => {
    assert.deepEqual(INT_VARCHAR.toJson(), {
      typeId: DuckDBTypeId.STRUCT,
      entryNames: ['', ''],
      entryTypes: [{ typeId: DuckDBTypeId.INTEGER }, { typeId: DuckDBTypeId.VARCHAR }],
    });
  });

  test('read from a query result', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(`select (1, 'a') as t`);
      const type = reader.columnTypes()[0];
      assert.instanceOf(type, DuckDBStructType);
      assert.isTrue((type as DuckDBStructType).isTuple);
      assert.equal(type.toString(), 'TUPLE(INTEGER, VARCHAR)');
    });
  });
});

describe('tuple values', () => {
  test('toString', () => {
    assert.equal(tupleValue([1, 'a', null]).toString(), `(1, 'a', NULL)`);
  });

  test('typeForValue infers a tuple type', () => {
    const type = typeForValue(tupleValue([1, 'a']));
    assert.instanceOf(type, DuckDBStructType);
    assert.isTrue((type as DuckDBStructType).isTuple);
    assert.equal(type.toString(), 'TUPLE(INTEGER, VARCHAR)');
  });
});

describe('reading tuples', () => {
  for (const sql of [`select (1, 'a') as t`, `select row(1, 'a') as t`]) {
    test(sql, async () => {
      await withConnection(async (connection) => {
        const reader = await connection.runAndReadAll(sql);
        const value = reader.getRows()[0][0];
        assert.instanceOf(value, DuckDBTupleValue);
        assert.deepEqual((value as DuckDBTupleValue).items, [1, 'a']);
        assert.deepEqual(reader.getRowsJS(), [[[1, 'a']]]);
        assert.deepEqual(reader.getRowsJson(), [[[1, 'a']]]);
        assert.deepEqual(reader.getRowObjectsJS(), [{ t: [1, 'a'] }]);
      });
    });
  }

  test('a NULL tuple and NULL items', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `select t from (values ((1, 'a')), (null), ((null, null))) v(t)`
      );
      assert.deepEqual(reader.getRowsJS(), [[[1, 'a']], [null], [[null, null]]]);
    });
  });

  test('a tuple inside a LIST', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `select [(1, 'a'), (2, 'b')] as l`
      );
      const value = reader.getRows()[0][0] as DuckDBListValue;
      assert.deepEqual(
        value.items.map((item) => (item as DuckDBTupleValue).items),
        [
          [1, 'a'],
          [2, 'b'],
        ]
      );
      assert.equal(value.toString(), `[(1, 'a'), (2, 'b')]`);
      const expected = [
        [
          [1, 'a'],
          [2, 'b'],
        ],
      ];
      assert.deepEqual(reader.getRowsJS(), [expected]);
      assert.deepEqual(reader.getRowsJson(), [expected]);
    });
  });

  test('a tuple inside a STRUCT, and a STRUCT inside a tuple', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `select {'x': (1, 'a')} as s, (2, {'y': 'b'}) as t`
      );
      const [s, t] = reader.getRows()[0];
      assert.instanceOf(s, DuckDBStructValue);
      assert.deepEqual(
        ((s as DuckDBStructValue).entries.x as DuckDBTupleValue).items,
        [1, 'a']
      );
      assert.instanceOf(t, DuckDBTupleValue);
      assert.equal(t!.toString(), `(2, {'y': 'b'})`);
      assert.deepEqual(reader.getRowsJS(), [[{ x: [1, 'a'] }, [2, { y: 'b' }]]]);
      assert.deepEqual(reader.getRowsJson(), [
        [{ x: [1, 'a'] }, [2, { y: 'b' }]],
      ]);
    });
  });

  test('a tuple inside a tuple', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `select (1, (2, 'b')) as t`
      );
      assert.equal(
        reader.columnTypes()[0].toString(),
        'TUPLE(INTEGER, TUPLE(INTEGER, VARCHAR))'
      );
      assert.deepEqual(reader.getRowsJS(), [[[1, [2, 'b']]]]);
    });
  });

  test('the converters convert tuples to arrays', () => {
    const value = tupleValue([1, listValue(['a'])]);
    const type = TUPLE([INTEGER, LIST(VARCHAR)]);
    assert.deepEqual(
      JSDuckDBValueConverter(value, type, JSDuckDBValueConverter),
      [1, ['a']]
    );
    assert.deepEqual(
      JsonDuckDBValueConverter(value, type, JsonDuckDBValueConverter),
      [1, ['a']]
    );
  });
});

describe('writing tuples', () => {
  test('bindTuple', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(
        'select $1 as t, $1[1] as a, $1[2] as b'
      );
      prepared.bindTuple(1, [1, 'a'], INT_VARCHAR);
      const reader = await prepared.runAndReadAll();
      assert.deepEqual(reader.getRowsJS(), [[[1, 'a'], 1, 'a']]);
    });
  });

  test('bind with an inferred type', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare('select $1 as t');
      prepared.bind([tupleValue([1, 'a'])]);
      const reader = await prepared.runAndReadAll();
      assert.equal(
        reader.columnTypes()[0].toString(),
        'TUPLE(INTEGER, VARCHAR)'
      );
      assert.deepEqual(reader.getRowsJS(), [[[1, 'a']]]);
    });
  });

  test('bind a tuple inside a LIST and a STRUCT', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare('select $1 as l, $2 as s');
      prepared.bindList(
        1,
        listValue([tupleValue([1, 'a']), tupleValue([2, 'b'])]),
        LIST(INT_VARCHAR)
      );
      prepared.bindStruct(
        2,
        structValue({ x: tupleValue([3, 'c']) }),
        STRUCT({ x: INT_VARCHAR })
      );
      const reader = await prepared.runAndReadAll();
      assert.deepEqual(reader.getRowsJS(), [
        [
          [
            [1, 'a'],
            [2, 'b'],
          ],
          { x: [3, 'c'] },
        ],
      ]);
    });
  });

  test('a struct value is refused for a tuple type, and vice versa', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare('select $1');
      assert.throws(
        () => prepared.bindValue(1, structValue({ '': 1 }), INT_VARCHAR),
        'input is not a DuckDBTupleValue'
      );
      assert.throws(
        () =>
          prepared.bindValue(
            1,
            tupleValue([1, 'a']),
            STRUCT({ a: INTEGER, b: VARCHAR })
          ),
        'input is not a DuckDBStructValue'
      );
      assert.throws(
        () => prepared.bindTuple(1, [1], INT_VARCHAR),
        'Expected 2 items for TUPLE(INTEGER, VARCHAR), got 1'
      );
    });
  });

  test('appendTuple to a named STRUCT column', async () => {
    await withConnection(async (connection) => {
      await connection.run('create table t (s struct(a integer, b varchar))');
      const appender = await connection.createAppender('t');
      appender.appendTuple([1, 'a'], INT_VARCHAR);
      appender.endRow();
      appender.closeSync();
      const reader = await connection.runAndReadAll('select s from t');
      assert.deepEqual(reader.getRowsJS(), [[{ a: 1, b: 'a' }]]);
    });
  });

  test('data chunk', () => {
    const type = LIST(INT_VARCHAR);
    const chunk = DuckDBDataChunk.create([INT_VARCHAR, type], 2);
    chunk.setColumnValues(0, [tupleValue([1, 'a']), null]);
    chunk.setColumnValues(1, [
      listValue([tupleValue([2, 'b']), tupleValue([3, 'c'])]),
      listValue([]),
    ]);
    assert.instanceOf(chunk.getColumnVector(0), DuckDBTupleVector);
    assert.deepEqual(chunkRowsJS(chunk), [
      [
        [1, 'a'],
        [
          [2, 'b'],
          [3, 'c'],
        ],
      ],
      [null, []],
    ]);
    assert.throws(
      () => chunk.setColumnValues(0, [structValue({ '': 1 }), null]),
      'Expected DuckDBTupleValue for TUPLE(INTEGER, VARCHAR)'
    );
  });

  test('scalar function returning a tuple', async () => {
    await withConnection(async (connection) => {
      connection.registerScalarFunction(
        DuckDBScalarFunction.create({
          name: 'make_tuple',
          mainFunction: (_info, input, output) => {
            const n = input.getColumnVector(0);
            for (let rowIndex = 0; rowIndex < input.rowCount; rowIndex++) {
              const item = n.getItem(rowIndex);
              output.setItem(rowIndex, tupleValue([item, `r${item}`]));
            }
            output.flush();
          },
          parameterTypes: [INTEGER],
          returnType: INT_VARCHAR,
        })
      );
      const reader = await connection.runAndReadAll(
        'select make_tuple(i::integer) as t from range(2) r(i)'
      );
      assert.deepEqual(reader.getRowsJS(), [[[0, 'r0']], [[1, 'r1']]]);
    });
  });

  test('jsToDuckDBValue accepts arrays for tuple types', () => {
    const value = jsToDuckDBValue([1, 'a'], INT_VARCHAR);
    assert.instanceOf(value, DuckDBTupleValue);
    assert.deepEqual((value as DuckDBTupleValue).items, [1, 'a']);
    assert.throws(
      () => jsToDuckDBValue({ '': 1 }, INT_VARCHAR),
      'Expected an array for TUPLE(INTEGER, VARCHAR)'
    );
    assert.throws(
      () => jsToDuckDBValue([1, 'a'], STRUCT({ a: INTEGER, b: VARCHAR })),
      'Expected an object for STRUCT("a" INTEGER, "b" VARCHAR)'
    );
  });

  test('JS round trip through setRowsConverted', () => {
    const chunk = DuckDBDataChunk.create([LIST(INT_VARCHAR)], 1);
    chunk.setRowsConverted([[[[1, 'a'], [2, 'b']]]], JSToDuckDBValueConverter);
    assert.deepEqual(chunkRowsJS(chunk), [
      [
        [
          [1, 'a'],
          [2, 'b'],
        ],
      ],
    ]);
  });
});
