import {
  ARRAY,
  BIGINT,
  BIT,
  bitValue,
  BLOB,
  BOOLEAN,
  DATE,
  DECIMAL,
  DOUBLE,
  DuckDBType,
  ENUM8,
  FLOAT,
  HUGEINT,
  INTEGER,
  INTERVAL,
  JS,
  LIST,
  MAP,
  SMALLINT,
  STRUCT,
  TIME,
  TIMESTAMP,
  TIMESTAMP_MS,
  TIMESTAMP_NS,
  TIMESTAMP_S,
  TIMESTAMPTZ,
  TIMETZ,
  TINYINT,
  UBIGINT,
  UHUGEINT,
  UINTEGER,
  UNION,
  USMALLINT,
  UTINYINT,
  UUID,
  VARCHAR,
  VARINT,
} from '../../src';
import { bytesFromString } from '../../src/conversion/bytesFromString';

const smallEnumValues = ['DUCK_DUCK_ENUM', 'GOOSE'];

export interface ColumnData {
  readonly name: string;
  readonly type: DuckDBType;
  readonly valuesStr: string[];
  readonly valuesJS: readonly JS[];
}

function col(
  name: string,
  type: DuckDBType,
  valuesStr: string[],
  valuesJS: readonly JS[],
): ColumnData {
  return { name, type, valuesStr, valuesJS };
}

export function createTestJSData(): ColumnData[] {
  return [
    col('bool', BOOLEAN, ['false', 'true', 'null'], [false, true, null]),
    col(
      'tinyint',
      TINYINT,
      [String(TINYINT.min), String(TINYINT.max), 'null'],
      [TINYINT.min, TINYINT.max, null],
    ),
    col(
      'smallint',
      SMALLINT,
      [String(SMALLINT.min), String(SMALLINT.max), 'null'],
      [SMALLINT.min, SMALLINT.max, null],
    ),
    col(
      'int',
      INTEGER,
      [String(INTEGER.min), String(INTEGER.max), 'null'],
      [INTEGER.min, INTEGER.max, null],
    ),
    col(
      'bigint',
      BIGINT,
      [String(BIGINT.min), String(BIGINT.max), 'null'],
      [BIGINT.min, BIGINT.max, null],
    ),
    col(
      'hugeint',
      HUGEINT,
      [String(HUGEINT.min), String(HUGEINT.max), 'null'],
      [HUGEINT.min, HUGEINT.max, null],
    ),
    col(
      'uhugeint',
      UHUGEINT,
      [String(UHUGEINT.min), String(UHUGEINT.max), 'null'],
      [UHUGEINT.min, UHUGEINT.max, null],
    ),
    col(
      'utinyint',
      UTINYINT,
      [String(UTINYINT.min), String(UTINYINT.max), 'null'],
      [UTINYINT.min, UTINYINT.max, null],
    ),
    col(
      'usmallint',
      USMALLINT,
      [String(USMALLINT.min), String(USMALLINT.max), 'null'],
      [USMALLINT.min, USMALLINT.max, null],
    ),
    col(
      'uint',
      UINTEGER,
      [String(UINTEGER.min), String(UINTEGER.max), 'null'],
      [UINTEGER.min, UINTEGER.max, null],
    ),
    col(
      'ubigint',
      UBIGINT,
      [String(UBIGINT.min), String(UBIGINT.max), 'null'],
      [UBIGINT.min, UBIGINT.max, null],
    ),
    col(
      'varint',
      VARINT,
      [String(VARINT.min), String(VARINT.max), 'null'],
      [VARINT.min, VARINT.max, null],
    ),
    col(
      'date',
      DATE,
      [`'-271821-04-20'`, `'275760-09-13'`, 'null'],
      [new Date('-271821-04-20'), new Date('+275760-09-13'), null],
    ),
    col(
      'time',
      TIME,
      [`'${TIME.min}'`, `'${TIME.max}'`, 'null'],
      [TIME.min.micros, TIME.max.micros, null],
    ),
    col(
      'timestamp',
      TIMESTAMP,
      [
        `'-271821-04-20T00:00:00.000000'`,
        `'275760-09-13T00:00:00.000000'`,
        'null',
      ],
      [
        new Date('-271821-04-20T00:00:00.000Z'),
        new Date('+275760-09-13T00:00:00.000Z'),
        null,
      ],
    ),
    col(
      'timestamp_s',
      TIMESTAMP_S,
      [`'-271821-04-20T00:00:00'`, `'275760-09-13T00:00:00'`, 'null'],
      [
        new Date('-271821-04-20T00:00:00Z'),
        new Date('+275760-09-13T00:00:00Z'),
        null,
      ],
    ),
    col(
      'timestamp_ms',
      TIMESTAMP_MS,
      [`'-271821-04-20T00:00:00.000'`, `'275760-09-13T00:00:00.000'`, 'null'],
      [
        new Date('-271821-04-20T00:00:00.000Z'),
        new Date('+275760-09-13T00:00:00.000Z'),
        null,
      ],
    ),
    col(
      'timestamp_ns',
      TIMESTAMP_NS,
      [
        `'1677-09-22T00:00:00.000000000'`,
        `'2262-04-11 23:47:16.854775806'`,
        'null',
      ],
      [
        new Date('1677-09-22T00:00:00.000Z'),
        new Date('+2262-04-11 23:47:16.854Z'),
        null,
      ],
    ),
    col(
      'time_tz',
      TIMETZ,
      [`'${TIMETZ.min}'`, `'${TIMETZ.max}'`, 'null'],
      [
        { micros: TIMETZ.min.micros, offset: TIMETZ.min.offset },
        { micros: TIMETZ.max.micros, offset: TIMETZ.max.offset },
        null,
      ],
    ),
    col(
      'timestamp_tz',
      TIMESTAMPTZ,
      [
        `'-271821-04-20T00:00:00.000000+00'`,
        `'275760-09-13T00:00:00.000000+00'`,
        'null',
      ],
      [
        new Date('-271821-04-20T00:00:00.000Z'),
        new Date('+275760-09-13T00:00:00.000Z'),
        null,
      ],
    ),
    col(
      'float',
      FLOAT,
      [String(FLOAT.min), String(FLOAT.max), 'null'],
      [FLOAT.min, FLOAT.max, null],
    ),
    col(
      'double',
      DOUBLE,
      [String(DOUBLE.min), String(DOUBLE.max), 'null'],
      [DOUBLE.min, DOUBLE.max, null],
    ),
    col(
      'dec_4_1',
      DECIMAL(4, 1),
      ['-999.9', '999.9', 'null'],
      [-999.9, 999.9, null],
    ),
    col(
      'dec_9_4',
      DECIMAL(9, 4),
      ['-99999.9999', '99999.9999', 'null'],
      [-99999.9999, 99999.9999, null],
    ),
    col(
      'dec_18_6',
      DECIMAL(18, 6),
      ['-999999999999.999999', '999999999999.999999', 'null'],
      [-999999999999.999999, 999999999999.999999, null],
    ),
    col(
      'dec38_10',
      DECIMAL(38, 10),
      [
        '-9999999999999999999999999999.9999999999',
        '9999999999999999999999999999.9999999999',
        'null',
      ],
      [
        -9999999999999999999999999999.9999999999,
        9999999999999999999999999999.9999999999,
        null,
      ],
    ),
    col(
      'uuid',
      UUID,
      [`'${UUID.min}'`, `'${UUID.max}'`, 'null'],
      [String(UUID.min), String(UUID.max), null],
    ),
    col(
      'interval',
      INTERVAL,
      [
        `'0 months 0 days 0 microseconds'`,
        `'999 months 999 days 999999999 microseconds'`,
        'null',
      ],
      [
        { months: 0, days: 0, micros: 0n },
        { months: 999, days: 999, micros: 999999999n },
        null,
      ],
    ),
    col(
      'varchar',
      VARCHAR,
      [`'🦆🦆🦆🦆🦆🦆'`, `'goo\\0se'`, 'null'],
      ['🦆🦆🦆🦆🦆🦆', 'goo\\0se', null],
    ),
    col(
      'blob',
      BLOB,
      [`'thisisalongblob\\x00withnullbytes'`, `'\\x00\\x00\\x00a'`, 'null'],
      [
        bytesFromString('thisisalongblob\x00withnullbytes'),
        bytesFromString('\x00\x00\x00a'),
        null,
      ],
    ),
    col(
      'bit',
      BIT,
      [`'0010001001011100010101011010111'`, `'10101'`, 'null'],
      [
        bitValue('0010001001011100010101011010111').data,
        bitValue('10101').data,
        null,
      ],
    ),
    col(
      'small_enum',
      ENUM8(smallEnumValues),
      [
        `'${smallEnumValues[0]}'`,
        `'${smallEnumValues[smallEnumValues.length - 1]}'`,
        'null',
      ],
      [smallEnumValues[0], smallEnumValues[smallEnumValues.length - 1], null],
    ),
    col(
      'int_array',
      LIST(INTEGER),
      [`[]`, `[42, 999, null, null, -42]`, 'null'],
      [[], [42, 999, null, null, -42], null],
    ),
    col(
      'double_array',
      LIST(DOUBLE),
      [
        `[]`,
        `[42.0::double, 'NaN', 'Infinity', '-Infinity', null, -42.0]`,
        'null',
      ],
      [[], [42, NaN, Infinity, -Infinity, null, -42], null],
    ),
    col(
      'date_array',
      LIST(DATE),
      [
        `[]`,
        `[
          '1970-01-01'::date,
          '275760-09-13',
          '-271821-04-20',
          null,
          '2022-05-12',
        ]`,
        'null',
      ],
      [
        [],
        [
          new Date('1970-01-01'),
          new Date('+275760-09-13'),
          new Date('-271821-04-20'),
          null,
          new Date('2022-05-12'),
        ],
        null,
      ],
    ),
    col(
      'timestamp_array',
      LIST(TIMESTAMP),
      [
        `[]`,
        `[
          '1970-01-01T00:00:00.000000',
          '275760-09-13T00:00:00.000000',
          '-271821-04-20T00:00:00.000000',
          null,
          '2022-05-12T16:23:45.000000'
        ]`,
        'null',
      ],
      [
        [],
        [
          new Date('1970-01-01T00:00:00.000000Z'),
          new Date('+275760-09-13T00:00:00.000000Z'),
          new Date('-271821-04-20T00:00:00.000000Z'),
          null,
          new Date('2022-05-12T16:23:45.000000Z'),
        ],
        null,
      ],
    ),
    col(
      'timestamptz_array',
      LIST(TIMESTAMPTZ),
      [
        `[]`,
        `[
          '1970-01-01T00:00:00.000000Z',
          '275760-09-13T00:00:00.000000Z',
          '-271821-04-20T00:00:00.000000Z',
          null,
          '2022-05-12T16:23:45.000000Z'
        ]`,
        'null',
      ],
      [
        [],
        [
          new Date('1970-01-01T00:00:00.000000Z'),
          new Date('+275760-09-13T00:00:00.000000Z'),
          new Date('-271821-04-20T00:00:00.000000Z'),
          null,
          new Date('2022-05-12T16:23:45.000000Z'),
        ],
        null,
      ],
    ),
    col(
      'varchar_array',
      LIST(VARCHAR),
      [`[]`, `['🦆🦆🦆🦆🦆🦆', 'goose', null, '']`, 'null'],
      [[], ['🦆🦆🦆🦆🦆🦆', 'goose', null, ''], null],
    ),
    col(
      'nested_int_array',
      LIST(LIST(INTEGER)),
      [
        `[]`,
        `[
          [],
          [42, 999, null, null, -42],
          null,
          [],
          [42, 999, null, null, -42],
        ]`,
        'null',
      ],
      [
        [],
        [[], [42, 999, null, null, -42], null, [], [42, 999, null, null, -42]],
        null,
      ],
    ),
    col(
      'struct',
      STRUCT({ a: INTEGER, b: VARCHAR }),
      [`{ 'a': null, 'b': null }`, `{ 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' }`, 'null'],
      [{ a: null, b: null }, { a: 42, b: '🦆🦆🦆🦆🦆🦆' }, null],
    ),
    col(
      'struct_of_arrays',
      STRUCT({ a: LIST(INTEGER), b: LIST(VARCHAR) }),
      [
        `{ 'a': null, 'b': null }`,
        `{
          'a': [42, 999, null, null, -42],
          'b': ['🦆🦆🦆🦆🦆🦆', 'goose', null, ''],
        }`,
        'null',
      ],
      [
        { a: null, b: null },
        {
          a: [42, 999, null, null, -42],
          b: ['🦆🦆🦆🦆🦆🦆', 'goose', null, ''],
        },
        null,
      ],
    ),
    col(
      'array_of_structs',
      LIST(STRUCT({ a: INTEGER, b: VARCHAR })),
      [
        `[]`,
        `[
          { 'a': null, 'b': null },
          { 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' },
          null,
        ]`,
        'null',
      ],
      [[], [{ a: null, b: null }, { a: 42, b: '🦆🦆🦆🦆🦆🦆' }, null], null],
    ),
    col(
      'map',
      MAP(VARCHAR, VARCHAR),
      [
        `MAP([], [])`,
        `MAP { 'key1': '🦆🦆🦆🦆🦆🦆', 'key2': 'goose' }`,
        'null',
      ],
      [
        [],
        [
          { key: 'key1', value: '🦆🦆🦆🦆🦆🦆' },
          { key: 'key2', value: 'goose' },
        ],
        null,
      ],
    ),
    col(
      'union',
      UNION({ name: VARCHAR, age: SMALLINT }),
      [`union_value(name => 'Frank')`, `union_value(age => 5)`, 'null'],
      [{ tag: 'name', value: 'Frank' }, { tag: 'age', value: 5 }, null],
    ),
    col(
      'fixed_int_array',
      ARRAY(INTEGER, 3),
      [`[null, 2, 3]`, `[4, 5, 6]`, 'null'],
      [[null, 2, 3], [4, 5, 6], null],
    ),
    col(
      'fixed_varchar_array',
      ARRAY(VARCHAR, 3),
      [`['a', null, 'c']`, `['d', 'e', 'f']`, 'null'],
      [['a', null, 'c'], ['d', 'e', 'f'], null],
    ),
    col(
      'fixed_nested_int_array',
      ARRAY(ARRAY(INTEGER, 3), 3),
      [
        `[[null, 2, 3], null, [null, 2, 3]]`,
        `[[4, 5, 6], [null, 2, 3], [4, 5, 6]]`,
        'null',
      ],
      [
        [[null, 2, 3], null, [null, 2, 3]],
        [
          [4, 5, 6],
          [null, 2, 3],
          [4, 5, 6],
        ],
        null,
      ],
    ),
    col(
      'fixed_nested_varchar_array',
      ARRAY(ARRAY(VARCHAR, 3), 3),
      [
        `[
          ['a', null, 'c'],
          null,
          ['a', null, 'c'],
        ]`,
        `[
          ['d', 'e', 'f'],
          ['a', null, 'c'],
          ['d', 'e', 'f'],
        ]`,
        'null',
      ],
      [
        [['a', null, 'c'], null, ['a', null, 'c']],
        [
          ['d', 'e', 'f'],
          ['a', null, 'c'],
          ['d', 'e', 'f'],
        ],
        null,
      ],
    ),
    col(
      'fixed_struct_array',
      ARRAY(STRUCT({ a: INTEGER, b: VARCHAR }), 3),
      [
        `[
          { 'a': null, 'b': null },
          { 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' },
          { 'a': null, 'b': null },
        ]`,
        `[
          { 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' },
          { 'a': null, 'b': null },
          { 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' },
        ]`,
        'null',
      ],
      [
        [
          { a: null, b: null },
          { a: 42, b: '🦆🦆🦆🦆🦆🦆' },
          { a: null, b: null },
        ],
        [
          { a: 42, b: '🦆🦆🦆🦆🦆🦆' },
          { a: null, b: null },
          { a: 42, b: '🦆🦆🦆🦆🦆🦆' },
        ],
        null,
      ],
    ),
    col(
      'struct_of_fixed_array',
      STRUCT({ a: ARRAY(INTEGER, 3), b: ARRAY(VARCHAR, 3) }),
      [
        `{
          'a': [null, 2, 3],
          'b': ['a', null, 'c'],
        }`,
        `{
          'a': [4, 5, 6],
          'b': ['d', 'e', 'f'],
        }`,
        'null',
      ],
      [
        { a: [null, 2, 3], b: ['a', null, 'c'] },
        { a: [4, 5, 6], b: ['d', 'e', 'f'] },
        null,
      ],
    ),
    col(
      'fixed_array_of_int_list',
      ARRAY(LIST(INTEGER), 3),
      [
        `[
          [],
          [42, 999, null, null, -42],
          [],
        ]`,
        `[
          [42, 999, null, null, -42],
          [],
          [42, 999, null, null, -42],
        ]`,
        'null',
      ],
      [
        [[], [42, 999, null, null, -42], []],
        [[42, 999, null, null, -42], [], [42, 999, null, null, -42]],
        null,
      ],
    ),
    col(
      'list_of_fixed_int_array',
      LIST(ARRAY(INTEGER, 3)),
      [
        `[
          [null, 2, 3],
          [4, 5, 6],
          [null, 2, 3],
        ]`,
        `[
          [4, 5, 6],
          [null, 2, 3],
          [4, 5, 6],
        ]`,
        'null',
      ],
      [
        [
          [null, 2, 3],
          [4, 5, 6],
          [null, 2, 3],
        ],
        [
          [4, 5, 6],
          [null, 2, 3],
          [4, 5, 6],
        ],
        null,
      ],
    ),
  ];
}

export function createTestJSValuesClause(): string {
  const data = createTestJSData();
  return `VALUES ${[
    `(${data
      .map(({ type, valuesStr }) => `(${valuesStr[0]})::${type}`)
      .join(', ')})`,
    `(${data
      .map(({ type, valuesStr }) => `(${valuesStr[1]})::${type}`)
      .join(', ')})`,
    `(${data
      .map(({ type, valuesStr }) => `(${valuesStr[2]})::${type}`)
      .join(', ')})`,
  ].join(', ')}`;
}

export function createTestJSColumnNames(): readonly string[] {
  return createTestJSData().map(({ name }) => `"${name}"`);
}

export function createTestJSQuery(): string {
  return `select * from (${createTestJSValuesClause()}) t(${createTestJSColumnNames().join(',')})`;
}

export function createTestJSColumnsJS(): readonly (readonly JS[])[] {
  return createTestJSData().map(({ valuesJS }) => valuesJS);
}

export function createTestJSColumnsObjectJS(): Record<string, readonly JS[]> {
  const columnsObject: Record<string, readonly JS[]> = {};
  const data = createTestJSData();
  for (const columnData of data) {
    columnsObject[columnData.name] = columnData.valuesJS;
  }
  return columnsObject;
}

export function createTestJSRowsJS(): readonly (readonly JS[])[] {
  const data = createTestJSData();
  return [
    data.map(({ valuesJS }) => valuesJS[0]),
    data.map(({ valuesJS }) => valuesJS[1]),
    data.map(({ valuesJS }) => valuesJS[2]),
  ];
}

export function createTestJSRowObjectsJS(): readonly Record<string, JS>[] {
  const rowObjects: Record<string, JS>[] = [{}, {}, {}];
  const data = createTestJSData();
  for (const columnData of data) {
    rowObjects[0][columnData.name] = columnData.valuesJS[0];
    rowObjects[1][columnData.name] = columnData.valuesJS[1];
    rowObjects[2][columnData.name] = columnData.valuesJS[2];
  }
  return rowObjects;
}
