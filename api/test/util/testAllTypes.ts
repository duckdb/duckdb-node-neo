import {
  ARRAY,
  arrayValue,
  BIGINT,
  BIGNUM,
  BIT,
  bitValue,
  BLOB,
  blobValue,
  BOOLEAN,
  DATE,
  dateValue,
  DECIMAL,
  decimalValue,
  DOUBLE,
  DuckDBType,
  DuckDBValue,
  ENUM16,
  ENUM32,
  ENUM8,
  FLOAT,
  HUGEINT,
  INTEGER,
  INTERVAL,
  intervalValue,
  Json,
  LIST,
  listValue,
  MAP,
  mapValue,
  SMALLINT,
  STRUCT,
  structValue,
  TIME,
  TIMESTAMP,
  TIMESTAMP_MS,
  TIMESTAMP_NS,
  TIMESTAMP_S,
  TIMESTAMPTZ,
  timestampTZValue,
  timestampValue,
  TIMETZ,
  TINYINT,
  UBIGINT,
  UHUGEINT,
  UINTEGER,
  UNION,
  unionValue,
  USMALLINT,
  UTINYINT,
  UUID,
  VARCHAR,
} from '../../src';

const BI_10_8 = 100000000n;
const BI_10_10 = 10000000000n;
const BI_18_9s = BI_10_8 * BI_10_10 - 1n;
const BI_38_9s = BI_10_8 * BI_10_10 * BI_10_10 * BI_10_10 - 1n;

const smallEnumValues = ['DUCK_DUCK_ENUM', 'GOOSE'];
const mediumEnumValues = Array.from({ length: 300 }).map((_, i) => `enum_${i}`);
const largeEnumValues = Array.from({ length: 70000 }).map(
  (_, i) => `enum_${i}`
);

export interface ColumnNameAndType {
  readonly name: string;
  readonly type: DuckDBType;
}

export interface ColumnNameTypeAndValues extends ColumnNameAndType {
  readonly name: string;
  readonly type: DuckDBType;
  readonly typeJson: Json;
  readonly values: readonly DuckDBValue[];
  readonly valuesJson: readonly Json[];
}

function col(
  name: string,
  type: DuckDBType,
  typeJson: Json,
  values: readonly DuckDBValue[],
  valuesJson: readonly Json[]
): ColumnNameTypeAndValues {
  return { name, type, typeJson, values, valuesJson };
}

export function createTestAllTypesData(): ColumnNameTypeAndValues[] {
  return [
    col(
      'bool',
      BOOLEAN,
      { typeId: 1 },
      [false, true, null],
      [false, true, null]
    ),
    col(
      'tinyint',
      TINYINT,
      { typeId: 2 },
      [TINYINT.min, TINYINT.max, null],
      [TINYINT.min, TINYINT.max, null]
    ),
    col(
      'smallint',
      SMALLINT,
      { typeId: 3 },
      [SMALLINT.min, SMALLINT.max, null],
      [SMALLINT.min, SMALLINT.max, null]
    ),
    col(
      'int',
      INTEGER,
      { typeId: 4 },
      [INTEGER.min, INTEGER.max, null],
      [INTEGER.min, INTEGER.max, null]
    ),
    col(
      'bigint',
      BIGINT,
      { typeId: 5 },
      [BIGINT.min, BIGINT.max, null],
      [String(BIGINT.min), String(BIGINT.max), null]
    ),
    col(
      'hugeint',
      HUGEINT,
      { typeId: 16 },
      [HUGEINT.min, HUGEINT.max, null],
      [String(HUGEINT.min), String(HUGEINT.max), null]
    ),
    col(
      'uhugeint',
      UHUGEINT,
      { typeId: 32 },
      [UHUGEINT.min, UHUGEINT.max, null],
      [String(UHUGEINT.min), String(UHUGEINT.max), null]
    ),
    col(
      'utinyint',
      UTINYINT,
      { typeId: 6 },
      [UTINYINT.min, UTINYINT.max, null],
      [UTINYINT.min, UTINYINT.max, null]
    ),
    col(
      'usmallint',
      USMALLINT,
      { typeId: 7 },
      [USMALLINT.min, USMALLINT.max, null],
      [USMALLINT.min, USMALLINT.max, null]
    ),
    col(
      'uint',
      UINTEGER,
      { typeId: 8 },
      [UINTEGER.min, UINTEGER.max, null],
      [UINTEGER.min, UINTEGER.max, null]
    ),
    col(
      'ubigint',
      UBIGINT,
      { typeId: 9 },
      [UBIGINT.min, UBIGINT.max, null],
      [String(UBIGINT.min), String(UBIGINT.max), null]
    ),
    col(
      'bignum',
      BIGNUM,
      { typeId: 35 },
      [BIGNUM.min, BIGNUM.max, null],
      [String(BIGNUM.min), String(BIGNUM.max), null]
    ),
    col(
      'date',
      DATE,
      { typeId: 13 },
      [DATE.min, DATE.max, null],
      [String(DATE.min), String(DATE.max), null]
    ),
    col(
      'time',
      TIME,
      { typeId: 14 },
      [TIME.min, TIME.max, null],
      [String(TIME.min), String(TIME.max), null]
    ),
    col(
      'timestamp',
      TIMESTAMP,
      { typeId: 12 },
      [TIMESTAMP.min, TIMESTAMP.max, null],
      [String(TIMESTAMP.min), String(TIMESTAMP.max), null]
    ),
    col(
      'timestamp_s',
      TIMESTAMP_S,
      { typeId: 20 },
      [TIMESTAMP_S.min, TIMESTAMP_S.max, null],
      [String(TIMESTAMP_S.min), String(TIMESTAMP_S.max), null]
    ),
    col(
      'timestamp_ms',
      TIMESTAMP_MS,
      { typeId: 21 },
      [TIMESTAMP_MS.min, TIMESTAMP_MS.max, null],
      [String(TIMESTAMP_MS.min), String(TIMESTAMP_MS.max), null]
    ),
    col(
      'timestamp_ns',
      TIMESTAMP_NS,
      { typeId: 22 },
      [TIMESTAMP_NS.min, TIMESTAMP_NS.max, null],
      [String(TIMESTAMP_NS.min), String(TIMESTAMP_NS.max), null]
    ),
    col(
      'time_tz',
      TIMETZ,
      { typeId: 30 },
      [TIMETZ.min, TIMETZ.max, null],
      [String(TIMETZ.min), String(TIMETZ.max), null]
    ),
    col(
      'timestamp_tz',
      TIMESTAMPTZ,
      { typeId: 31 },
      [TIMESTAMPTZ.min, TIMESTAMPTZ.max, null],
      [String(TIMESTAMPTZ.min), String(TIMESTAMPTZ.max), null]
    ),
    col(
      'float',
      FLOAT,
      { typeId: 10 },
      [FLOAT.min, FLOAT.max, null],
      [FLOAT.min, FLOAT.max, null]
    ),
    col(
      'double',
      DOUBLE,
      { typeId: 11 },
      [DOUBLE.min, DOUBLE.max, null],
      [DOUBLE.min, DOUBLE.max, null]
    ),
    col(
      'dec_4_1',
      DECIMAL(4, 1),
      { typeId: 19, width: 4, scale: 1 },
      [decimalValue(-9999n, 4, 1), decimalValue(9999n, 4, 1), null],
      ['-999.9', '999.9', null]
    ),
    col(
      'dec_9_4',
      DECIMAL(9, 4),
      { typeId: 19, width: 9, scale: 4 },
      [decimalValue(-999999999n, 9, 4), decimalValue(999999999n, 9, 4), null],
      ['-99999.9999', '99999.9999', null]
    ),
    col(
      'dec_18_6',
      DECIMAL(18, 6),
      { typeId: 19, width: 18, scale: 6 },
      [decimalValue(-BI_18_9s, 18, 6), decimalValue(BI_18_9s, 18, 6), null],
      ['-999999999999.999999', '999999999999.999999', null]
    ),
    col(
      'dec38_10',
      DECIMAL(38, 10),
      { typeId: 19, width: 38, scale: 10 },
      [decimalValue(-BI_38_9s, 38, 10), decimalValue(BI_38_9s, 38, 10), null],
      [
        '-9999999999999999999999999999.9999999999',
        '9999999999999999999999999999.9999999999',
        null,
      ]
    ),
    col(
      'uuid',
      UUID,
      { typeId: 27 },
      [UUID.min, UUID.max, null],
      [String(UUID.min), String(UUID.max), null]
    ),
    col(
      'interval',
      INTERVAL,
      { typeId: 15 },
      [intervalValue(0, 0, 0n), intervalValue(999, 999, 999999999n), null],
      [
        { months: 0, days: 0, micros: '0' },
        { months: 999, days: 999, micros: '999999999' },
        null,
      ]
    ),
    col(
      'varchar',
      VARCHAR,
      { typeId: 17 },
      ['🦆🦆🦆🦆🦆🦆', 'goo\0se', null],
      ['🦆🦆🦆🦆🦆🦆', 'goo\0se', null]
    ),
    col(
      'blob',
      BLOB,
      { typeId: 18 },
      [
        blobValue('thisisalongblob\x00withnullbytes'),
        blobValue('\x00\x00\x00a'),
        null,
      ],
      ['thisisalongblob\\x00withnullbytes', '\\x00\\x00\\x00a', null]
    ),
    col(
      'bit',
      BIT,
      { typeId: 29 },
      [bitValue('0010001001011100010101011010111'), bitValue('10101'), null],
      ['0010001001011100010101011010111', '10101', null]
    ),
    col(
      'small_enum',
      ENUM8(smallEnumValues),
      { typeId: 23, values: smallEnumValues, internalTypeId: 6 },
      [smallEnumValues[0], smallEnumValues[smallEnumValues.length - 1], null],
      [smallEnumValues[0], smallEnumValues[smallEnumValues.length - 1], null]
    ),
    col(
      'medium_enum',
      ENUM16(mediumEnumValues),
      { typeId: 23, values: mediumEnumValues, internalTypeId: 7 },
      [
        mediumEnumValues[0],
        mediumEnumValues[mediumEnumValues.length - 1],
        null,
      ],
      [mediumEnumValues[0], mediumEnumValues[mediumEnumValues.length - 1], null]
    ),
    col(
      'large_enum',
      ENUM32(largeEnumValues),
      { typeId: 23, values: largeEnumValues, internalTypeId: 8 },
      [largeEnumValues[0], largeEnumValues[largeEnumValues.length - 1], null],
      [largeEnumValues[0], largeEnumValues[largeEnumValues.length - 1], null]
    ),
    col(
      'int_array',
      LIST(INTEGER),
      { typeId: 24, valueType: { typeId: 4 } },
      [listValue([]), listValue([42, 999, null, null, -42]), null],
      [[], [42, 999, null, null, -42], null]
    ),
    col(
      'double_array',
      LIST(DOUBLE),
      { typeId: 24, valueType: { typeId: 11 } },
      [
        listValue([]),
        listValue([42.0, NaN, Infinity, -Infinity, null, -42.0]),
        null,
      ],
      [[], [42, 'NaN', 'Infinity', '-Infinity', null, -42], null]
    ),
    col(
      'date_array',
      LIST(DATE),
      { typeId: 24, valueType: { typeId: 13 } },
      [
        listValue([]),
        // 19124 days from the epoch is 2022-05-12
        listValue([
          DATE.epoch,
          DATE.posInf,
          DATE.negInf,
          null,
          dateValue(19124),
        ]),
        null,
      ],
      [
        [],
        [
          '1970-01-01',
          '5881580-07-11',
          '5877642-06-24 (BC)',
          null,
          '2022-05-12',
        ],
        null,
      ]
    ),
    col(
      'timestamp_array',
      LIST(TIMESTAMP),
      { typeId: 24, valueType: { typeId: 12 } },
      [
        listValue([]),
        listValue([
          TIMESTAMP.epoch,
          TIMESTAMP.posInf,
          TIMESTAMP.negInf,
          null,
          // 1652372625 seconds from the epoch is 2022-05-12 16:23:45
          timestampValue(1652372625n * 1000n * 1000n),
        ]),
        null,
      ],
      [
        [],
        [
          '1970-01-01 00:00:00',
          'infinity',
          '-infinity',
          null,
          '2022-05-12 16:23:45',
        ],
        null,
      ]
    ),
    col(
      'timestamptz_array',
      LIST(TIMESTAMPTZ),
      { typeId: 24, valueType: { typeId: 31 } },
      [
        listValue([]),
        listValue([
          TIMESTAMPTZ.epoch,
          TIMESTAMPTZ.posInf,
          TIMESTAMPTZ.negInf,
          null,
          // 1652397825 = 1652372625 + 25200, 25200 = 7 * 60 * 60 = 7 hours in seconds
          // This 7 hour difference is hard coded into test_all_types (value is 2022-05-12 16:23:45-07)
          timestampTZValue(1652397825n * 1000n * 1000n),
        ]),
        null,
      ],
      [
        [],
        [
          String(TIMESTAMPTZ.epoch),
          'infinity',
          '-infinity',
          null,
          String(timestampTZValue(1652397825n * 1000n * 1000n)),
        ],
        null,
      ]
    ),
    col(
      'varchar_array',
      LIST(VARCHAR),
      { typeId: 24, valueType: { typeId: 17 } },
      [
        listValue([]),
        // Note that the string 'goose' in varchar_array does NOT have an embedded null character.
        listValue(['🦆🦆🦆🦆🦆🦆', 'goose', null, '']),
        null,
      ],
      [[], ['🦆🦆🦆🦆🦆🦆', 'goose', null, ''], null]
    ),
    col(
      'nested_int_array',
      LIST(LIST(INTEGER)),
      { typeId: 24, valueType: { typeId: 24, valueType: { typeId: 4 } } },
      [
        listValue([]),
        listValue([
          listValue([]),
          listValue([42, 999, null, null, -42]),
          null,
          listValue([]),
          listValue([42, 999, null, null, -42]),
        ]),
        null,
      ],
      [
        [],
        [[], [42, 999, null, null, -42], null, [], [42, 999, null, null, -42]],
        null,
      ]
    ),
    col(
      'struct',
      STRUCT({ 'a': INTEGER, 'b': VARCHAR }),
      {
        typeId: 25,
        entryNames: ['a', 'b'],
        entryTypes: [{ typeId: 4 }, { typeId: 17 }],
      },
      [
        structValue({ 'a': null, 'b': null }),
        structValue({ 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' }),
        null,
      ],
      [{ 'a': null, 'b': null }, { 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' }, null]
    ),
    col(
      'struct_of_arrays',
      STRUCT({ 'a': LIST(INTEGER), 'b': LIST(VARCHAR) }),
      {
        typeId: 25,
        entryNames: ['a', 'b'],
        entryTypes: [
          { typeId: 24, valueType: { typeId: 4 } },
          { typeId: 24, valueType: { typeId: 17 } },
        ],
      },
      [
        structValue({ 'a': null, 'b': null }),
        structValue({
          'a': listValue([42, 999, null, null, -42]),
          'b': listValue(['🦆🦆🦆🦆🦆🦆', 'goose', null, '']),
        }),
        null,
      ],
      [
        { 'a': null, 'b': null },
        {
          'a': [42, 999, null, null, -42],
          'b': ['🦆🦆🦆🦆🦆🦆', 'goose', null, ''],
        },
        null,
      ]
    ),
    col(
      'array_of_structs',
      LIST(STRUCT({ 'a': INTEGER, 'b': VARCHAR })),
      {
        typeId: 24,
        valueType: {
          typeId: 25,
          entryNames: ['a', 'b'],
          entryTypes: [{ typeId: 4 }, { typeId: 17 }],
        },
      },
      [
        listValue([]),
        listValue([
          structValue({ 'a': null, 'b': null }),
          structValue({ 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' }),
          null,
        ]),
        null,
      ],
      [
        [],
        [{ 'a': null, 'b': null }, { 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' }, null],
        null,
      ]
    ),
    col(
      'map',
      MAP(VARCHAR, VARCHAR),
      { typeId: 26, keyType: { typeId: 17 }, valueType: { typeId: 17 } },
      [
        mapValue([]),
        mapValue([
          { key: 'key1', value: '🦆🦆🦆🦆🦆🦆' },
          { key: 'key2', value: 'goose' },
        ]),
        null,
      ],
      [
        [],
        [
          { 'key': 'key1', 'value': '🦆🦆🦆🦆🦆🦆' },
          { 'key': 'key2', 'value': 'goose' },
        ],
        null,
      ]
    ),
    col(
      'union',
      UNION({ 'name': VARCHAR, 'age': SMALLINT }),
      {
        typeId: 28,
        memberTags: ['name', 'age'],
        memberTypes: [{ typeId: 17 }, { typeId: 3 }],
      },
      [unionValue('name', 'Frank'), unionValue('age', 5), null],
      [{ 'tag': 'name', 'value': 'Frank' }, { 'tag': 'age', 'value': 5 }, null]
    ),
    col(
      'fixed_int_array',
      ARRAY(INTEGER, 3),
      { typeId: 33, valueType: { typeId: 4 }, length: 3 },
      [arrayValue([null, 2, 3]), arrayValue([4, 5, 6]), null],
      [[null, 2, 3], [4, 5, 6], null]
    ),
    col(
      'fixed_varchar_array',
      ARRAY(VARCHAR, 3),
      { typeId: 33, valueType: { typeId: 17 }, length: 3 },
      [arrayValue(['a', null, 'c']), arrayValue(['d', 'e', 'f']), null],
      [['a', null, 'c'], ['d', 'e', 'f'], null]
    ),
    col(
      'fixed_nested_int_array',
      ARRAY(ARRAY(INTEGER, 3), 3),
      {
        typeId: 33,
        valueType: { typeId: 33, valueType: { typeId: 4 }, length: 3 },
        length: 3,
      },
      [
        arrayValue([arrayValue([null, 2, 3]), null, arrayValue([null, 2, 3])]),
        arrayValue([
          arrayValue([4, 5, 6]),
          arrayValue([null, 2, 3]),
          arrayValue([4, 5, 6]),
        ]),
        null,
      ],
      [
        [[null, 2, 3], null, [null, 2, 3]],
        [
          [4, 5, 6],
          [null, 2, 3],
          [4, 5, 6],
        ],
        null,
      ]
    ),
    col(
      'fixed_nested_varchar_array',
      ARRAY(ARRAY(VARCHAR, 3), 3),
      {
        typeId: 33,
        valueType: { typeId: 33, valueType: { typeId: 17 }, length: 3 },
        length: 3,
      },
      [
        arrayValue([
          arrayValue(['a', null, 'c']),
          null,
          arrayValue(['a', null, 'c']),
        ]),
        arrayValue([
          arrayValue(['d', 'e', 'f']),
          arrayValue(['a', null, 'c']),
          arrayValue(['d', 'e', 'f']),
        ]),
        null,
      ],
      [
        [['a', null, 'c'], null, ['a', null, 'c']],
        [
          ['d', 'e', 'f'],
          ['a', null, 'c'],
          ['d', 'e', 'f'],
        ],
        null,
      ]
    ),
    col(
      'fixed_struct_array',
      ARRAY(STRUCT({ 'a': INTEGER, 'b': VARCHAR }), 3),
      {
        typeId: 33,
        valueType: {
          typeId: 25,
          entryNames: ['a', 'b'],
          entryTypes: [{ typeId: 4 }, { typeId: 17 }],
        },
        length: 3,
      },
      [
        arrayValue([
          structValue({ 'a': null, 'b': null }),
          structValue({ 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' }),
          structValue({ 'a': null, 'b': null }),
        ]),
        arrayValue([
          structValue({ 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' }),
          structValue({ 'a': null, 'b': null }),
          structValue({ 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' }),
        ]),
        null,
      ],
      [
        [
          { 'a': null, 'b': null },
          { 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' },
          { 'a': null, 'b': null },
        ],
        [
          { 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' },
          { 'a': null, 'b': null },
          { 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' },
        ],
        null,
      ]
    ),
    col(
      'struct_of_fixed_array',
      STRUCT({ 'a': ARRAY(INTEGER, 3), 'b': ARRAY(VARCHAR, 3) }),
      {
        typeId: 25,
        entryNames: ['a', 'b'],
        entryTypes: [
          { typeId: 33, valueType: { typeId: 4 }, length: 3 },
          { typeId: 33, valueType: { typeId: 17 }, length: 3 },
        ],
      },
      [
        structValue({
          'a': arrayValue([null, 2, 3]),
          'b': arrayValue(['a', null, 'c']),
        }),
        structValue({
          'a': arrayValue([4, 5, 6]),
          'b': arrayValue(['d', 'e', 'f']),
        }),
        null,
      ],
      [
        { 'a': [null, 2, 3], 'b': ['a', null, 'c'] },
        { 'a': [4, 5, 6], 'b': ['d', 'e', 'f'] },
        null,
      ]
    ),
    col(
      'fixed_array_of_int_list',
      ARRAY(LIST(INTEGER), 3),
      {
        typeId: 33,
        valueType: { typeId: 24, valueType: { typeId: 4 } },
        length: 3,
      },
      [
        arrayValue([
          listValue([]),
          listValue([42, 999, null, null, -42]),
          listValue([]),
        ]),
        arrayValue([
          listValue([42, 999, null, null, -42]),
          listValue([]),
          listValue([42, 999, null, null, -42]),
        ]),
        null,
      ],
      [
        [[], [42, 999, null, null, -42], []],
        [[42, 999, null, null, -42], [], [42, 999, null, null, -42]],
        null,
      ]
    ),
    col(
      'list_of_fixed_int_array',
      LIST(ARRAY(INTEGER, 3)),
      {
        typeId: 24,
        valueType: { typeId: 33, valueType: { typeId: 4 }, length: 3 },
      },
      [
        listValue([
          arrayValue([null, 2, 3]),
          arrayValue([4, 5, 6]),
          arrayValue([null, 2, 3]),
        ]),
        listValue([
          arrayValue([4, 5, 6]),
          arrayValue([null, 2, 3]),
          arrayValue([4, 5, 6]),
        ]),
        null,
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
      ]
    ),
  ];
}

export function createTestAllTypesColumnNames(): readonly string[] {
  return createTestAllTypesData().map(({ name }) => name);
}

export function createTestAllTypesColumnTypes(): readonly DuckDBType[] {
  return createTestAllTypesData().map(({ type }) => type);
}

export function createTestAllTypesColumnTypesJson(): readonly Json[] {
  return createTestAllTypesData().map(({ typeJson }) => typeJson);
}

export function createTestAllTypesColumnNamesAndTypesJson(): Json {
  return {
    columnNames: [...createTestAllTypesColumnNames()],
    columnTypes: [...createTestAllTypesColumnTypesJson()],
  };
}

export function createTestAllTypesColumnNameAndTypeObjects(): readonly ColumnNameAndType[] {
  return createTestAllTypesData().map(({ name, type }) => ({ name, type }));
}

export function createTestAllTypesColumnNameAndTypeObjectsJson(): Json {
  return createTestAllTypesData().map(({ name, typeJson }) => ({
    columnName: name,
    columnType: typeJson,
  }));
}

export function createTestAllTypesColumns(): readonly (readonly DuckDBValue[])[] {
  return createTestAllTypesData().map(({ values }) => values);
}

export function createTestAllTypesColumnsJson(): readonly (readonly Json[])[] {
  return createTestAllTypesData().map(({ valuesJson }) => valuesJson);
}

export function createTestAllTypesColumnsObjectJson(): Record<
  string,
  readonly Json[]
> {
  const columnsObject: Record<string, readonly Json[]> = {};
  const data = createTestAllTypesData();
  for (const columnData of data) {
    columnsObject[columnData.name] = columnData.valuesJson;
  }
  return columnsObject;
}

export function createTestAllTypesRowsJson(): readonly (readonly Json[])[] {
  const data = createTestAllTypesData();
  return [
    data.map(({ valuesJson }) => valuesJson[0]),
    data.map(({ valuesJson }) => valuesJson[1]),
    data.map(({ valuesJson }) => valuesJson[2]),
  ];
}

export function createTestAllTypesRowObjectsJson(): readonly Record<
  string,
  Json
>[] {
  const rowObjects: Record<string, Json>[] = [{}, {}, {}];
  const data = createTestAllTypesData();
  for (const columnData of data) {
    rowObjects[0][columnData.name] = columnData.valuesJson[0];
    rowObjects[1][columnData.name] = columnData.valuesJson[1];
    rowObjects[2][columnData.name] = columnData.valuesJson[2];
  }
  return rowObjects;
}
