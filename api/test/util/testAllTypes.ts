import {
  ARRAY,
  arrayValue,
  BIGINT,
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
  VARINT,
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

export interface ColumnNameTypeAndRows extends ColumnNameAndType {
  readonly name: string;
  readonly type: DuckDBType;
  readonly rows: readonly DuckDBValue[];
}

function col(
  name: string,
  type: DuckDBType,
  rows: readonly DuckDBValue[]
): ColumnNameTypeAndRows {
  return { name, type, rows };
}

export const testAllTypesData: ColumnNameTypeAndRows[] = [
  col('bool', BOOLEAN, [false, true, null]),
  col('tinyint', TINYINT, [TINYINT.min, TINYINT.max, null]),
  col('smallint', SMALLINT, [SMALLINT.min, SMALLINT.max, null]),
  col('int', INTEGER, [INTEGER.min, INTEGER.max, null]),
  col('bigint', BIGINT, [BIGINT.min, BIGINT.max, null]),
  col('hugeint', HUGEINT, [HUGEINT.min, HUGEINT.max, null]),
  col('uhugeint', UHUGEINT, [UHUGEINT.min, UHUGEINT.max, null]),
  col('utinyint', UTINYINT, [UTINYINT.min, UTINYINT.max, null]),
  col('usmallint', USMALLINT, [USMALLINT.min, USMALLINT.max, null]),
  col('uint', UINTEGER, [UINTEGER.min, UINTEGER.max, null]),
  col('ubigint', UBIGINT, [UBIGINT.min, UBIGINT.max, null]),
  col('varint', VARINT, [VARINT.min, VARINT.max, null]),
  col('date', DATE, [DATE.min, DATE.max, null]),
  col('time', TIME, [TIME.min, TIME.max, null]),
  col('timestamp', TIMESTAMP, [TIMESTAMP.min, TIMESTAMP.max, null]),
  col('timestamp_s', TIMESTAMP_S, [TIMESTAMP_S.min, TIMESTAMP_S.max, null]),
  col('timestamp_ms', TIMESTAMP_MS, [TIMESTAMP_MS.min, TIMESTAMP_MS.max, null]),
  col('timestamp_ns', TIMESTAMP_NS, [TIMESTAMP_NS.min, TIMESTAMP_NS.max, null]),
  col('time_tz', TIMETZ, [TIMETZ.min, TIMETZ.max, null]),
  col('timestamp_tz', TIMESTAMPTZ, [TIMESTAMPTZ.min, TIMESTAMPTZ.max, null]),
  col('float', FLOAT, [FLOAT.min, FLOAT.max, null]),
  col('double', DOUBLE, [DOUBLE.min, DOUBLE.max, null]),
  col('dec_4_1', DECIMAL(4, 1), [
    decimalValue(-9999n, 4, 1),
    decimalValue(9999n, 4, 1),
    null,
  ]),
  col('dec_9_4', DECIMAL(9, 4), [
    decimalValue(-999999999n, 9, 4),
    decimalValue(999999999n, 9, 4),
    null,
  ]),
  col('dec_18_6', DECIMAL(18, 6), [
    decimalValue(-BI_18_9s, 18, 6),
    decimalValue(BI_18_9s, 18, 6),
    null,
  ]),
  col('dec38_10', DECIMAL(38, 10), [
    decimalValue(-BI_38_9s, 38, 10),
    decimalValue(BI_38_9s, 38, 10),
    null,
  ]),
  col('uuid', UUID, [UUID.min, UUID.max, null]),
  col('interval', INTERVAL, [
    intervalValue(0, 0, 0n),
    intervalValue(999, 999, 999999999n),
    null,
  ]),
  col('varchar', VARCHAR, ['🦆🦆🦆🦆🦆🦆', 'goo\0se', null]),
  col('blob', BLOB, [
    blobValue('thisisalongblob\x00withnullbytes'),
    blobValue('\x00\x00\x00a'),
    null,
  ]),
  col('bit', BIT, [
    bitValue('0010001001011100010101011010111'),
    bitValue('10101'),
    null,
  ]),
  col('small_enum', ENUM8(smallEnumValues), [
    smallEnumValues[0],
    smallEnumValues[smallEnumValues.length - 1],
    null,
  ]),
  col('medium_enum', ENUM16(mediumEnumValues), [
    mediumEnumValues[0],
    mediumEnumValues[mediumEnumValues.length - 1],
    null,
  ]),
  col('large_enum', ENUM32(largeEnumValues), [
    largeEnumValues[0],
    largeEnumValues[largeEnumValues.length - 1],
    null,
  ]),
  col('int_array', LIST(INTEGER), [
    listValue([]),
    listValue([42, 999, null, null, -42]),
    null,
  ]),
  col('double_array', LIST(DOUBLE), [
    listValue([]),
    listValue([42.0, NaN, Infinity, -Infinity, null, -42.0]),
    null,
  ]),
  col('date_array', LIST(DATE), [
    listValue([]),
    // 19124 days from the epoch is 2022-05-12
    listValue([DATE.epoch, DATE.posInf, DATE.negInf, null, dateValue(19124)]),
    null,
  ]),
  col('timestamp_array', LIST(TIMESTAMP), [
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
  ]),
  col('timestamptz_array', LIST(TIMESTAMPTZ), [
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
  ]),
  col('varchar_array', LIST(VARCHAR), [
    listValue([]),
    // Note that the string 'goose' in varchar_array does NOT have an embedded null character.
    listValue(['🦆🦆🦆🦆🦆🦆', 'goose', null, '']),
    null,
  ]),
  col('nested_int_array', LIST(LIST(INTEGER)), [
    listValue([]),
    listValue([
      listValue([]),
      listValue([42, 999, null, null, -42]),
      null,
      listValue([]),
      listValue([42, 999, null, null, -42]),
    ]),
    null,
  ]),
  col('struct', STRUCT({ 'a': INTEGER, 'b': VARCHAR }), [
    structValue({ 'a': null, 'b': null }),
    structValue({ 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' }),
    null,
  ]),
  col('struct_of_arrays', STRUCT({ 'a': LIST(INTEGER), 'b': LIST(VARCHAR) }), [
    structValue({ 'a': null, 'b': null }),
    structValue({
      'a': listValue([42, 999, null, null, -42]),
      'b': listValue(['🦆🦆🦆🦆🦆🦆', 'goose', null, '']),
    }),
    null,
  ]),
  col('array_of_structs', LIST(STRUCT({ 'a': INTEGER, 'b': VARCHAR })), [
    listValue([]),
    listValue([
      structValue({ 'a': null, 'b': null }),
      structValue({ 'a': 42, 'b': '🦆🦆🦆🦆🦆🦆' }),
      null,
    ]),
    null,
  ]),
  col('map', MAP(VARCHAR, VARCHAR), [
    mapValue([]),
    mapValue([
      { key: 'key1', value: '🦆🦆🦆🦆🦆🦆' },
      { key: 'key2', value: 'goose' },
    ]),
    null,
  ]),
  col('union', UNION({ 'name': VARCHAR, 'age': SMALLINT }), [
    unionValue('name', 'Frank'),
    unionValue('age', 5),
    null,
  ]),
  col('fixed_int_array', ARRAY(INTEGER, 3), [
    arrayValue([null, 2, 3]),
    arrayValue([4, 5, 6]),
    null,
  ]),
  col('fixed_varchar_array', ARRAY(VARCHAR, 3), [
    arrayValue(['a', null, 'c']),
    arrayValue(['d', 'e', 'f']),
    null,
  ]),
  col('fixed_nested_int_array', ARRAY(ARRAY(INTEGER, 3), 3), [
    arrayValue([arrayValue([null, 2, 3]), null, arrayValue([null, 2, 3])]),
    arrayValue([
      arrayValue([4, 5, 6]),
      arrayValue([null, 2, 3]),
      arrayValue([4, 5, 6]),
    ]),
    null,
  ]),
  col('fixed_nested_varchar_array', ARRAY(ARRAY(VARCHAR, 3), 3), [
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
  ]),
  col('fixed_struct_array', ARRAY(STRUCT({ 'a': INTEGER, 'b': VARCHAR }), 3), [
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
  ]),
  col(
    'struct_of_fixed_array',
    STRUCT({ 'a': ARRAY(INTEGER, 3), 'b': ARRAY(VARCHAR, 3) }),
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
    ]
  ),
  col('fixed_array_of_int_list', ARRAY(LIST(INTEGER), 3), [
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
  ]),
  col('list_of_fixed_int_array', LIST(ARRAY(INTEGER, 3)), [
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
  ]),
];

export const testAllTypesColumnNames: readonly string[] =
  testAllTypesData.map(({ name }) => name);

export const testAllTypesColumnTypes: readonly DuckDBType[] =
  testAllTypesData.map(({ type }) => type);

export const testAllTypesColumnsNamesAndTypes: readonly ColumnNameAndType[] =
  testAllTypesData.map(({ name, type }) => ({ name, type }));

export const testAllTypesColumns: readonly (readonly DuckDBValue[])[] =
  testAllTypesData.map(({ rows }) => rows);
