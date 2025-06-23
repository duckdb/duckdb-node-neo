import duckdb, { Value } from '@duckdb/node-bindings';
import { DuckDBType } from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';
import {
  DuckDBArrayValue,
  DuckDBBitValue,
  DuckDBBlobValue,
  DuckDBDateValue,
  DuckDBDecimalValue,
  DuckDBIntervalValue,
  DuckDBListValue,
  DuckDBMapValue,
  DuckDBStructValue,
  DuckDBTimestampMillisecondsValue,
  DuckDBTimestampNanosecondsValue,
  DuckDBTimestampSecondsValue,
  DuckDBTimestampTZValue,
  DuckDBTimestampValue,
  DuckDBTimeTZValue,
  DuckDBTimeValue,
  DuckDBUnionValue,
  DuckDBUUIDValue,
  DuckDBValue,
} from './values';

export function createValue(type: DuckDBType, input: DuckDBValue): Value {
  if (input === null) {
    return duckdb.create_null_value();
  }
  const { typeId } = type;
  switch (typeId) {
    case DuckDBTypeId.BOOLEAN:
      if (typeof input === 'boolean') {
        return duckdb.create_bool(input);
      }
      throw new Error(`input is not a boolean`);
    case DuckDBTypeId.TINYINT:
      if (typeof input === 'number') {
        return duckdb.create_int8(input);
      }
      throw new Error(`input is not a number`);
    case DuckDBTypeId.SMALLINT:
      if (typeof input === 'number') {
        return duckdb.create_int16(input);
      }
      throw new Error(`input is not a number`);
    case DuckDBTypeId.INTEGER:
      if (typeof input === 'number') {
        return duckdb.create_int32(input);
      }
      throw new Error(`input is not a number`);
    case DuckDBTypeId.BIGINT:
      if (typeof input === 'bigint') {
        return duckdb.create_int64(input);
      }
      throw new Error(`input is not a bigint`);
    case DuckDBTypeId.UTINYINT:
      if (typeof input === 'number') {
        return duckdb.create_uint8(input);
      }
      throw new Error(`input is not a number`);
    case DuckDBTypeId.USMALLINT:
      if (typeof input === 'number') {
        return duckdb.create_uint16(input);
      }
      throw new Error(`input is not a number`);
    case DuckDBTypeId.UINTEGER:
      if (typeof input === 'number') {
        return duckdb.create_uint32(input);
      }
      throw new Error(`input is not a number`);
    case DuckDBTypeId.UBIGINT:
      if (typeof input === 'bigint') {
        return duckdb.create_uint64(input);
      }
      throw new Error(`input is not a bigint`);
    case DuckDBTypeId.FLOAT:
      if (typeof input === 'number') {
        return duckdb.create_float(input);
      }
      throw new Error(`input is not a number`);
    case DuckDBTypeId.DOUBLE:
      if (typeof input === 'number') {
        return duckdb.create_double(input);
      }
      throw new Error(`input is not a number`);
    case DuckDBTypeId.TIMESTAMP:
      if (input instanceof DuckDBTimestampValue) {
        return duckdb.create_timestamp(input);
      }
      throw new Error(`input is not a DuckDBTimestampValue`);
    case DuckDBTypeId.DATE:
      if (input instanceof DuckDBDateValue) {
        return duckdb.create_date(input);
      }
      throw new Error(`input is not a DuckDBDateValue`);
    case DuckDBTypeId.TIME:
      if (input instanceof DuckDBTimeValue) {
        return duckdb.create_time(input);
      }
      throw new Error(`input is not a DuckDBTimeValue`);
    case DuckDBTypeId.INTERVAL:
      if (input instanceof DuckDBIntervalValue) {
        return duckdb.create_interval(input);
      }
      throw new Error(`input is not a DuckDBIntervalValue`);
    case DuckDBTypeId.HUGEINT:
      if (typeof input === 'bigint') {
        return duckdb.create_hugeint(input);
      }
      throw new Error(`input is not a bigint`);
    case DuckDBTypeId.UHUGEINT:
      if (typeof input === 'bigint') {
        return duckdb.create_uhugeint(input);
      }
      throw new Error(`input is not a bigint`);
    case DuckDBTypeId.VARCHAR:
      if (typeof input === 'string') {
        return duckdb.create_varchar(input);
      }
      throw new Error(`input is not a string`);
    case DuckDBTypeId.BLOB:
      if (input instanceof DuckDBBlobValue) {
        return duckdb.create_blob(input.bytes);
      }
      throw new Error(`input is not a DuckDBBlobValue`);
    case DuckDBTypeId.DECIMAL:
      if (input instanceof DuckDBDecimalValue) {
        return duckdb.create_decimal(input);
      }
      throw new Error(`input is not a DuckDBDecimalValue`);
    case DuckDBTypeId.TIMESTAMP_S:
      if (input instanceof DuckDBTimestampSecondsValue) {
        return duckdb.create_timestamp_s(input);
      }
      throw new Error(`input is not a DuckDBTimestampSecondsValue`);
    case DuckDBTypeId.TIMESTAMP_MS:
      if (input instanceof DuckDBTimestampMillisecondsValue) {
        return duckdb.create_timestamp_ms(input);
      }
      throw new Error(`input is not a DuckDBTimestampMillisecondsValue`);
    case DuckDBTypeId.TIMESTAMP_NS:
      if (input instanceof DuckDBTimestampNanosecondsValue) {
        return duckdb.create_timestamp_ns(input);
      }
      throw new Error(`input is not a DuckDBTimestampNanosecondsValue`);
    case DuckDBTypeId.ENUM:
      if (typeof input === 'string') {
        return duckdb.create_enum_value(
          type.toLogicalType().logical_type,
          type.indexForValue(input)
        );
      }
      throw new Error(`input is not a string`);
    case DuckDBTypeId.LIST:
      if (input instanceof DuckDBListValue) {
        if (type.valueType.typeId === DuckDBTypeId.ANY) {
          throw new Error(
            'Cannot create lists with item type of ANY. Specify a specific type.'
          );
        }
        return duckdb.create_list_value(
          type.valueType.toLogicalType().logical_type,
          input.items.map((item) => createValue(type.valueType, item))
        );
      }
      throw new Error(`input is not a DuckDBListValue`);
    case DuckDBTypeId.STRUCT:
      if (input instanceof DuckDBStructValue) {
        if (
          type.entryTypes.find(
            (entryType) => entryType.typeId === DuckDBTypeId.ANY
          )
        ) {
          throw new Error(
            'Cannot create structs with an entry type of ANY. Specify a specific type.'
          );
        }
        return duckdb.create_struct_value(
          type.toLogicalType().logical_type,
          Object.values(input.entries).map((value, i) =>
            createValue(type.entryTypes[i], value)
          )
        );
      }
      throw new Error(`input is not a DuckDBStructValue`);
    case DuckDBTypeId.MAP:
      if (input instanceof DuckDBMapValue) {
        if (type.keyType.typeId === DuckDBTypeId.ANY) {
          throw new Error(
            'Cannot create maps with key type of ANY. Specify a specific type.'
          );
        }
        if (type.valueType.typeId === DuckDBTypeId.ANY) {
          throw new Error(
            'Cannot create maps with value type of ANY. Specify a specific type.'
          );
        }
        return duckdb.create_map_value(
          type.toLogicalType().logical_type,
          input.entries.map((entry) => createValue(type.keyType, entry.key)),
          input.entries.map((entry) => createValue(type.valueType, entry.value))
        );
      }
      throw new Error(`input is not a DuckDBMapValue`);
    case DuckDBTypeId.ARRAY:
      if (input instanceof DuckDBArrayValue) {
        if (type.valueType.typeId === DuckDBTypeId.ANY) {
          throw new Error(
            'Cannot create arrays with item type of ANY. Specify a specific type.'
          );
        }
        return duckdb.create_array_value(
          type.valueType.toLogicalType().logical_type,
          input.items.map((item) => createValue(type.valueType, item))
        );
      }
      throw new Error(`input is not a DuckDBArrayValue`);
    case DuckDBTypeId.UUID:
      if (input instanceof DuckDBUUIDValue) {
        return duckdb.create_uuid(input.toUint128());
      }
      throw new Error(`input is not a bigint`);
    case DuckDBTypeId.UNION:
      if (input instanceof DuckDBUnionValue) {
        const tagIndex = type.memberIndexForTag(input.tag);
        const memberType = type.memberTypes[tagIndex];
        if (memberType.typeId === DuckDBTypeId.ANY) {
          throw new Error('Cannot create union values with type of ANY.');
        }
        return duckdb.create_union_value(
          type.toLogicalType().logical_type,
          tagIndex,
          createValue(memberType, input.value)
        );
      }
      throw new Error(`input is not a DuckDBUnionValue`);
    case DuckDBTypeId.BIT:
      if (input instanceof DuckDBBitValue) {
        return duckdb.create_bit(input.data);
      }
      throw new Error(`input is not a DuckDBBitValue`);
    case DuckDBTypeId.TIME_TZ:
      if (input instanceof DuckDBTimeTZValue) {
        return duckdb.create_time_tz_value(input);
      }
      throw new Error(`input is not a DuckDBTimeTZValue`);
    case DuckDBTypeId.TIMESTAMP_TZ:
      if (input instanceof DuckDBTimestampTZValue) {
        return duckdb.create_timestamp_tz(input);
      }
      throw new Error(`input is not a DuckDBTimestampTZValue`);
    case DuckDBTypeId.ANY:
      throw new Error(
        `Cannot create values of type ANY. Specify a specific type.`
      );
    case DuckDBTypeId.VARINT:
      if (typeof input === 'bigint') {
        return duckdb.create_varint(input);
      }
      throw new Error(`input is not a bigint`);
    case DuckDBTypeId.SQLNULL:
      return duckdb.create_null_value();
    default:
      throw new Error(`unrecognized type id ${typeId}`);
  }
}
