import duckdb from '@duckdb/node-bindings';
import { createValue } from './createValue';
import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBLogicalType } from './DuckDBLogicalType';
import {
  BIT,
  DECIMAL,
  DuckDBArrayType,
  DuckDBEnumType,
  DuckDBListType,
  DuckDBStructType,
  DuckDBType,
  TIMESTAMP_MS,
  TIMESTAMP_NS,
  TIMESTAMP_S,
  TIMESTAMPTZ,
  TIMETZ,
  UUID,
  VARINT,
} from './DuckDBType';
import {
  DuckDBArrayValue,
  DuckDBBitValue,
  DuckDBDateValue,
  DuckDBDecimalValue,
  DuckDBIntervalValue,
  DuckDBListValue,
  DuckDBStructValue,
  DuckDBTimestampMillisecondsValue,
  DuckDBTimestampNanosecondsValue,
  DuckDBTimestampSecondsValue,
  DuckDBTimestampTZValue,
  DuckDBTimestampValue,
  DuckDBTimeTZValue,
  DuckDBTimeValue,
  DuckDBUUIDValue,
  DuckDBValue,
} from './values';

export class DuckDBAppender {
  private readonly appender: duckdb.Appender;
  constructor(appender: duckdb.Appender) {
    this.appender = appender;
  }
  public close() {
    duckdb.appender_close(this.appender);
  }
  public flush() {
    duckdb.appender_flush(this.appender);
  }
  public get columnCount(): number {
    return duckdb.appender_column_count(this.appender);
  }
  public columnType(columnIndex: number): DuckDBType {
    return DuckDBLogicalType.create(
      duckdb.appender_column_type(this.appender, columnIndex)
    ).asType();
  }
  public endRow() {
    duckdb.appender_end_row(this.appender);
  }
  public appendDefault() {
    duckdb.append_default(this.appender);
  }
  public appendBoolean(value: boolean) {
    duckdb.append_bool(this.appender, value);
  }
  public appendTinyInt(value: number) {
    duckdb.append_int8(this.appender, value);
  }
  public appendSmallInt(value: number) {
    duckdb.append_int16(this.appender, value);
  }
  public appendInteger(value: number) {
    duckdb.append_int32(this.appender, value);
  }
  public appendBigInt(value: bigint) {
    duckdb.append_int64(this.appender, value);
  }
  public appendHugeInt(value: bigint) {
    duckdb.append_hugeint(this.appender, value);
  }
  public appendUTinyInt(value: number) {
    duckdb.append_uint8(this.appender, value);
  }
  public appendUSmallInt(value: number) {
    duckdb.append_uint16(this.appender, value);
  }
  public appendUInteger(value: number) {
    duckdb.append_uint32(this.appender, value);
  }
  public appendUBigInt(value: bigint) {
    duckdb.append_uint64(this.appender, value);
  }
  public appendUHugeInt(value: bigint) {
    duckdb.append_uhugeint(this.appender, value);
  }
  public appendDecimal(value: DuckDBDecimalValue) {
    // The width and scale of the DECIMAL type here aren't actually used.
    this.appendValue(value, DECIMAL(value.width, value.scale));
  }
  public appendFloat(value: number) {
    duckdb.append_float(this.appender, value);
  }
  public appendDouble(value: number) {
    duckdb.append_double(this.appender, value);
  }
  public appendDate(value: DuckDBDateValue) {
    duckdb.append_date(this.appender, value);
  }
  public appendTime(value: DuckDBTimeValue) {
    duckdb.append_time(this.appender, value);
  }
  public appendTimeTZ(value: DuckDBTimeTZValue) {
    this.appendValue(value, TIMETZ);
  }
  public appendTimestamp(value: DuckDBTimestampValue) {
    duckdb.append_timestamp(this.appender, value);
  }
  public appendTimestampTZ(value: DuckDBTimestampTZValue) {
    this.appendValue(value, TIMESTAMPTZ);
  }
  public appendTimestampSeconds(value: DuckDBTimestampSecondsValue) {
    this.appendValue(value, TIMESTAMP_S);
  }
  public appendTimestampMilliseconds(value: DuckDBTimestampMillisecondsValue) {
    this.appendValue(value, TIMESTAMP_MS);
  }
  public appendTimestampNanoseconds(value: DuckDBTimestampNanosecondsValue) {
    this.appendValue(value, TIMESTAMP_NS);
  }
  public appendInterval(value: DuckDBIntervalValue) {
    duckdb.append_interval(this.appender, value);
  }
  public appendVarchar(value: string) {
    duckdb.append_varchar(this.appender, value);
  }
  public appendBlob(value: Uint8Array) {
    duckdb.append_blob(this.appender, value);
  }
  public appendEnum(value: string, type: DuckDBEnumType) {
    this.appendValue(value, type);
  }
  public appendList(value: DuckDBListValue, type: DuckDBListType) {
    this.appendValue(value, type);
  }
  public appendStruct(value: DuckDBStructValue, type: DuckDBStructType) {
    this.appendValue(value, type);
  }
  // TODO: MAP (when DuckDB C API supports creating MAP values)
  public appendArray(value: DuckDBArrayValue, type: DuckDBArrayType) {
    this.appendValue(value, type);
  }
  // TODO: UNION (when DuckDB C API supports creating UNION values)
  public appendUUID(value: DuckDBUUIDValue) {
    this.appendValue(value, UUID);
  }
  public appendBit(value: DuckDBBitValue) {
    this.appendValue(value, BIT);
  }
  public appendVarInt(value: bigint) {
    this.appendValue(value, VARINT);
  }
  public appendNull() {
    duckdb.append_null(this.appender);
  }
  public appendValue(value: DuckDBValue, type: DuckDBType) {
    duckdb.append_value(this.appender, createValue(type, value));
  }
  public appendDataChunk(dataChunk: DuckDBDataChunk) {
    duckdb.append_data_chunk(this.appender, dataChunk.chunk);
  }
}
