import duckdb from '@duckdb/node-bindings';
import { DuckDBType } from './DuckDBType';
import { DuckDBLogicalType } from './DuckDBLogicalType';
import { Date_, Interval, Time, Timestamp } from './DuckDBValue';
import { DuckDBDataChunk } from './DuckDBDataChunk';

export class DuckDBAppender {
  private readonly appender: duckdb.Appender;
  constructor(appender: duckdb.Appender) {
    this.appender = appender;
  }
  public dispose() {
    duckdb.appender_destroy(this.appender);
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
    return DuckDBLogicalType.consumeAsType(duckdb.appender_column_type(this.appender, columnIndex));
  }
  public endRow() {
    duckdb.appender_end_row(this.appender);
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
  // TODO: append DECIMAL?
  public appendFloat(value: number) {
    duckdb.append_float(this.appender, value);
  }
  public appendDouble(value: number) {
    duckdb.append_double(this.appender, value);
  }
  public appendDate(value: Date_) {
    duckdb.append_date(this.appender, value);
  }
  public appendTime(value: Time) {
    duckdb.append_time(this.appender, value);
  }
  public appendTimestamp(value: Timestamp) {
    duckdb.append_timestamp(this.appender, value);
  }
  // TODO: append TIMESTAMPS_S/_MS/_NS?
  // TODO: append TIME_TZ/TIMESTAMP_TZ?
  public appendInterval(value: Interval) {
    duckdb.append_interval(this.appender, value);
  }
  public appendVarchar(value: string) {
    duckdb.append_varchar(this.appender, value);
  }
  public appendBlob(value: Uint8Array) {
    duckdb.append_blob(this.appender, value);
  }
  // TODO: append ENUM?
  // TODO: append nested types? (ARRAY, LIST, STRUCT, MAP, UNION)
  // TODO: append UUID?
  // TODO: append BIT?
  public appendNull() {
    duckdb.append_null(this.appender);
  }
  public appendDataChunk(dataChunk: DuckDBDataChunk) {
    duckdb.append_data_chunk(this.appender, dataChunk.chunk);
  }
}
