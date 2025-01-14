import duckdb from '@duckdb/node-bindings';
import { createValue } from './createValue';
import { DuckDBMaterializedResult } from './DuckDBMaterializedResult';
import { DuckDBPendingResult } from './DuckDBPendingResult';
import { DuckDBResult } from './DuckDBResult';
import { DuckDBResultReader } from './DuckDBResultReader';
import { DuckDBTimestampTZType, DuckDBTimeTZType } from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';
import { StatementType } from './enums';
import {
  DuckDBDateValue,
  DuckDBDecimalValue,
  DuckDBIntervalValue,
  DuckDBTimestampTZValue,
  DuckDBTimestampValue,
  DuckDBTimeTZValue,
  DuckDBTimeValue,
} from './values';

export class DuckDBPreparedStatement {
  private readonly prepared_statement: duckdb.PreparedStatement;
  constructor(prepared_statement: duckdb.PreparedStatement) {
    this.prepared_statement = prepared_statement;
  }
  public get statementType(): StatementType {
    return duckdb.prepared_statement_type(this.prepared_statement);
  }
  public get parameterCount(): number {
    return duckdb.nparams(this.prepared_statement);
  }
  public parameterName(parameterIndex: number): string {
    return duckdb.parameter_name(this.prepared_statement, parameterIndex);
  }
  public parameterTypeId(parameterIndex: number): DuckDBTypeId {
    return duckdb.param_type(
      this.prepared_statement,
      parameterIndex
    ) as number as DuckDBTypeId;
  }
  public clearBindings() {
    duckdb.clear_bindings(this.prepared_statement);
  }
  public parameterIndex(parameterName: string): number {
    return duckdb.bind_parameter_index(this.prepared_statement, parameterName);
  }
  public bindBoolean(parameterIndex: number, value: boolean) {
    duckdb.bind_boolean(this.prepared_statement, parameterIndex, value);
  }
  public bindTinyInt(parameterIndex: number, value: number) {
    duckdb.bind_int8(this.prepared_statement, parameterIndex, value);
  }
  public bindSmallInt(parameterIndex: number, value: number) {
    duckdb.bind_int16(this.prepared_statement, parameterIndex, value);
  }
  public bindInteger(parameterIndex: number, value: number) {
    duckdb.bind_int32(this.prepared_statement, parameterIndex, value);
  }
  public bindBigInt(parameterIndex: number, value: bigint) {
    duckdb.bind_int64(this.prepared_statement, parameterIndex, value);
  }
  public bindHugeInt(parameterIndex: number, value: bigint) {
    duckdb.bind_hugeint(this.prepared_statement, parameterIndex, value);
  }
  public bindUTinyInt(parameterIndex: number, value: number) {
    duckdb.bind_uint8(this.prepared_statement, parameterIndex, value);
  }
  public bindUSmallInt(parameterIndex: number, value: number) {
    duckdb.bind_uint16(this.prepared_statement, parameterIndex, value);
  }
  public bindUInteger(parameterIndex: number, value: number) {
    duckdb.bind_uint32(this.prepared_statement, parameterIndex, value);
  }
  public bindUBigInt(parameterIndex: number, value: bigint) {
    duckdb.bind_uint64(this.prepared_statement, parameterIndex, value);
  }
  public bindUHugeInt(parameterIndex: number, value: bigint) {
    duckdb.bind_uhugeint(this.prepared_statement, parameterIndex, value);
  }
  public bindDecimal(parameterIndex: number, value: DuckDBDecimalValue) {
    duckdb.bind_decimal(this.prepared_statement, parameterIndex, value);
  }
  public bindFloat(parameterIndex: number, value: number) {
    duckdb.bind_float(this.prepared_statement, parameterIndex, value);
  }
  public bindDouble(parameterIndex: number, value: number) {
    duckdb.bind_double(this.prepared_statement, parameterIndex, value);
  }
  public bindDate(parameterIndex: number, value: DuckDBDateValue) {
    duckdb.bind_date(this.prepared_statement, parameterIndex, value);
  }
  public bindTime(parameterIndex: number, value: DuckDBTimeValue) {
    duckdb.bind_time(this.prepared_statement, parameterIndex, value);
  }
  public bindTimeTZ(parameterIndex: number, value: DuckDBTimeTZValue) {
    duckdb.bind_value(this.prepared_statement, parameterIndex, createValue(DuckDBTimeTZType.instance, value));
  }
  public bindTimestamp(parameterIndex: number, value: DuckDBTimestampValue) {
    duckdb.bind_timestamp(this.prepared_statement, parameterIndex, value);
  }
  public bindTimestampTZ(parameterIndex: number, value: DuckDBTimestampTZValue) {
    duckdb.bind_value(this.prepared_statement, parameterIndex, createValue(DuckDBTimestampTZType.instance, value));
  }
  // TODO: bind TIMESTAMPS_S/_MS/_NS?  
  public bindInterval(parameterIndex: number, value: DuckDBIntervalValue) {
    duckdb.bind_interval(this.prepared_statement, parameterIndex, value);
  }
  public bindVarchar(parameterIndex: number, value: string) {
    duckdb.bind_varchar(this.prepared_statement, parameterIndex, value);
  }
  public bindBlob(parameterIndex: number, value: Uint8Array) {
    duckdb.bind_blob(this.prepared_statement, parameterIndex, value);
  }
  // TODO: bind ENUM?
  // TODO: bind nested types? (ARRAY, LIST, STRUCT, MAP, UNION) (using bindValue?)
  // TODO: bind UUID?
  // TODO: bind BIT?
  public bindNull(parameterIndex: number) {
    duckdb.bind_null(this.prepared_statement, parameterIndex);
  }
  public async run(): Promise<DuckDBMaterializedResult> {
    return new DuckDBMaterializedResult(await duckdb.execute_prepared(this.prepared_statement));
  }
  public async runAndRead(): Promise<DuckDBResultReader> {
    return new DuckDBResultReader(await this.run());
  }
  public async runAndReadAll(): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(await this.run());
    await reader.readAll();
    return reader;
  }
  public async runAndReadUntil(targetRowCount: number): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(await this.run());
    await reader.readUntil(targetRowCount);
    return reader;
  }
  public async stream(): Promise<DuckDBResult> {
    return new DuckDBResult(await duckdb.execute_prepared_streaming(this.prepared_statement));
  }
  public async streamAndRead(): Promise<DuckDBResultReader> {
    return new DuckDBResultReader(await this.stream());
  }
  public async streamAndReadAll(): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(await this.stream());
    await reader.readAll();
    return reader;
  }
  public async streamAndReadUntil(targetRowCount: number): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(await this.stream());
    await reader.readUntil(targetRowCount);
    return reader;
  }
  public start(): DuckDBPendingResult {
    return new DuckDBPendingResult(
      duckdb.pending_prepared(this.prepared_statement)
    );
  }
  public startStream(): DuckDBPendingResult {
    return new DuckDBPendingResult(
      duckdb.pending_prepared_streaming(this.prepared_statement)
    );
  }
}
