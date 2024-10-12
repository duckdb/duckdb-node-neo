import duckdb from '@duckdb/node-bindings';
import { DuckDBPendingResult } from './DuckDBPendingResult';
import { DuckDBResult } from './DuckDBResult';
import { DuckDBTypeId } from './DuckDBTypeId';
import { Date_, Decimal, Interval, Time, Timestamp } from './DuckDBValue';
import { StatementType } from './enums';

export class DuckDBPreparedStatement {
  private readonly prepared_statement: duckdb.PreparedStatement;
  constructor(prepared_statement: duckdb.PreparedStatement) {
    this.prepared_statement = prepared_statement;
  }
  public dispose() {
    duckdb.destroy_prepare(this.prepared_statement);
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
  public bindDecimal(parameterIndex: number, value: Decimal) {
    duckdb.bind_decimal(this.prepared_statement, parameterIndex, value);
  }
  public bindFloat(parameterIndex: number, value: number) {
    duckdb.bind_float(this.prepared_statement, parameterIndex, value);
  }
  public bindDouble(parameterIndex: number, value: number) {
    duckdb.bind_double(this.prepared_statement, parameterIndex, value);
  }
  public bindDate(parameterIndex: number, value: Date_) {
    duckdb.bind_date(this.prepared_statement, parameterIndex, value);
  }
  public bindTime(parameterIndex: number, value: Time) {
    duckdb.bind_time(this.prepared_statement, parameterIndex, value);
  }
  public bindTimestamp(parameterIndex: number, value: Timestamp) {
    duckdb.bind_timestamp(this.prepared_statement, parameterIndex, value);
  }
  // TODO: bind TIMESTAMPS_S/_MS/_NS?
  // TODO: bind TIME_TZ/TIMESTAMP_TZ?
  public bindInterval(parameterIndex: number, value: Interval) {
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
  // TODO: expose bindValue, or implement bindList, bindStruct, etc.?
  // public bindValue(parameterIndex: number, value: Value) {
  //   duckdb.bind_value(this.prepared_statement, parameterIndex, value);
  // }
  public async run(): Promise<DuckDBResult> {
    return new DuckDBResult(
      await duckdb.execute_prepared(this.prepared_statement)
    );
  }
  public start(): DuckDBPendingResult {
    return new DuckDBPendingResult(
      duckdb.pending_prepared(this.prepared_statement)
    );
  }
}
