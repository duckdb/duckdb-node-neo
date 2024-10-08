import duckdb from '@duckdb/node-bindings';
import { DuckDBPendingResult } from './DuckDBPendingResult';
import { DuckDBResult } from './DuckDBResult';
import { DuckDBTypeId } from './DuckDBTypeId';

export class DuckDBPreparedStatement {
  private readonly prepared_statement: duckdb.PreparedStatement;
  constructor(prepared_statement: duckdb.PreparedStatement) {
    this.prepared_statement = prepared_statement;
  }
  public dispose() {
    duckdb.destroy_prepare(this.prepared_statement);
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
  // TODO: is duckdb_bind_value useful?
  // TODO: get parameter index from name (duckdb_bind_parameter_index)
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
  // TODO: bind HUGEINT
  // TODO: bind DECIMAL
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
  public bindFloat(parameterIndex: number, value: number) {
    duckdb.bind_float(this.prepared_statement, parameterIndex, value);
  }
  public bindDouble(parameterIndex: number, value: number) {
    duckdb.bind_double(this.prepared_statement, parameterIndex, value);
  }
  // TODO: bind DATE
  // TODO: bind TIME
  // TODO: bind TIMESTAMP
  // TODO: bind TIMESTAMPS_S/_MS/_NS?
  // TODO: bind INTERVAL
  public bindVarchar(parameterIndex: number, value: string) {
    duckdb.bind_varchar(this.prepared_statement, parameterIndex, value);
  }
  // TODO: bind BLOB
  // TODO: bind ENUM?
  // TODO: bind nested types? (LIST, STRUCT, MAP, UNION)
  // TODO: bind UUID?
  // TODO: bind BIT?
  public bindNull(parameterIndex: number) {
    duckdb.bind_null(this.prepared_statement, parameterIndex);
  }
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
