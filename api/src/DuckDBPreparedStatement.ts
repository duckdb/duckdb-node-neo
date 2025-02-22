import duckdb from '@duckdb/node-bindings';
import { createValue } from './createValue';
import { DuckDBLogicalType } from './DuckDBLogicalType';
import { DuckDBMaterializedResult } from './DuckDBMaterializedResult';
import { DuckDBPendingResult } from './DuckDBPendingResult';
import { DuckDBResult } from './DuckDBResult';
import { DuckDBResultReader } from './DuckDBResultReader';
import {
  BIT,
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
import { DuckDBTypeId } from './DuckDBTypeId';
import { StatementType } from './enums';
import { typeForValue } from './typeForValue';
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
  public parameterType(parameterIndex: number): DuckDBType {
    return DuckDBLogicalType.create(
      duckdb.param_logical_type(this.prepared_statement, parameterIndex)
    ).asType();
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
  public bindVarInt(parameterIndex: number, value: bigint) {
    this.bindValue(parameterIndex, value, VARINT);
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
    this.bindValue(parameterIndex, value, TIMETZ);
  }
  public bindTimestamp(parameterIndex: number, value: DuckDBTimestampValue) {
    duckdb.bind_timestamp(this.prepared_statement, parameterIndex, value);
  }
  public bindTimestampTZ(
    parameterIndex: number,
    value: DuckDBTimestampTZValue
  ) {
    this.bindValue(parameterIndex, value, TIMESTAMPTZ);
  }
  public bindTimestampSeconds(
    parameterIndex: number,
    value: DuckDBTimestampSecondsValue
  ) {
    this.bindValue(parameterIndex, value, TIMESTAMP_S);
  }
  public bindTimestampMilliseconds(
    parameterIndex: number,
    value: DuckDBTimestampMillisecondsValue
  ) {
    this.bindValue(parameterIndex, value, TIMESTAMP_MS);
  }
  public bindTimestampNanoseconds(
    parameterIndex: number,
    value: DuckDBTimestampNanosecondsValue
  ) {
    this.bindValue(parameterIndex, value, TIMESTAMP_NS);
  }
  public bindInterval(parameterIndex: number, value: DuckDBIntervalValue) {
    duckdb.bind_interval(this.prepared_statement, parameterIndex, value);
  }
  public bindVarchar(parameterIndex: number, value: string) {
    duckdb.bind_varchar(this.prepared_statement, parameterIndex, value);
  }
  public bindBlob(parameterIndex: number, value: Uint8Array) {
    duckdb.bind_blob(this.prepared_statement, parameterIndex, value);
  }
  public bindEnum(parameterIndex: number, value: string, type: DuckDBEnumType) {
    this.bindValue(parameterIndex, value, type);
  }
  public bindArray(
    parameterIndex: number,
    value: DuckDBArrayValue,
    type: DuckDBArrayType
  ) {
    this.bindValue(parameterIndex, value, type);
  }
  public bindList(
    parameterIndex: number,
    value: DuckDBListValue,
    type: DuckDBListType
  ) {
    this.bindValue(parameterIndex, value, type);
  }
  public bindStruct(
    parameterIndex: number,
    value: DuckDBStructValue,
    type: DuckDBStructType
  ) {
    this.bindValue(parameterIndex, value, type);
  }
  // TODO: bind MAP, UNION
  public bindUUID(parameterIndex: number, value: DuckDBUUIDValue) {
    this.bindValue(parameterIndex, value, UUID);
  }
  public bindBit(parameterIndex: number, value: DuckDBBitValue) {
    this.bindValue(parameterIndex, value, BIT);
  }
  public bindNull(parameterIndex: number) {
    duckdb.bind_null(this.prepared_statement, parameterIndex);
  }
  public bindValue(
    parameterIndex: number,
    value: DuckDBValue,
    type: DuckDBType
  ) {
    duckdb.bind_value(
      this.prepared_statement,
      parameterIndex,
      createValue(type, value)
    );
  }
  public bind(
    values: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType | undefined>
  ) {
    if (Array.isArray(values)) {
      const typesIsArray = Array.isArray(types);
      for (let i = 0; i < values.length; i++) {
        this.bindValue(
          i + 1,
          values[i],
          typesIsArray ? types[i] : typeForValue(values[i])
        );
      }
    } else {
      const typesIsRecord = types && !Array.isArray(types);
      for (const key in values) {
        const index = this.parameterIndex(key);
        let type = typesIsRecord ? types[key] : undefined;
        if (type === undefined) {
          type = typeForValue(values[key]);
        }
        this.bindValue(index, values[key], type);
      }
    }
  }
  public async run(): Promise<DuckDBMaterializedResult> {
    return new DuckDBMaterializedResult(
      await duckdb.execute_prepared(this.prepared_statement)
    );
  }
  public async runAndRead(): Promise<DuckDBResultReader> {
    return new DuckDBResultReader(await this.run());
  }
  public async runAndReadAll(): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(await this.run());
    await reader.readAll();
    return reader;
  }
  public async runAndReadUntil(
    targetRowCount: number
  ): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(await this.run());
    await reader.readUntil(targetRowCount);
    return reader;
  }
  public async stream(): Promise<DuckDBResult> {
    return new DuckDBResult(
      await duckdb.execute_prepared_streaming(this.prepared_statement)
    );
  }
  public async streamAndRead(): Promise<DuckDBResultReader> {
    return new DuckDBResultReader(await this.stream());
  }
  public async streamAndReadAll(): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(await this.stream());
    await reader.readAll();
    return reader;
  }
  public async streamAndReadUntil(
    targetRowCount: number
  ): Promise<DuckDBResultReader> {
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
