// Enums

export const sizeof_bool: number;

export enum PendingState {
  RESULT_READY = 0,
  RESULT_NOT_READY = 1,
  ERROR = 2,
  NO_TASKS_AVAILABLE = 3,
}

export enum ResultType {
  INVALID = 0,
  CHANGED_ROWS = 1,
  NOTHING = 2,
  QUERY_RESULT = 3,
}

export enum StatementType {
  INVALID = 0,
  SELECT = 1,
  INSERT = 2,
  UPDATE = 3,
  EXPLAIN = 4,
  DELETE = 5,
  PREPARE = 6,
  CREATE = 7,
  EXECUTE = 8,
  ALTER = 9,
  TRANSACTION = 10,
  COPY = 11,
  ANALYZE = 12,
  VARIABLE_SET = 13,
  CREATE_FUNC = 14,
  DROP = 15,
  EXPORT = 16,
  PRAGMA = 17,
  VACUUM = 18,
  CALL = 19,
  SET = 20,
  LOAD = 21,
  RELATION = 22,
  EXTENSION = 23,
  LOGICAL_PLAN = 24,
  ATTACH = 25,
  DETACH = 26,
  MULTI = 27,
}

export enum Type {
  INVALID = 0,
  BOOLEAN = 1,
  TINYINT = 2,
  SMALLINT = 3,
  INTEGER = 4,
  BIGINT = 5,
  UTINYINT = 6,
  USMALLINT = 7,
  UINTEGER = 8,
  UBIGINT = 9,
  FLOAT = 10,
  DOUBLE = 11,
  TIMESTAMP = 12,
  DATE = 13,
  TIME = 14,
  INTERVAL = 15,
  HUGEINT = 16,
  UHUGEINT = 32,
  VARCHAR = 17,
  BLOB = 18,
  DECIMAL = 19,
  TIMESTAMP_S = 20,
  TIMESTAMP_MS = 21,
  TIMESTAMP_NS = 22,
  ENUM = 23,
  LIST = 24,
  STRUCT = 25,
  MAP = 26,
  ARRAY = 33,
  UUID = 27,
  UNION = 28,
  BIT = 29,
  TIME_TZ = 30,
  TIMESTAMP_TZ = 31,
  ANY = 34,
  VARINT = 35,
  SQLNULL = 36,
}


// Types (no explicit destroy)

export interface Date_ {
  /** Days since 1970-01-01 */
  days: number;
}
export interface DateParts {
  year: number;
  month: number;
  day: number;
}

export interface Decimal {
  width: number;
  scale: number;
  value: bigint;
}

export interface Interval {
  months: number;
  days: number;
  micros: bigint;
}

export interface QueryProgress {
  percentage: number;
  rows_processed: bigint;
  total_rows_to_process: bigint;
}

export interface Time {
  /** Microseconds since 00:00:00 */
  micros: bigint;
}
export interface TimeParts {
  hour: number;
  min: number;
  sec: number;
  micros: number;
}

export interface TimeTZ {
  /**
  * 40 bits for micros, then 24 bits for encoded offset in seconds.
  * 
  * Max absolute unencoded offset = 15:59:59 = 60 * (60 * 15 + 59) + 59 = 57599.
  * 
  * Encoded offset is unencoded offset inverted then shifted (by +57599) to unsigned.
  * 
  * Max unencoded offset = 57599 -> -57599 -> 0 encoded.
  * 
  * Min unencoded offset = -57599 -> 57599 -> 115198 encoded.
  */
  bits: bigint;
}
export interface TimeTZParts {
  time: TimeParts;
  /** Offset in seconds, from -15:59:59 = -57599 to 15:59:59 = 57599 */
  offset: number;
}

export interface Timestamp {
  /** Microseconds since 1970-01-01 */
  micros: bigint;
}
export interface TimestampSeconds {
  /** Seconds since 1970-01-01 */
  seconds: bigint;
}
export interface TimestampMilliseconds {
  /** Milliseconds since 1970-01-01 */
  millis: bigint;
}
export interface TimestampNanoseconds {
  /** Nanoseconds since 1970-01-01 */
  nanos: bigint;
}
export interface TimestampParts {
  date: DateParts;
  time: TimeParts;
}

export interface FunctionInfo {
  __duckdb_type: 'duckdb_function_info';
}

export interface Vector {
  __duckdb_type: 'duckdb_vector';
}

// Types (explicit destroy)

export interface Appender {
  __duckdb_type: 'duckdb_appender';
}

// export interface ClientContext {
//   __duckdb_type: 'duckdb_client_context';
// }

export interface Config {
  __duckdb_type: 'duckdb_config';
}

export interface Connection {
  __duckdb_type: 'duckdb_connection';
}

// export interface CreateTypeInfo {
//   __duckdb_type: 'duckdb_create_type_info';
// }

export interface Database {
  __duckdb_type: 'duckdb_database';
}

export interface DataChunk {
  __duckdb_type: 'duckdb_data_chunk';
}

export interface ExtractedStatements {
  __duckdb_type: 'duckdb_extracted_statements';
}

export interface InstanceCache {
  __duckdb_type: 'duckdb_instance_cache';
}

export interface LogicalType {
  __duckdb_type: 'duckdb_logical_type';
}

export interface PendingResult {
  __duckdb_type: 'duckdb_pending_result';
}

export interface PreparedStatement {
  __duckdb_type: 'duckdb_prepared_statement';
}

export interface Result {
  __duckdb_type: 'duckdb_result';
}

export interface ScalarFunction {
  __duckdb_type: 'duckdb_scalar_function';
}

// export interface SelectionVector {
//   __duckdb_type: 'duckdb_selection_vector';
// }

export interface Value {
  __duckdb_type: 'duckdb_value';
}

// Types (TypeScript only)

export interface ConfigFlag {
  name: string;
  description: string;
}

export interface ExtractedStatementsAndCount {
  extracted_statements: ExtractedStatements;
  statement_count: number;
}

export type ScalarFunctionMainFunction = (info: FunctionInfo, input: DataChunk, output: Vector) => void;

// Functions

// DUCKDB_C_API duckdb_instance_cache duckdb_create_instance_cache();
export function create_instance_cache(): InstanceCache;

// DUCKDB_C_API duckdb_state duckdb_get_or_create_from_cache(duckdb_instance_cache instance_cache, const char *path, duckdb_database *out_database, duckdb_config config, char **out_error);
export function get_or_create_from_cache(cache: InstanceCache, path?: string, config?: Config): Promise<Database>;

// DUCKDB_C_API void duckdb_destroy_instance_cache(duckdb_instance_cache *instance_cache);
// not exposed: destroyed in finalizer

// DUCKDB_C_API duckdb_state duckdb_open(const char *path, duckdb_database *out_database);
export function open(path?: string, config?: Config): Promise<Database>;

// DUCKDB_C_API duckdb_state duckdb_open_ext(const char *path, duckdb_database *out_database, duckdb_config config, char **out_error);
// not exposed: consolidated into open

// DUCKDB_C_API void duckdb_close(duckdb_database *database);
export function close_sync(database: Database): void;

// DUCKDB_C_API duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection);
export function connect(database: Database): Promise<Connection>;

// DUCKDB_C_API void duckdb_interrupt(duckdb_connection connection);
export function interrupt(connection: Connection): void;

// DUCKDB_C_API duckdb_query_progress_type duckdb_query_progress(duckdb_connection connection);
export function query_progress(connection: Connection): QueryProgress;

// DUCKDB_C_API void duckdb_disconnect(duckdb_connection *connection);
export function disconnect_sync(connection: Connection): void;

// DUCKDB_C_API void duckdb_connection_get_client_context(duckdb_connection connection, duckdb_client_context *out_context);

// DUCKDB_C_API idx_t duckdb_client_context_get_connection_id(duckdb_client_context context);

// DUCKDB_C_API void duckdb_destroy_client_context(duckdb_client_context *context);

// DUCKDB_C_API const char *duckdb_library_version();
export function library_version(): string;

// DUCKDB_C_API duckdb_value duckdb_get_table_names(duckdb_connection connection, const char *query, bool qualified);

// DUCKDB_C_API duckdb_state duckdb_create_config(duckdb_config *out_config);
export function create_config(): Config;

// DUCKDB_C_API size_t duckdb_config_count();
export function config_count(): number;

// DUCKDB_C_API duckdb_state duckdb_get_config_flag(size_t index, const char **out_name, const char **out_description);
export function get_config_flag(index: number): ConfigFlag;

// DUCKDB_C_API duckdb_state duckdb_set_config(duckdb_config config, const char *name, const char *option);
export function set_config(config: Config, name: string, option: string): void;

// DUCKDB_C_API void duckdb_destroy_config(duckdb_config *config);
// not exposed: destroyed in finalizer

// DUCKDB_C_API duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out_result);
export function query(connection: Connection, query: string): Promise<Result>;

// DUCKDB_C_API void duckdb_destroy_result(duckdb_result *result);
// not exposed: destroyed in finalizer

// DUCKDB_C_API const char *duckdb_column_name(duckdb_result *result, idx_t col);
export function column_name(result: Result, column_index: number): string;

// DUCKDB_C_API duckdb_type duckdb_column_type(duckdb_result *result, idx_t col);
export function column_type(result: Result, column_index: number): Type;

// DUCKDB_C_API duckdb_statement_type duckdb_result_statement_type(duckdb_result result);
export function result_statement_type(result: Result): StatementType;

// DUCKDB_C_API duckdb_logical_type duckdb_column_logical_type(duckdb_result *result, idx_t col);
export function column_logical_type(result: Result, column_index: number): LogicalType;

// DUCKDB_C_API idx_t duckdb_column_count(duckdb_result *result);
export function column_count(result: Result): number;

// #ifndef DUCKDB_API_NO_DEPRECATED

// DUCKDB_C_API idx_t duckdb_row_count(duckdb_result *result);
export function row_count(result: Result): number;

// #endif

// DUCKDB_C_API idx_t duckdb_rows_changed(duckdb_result *result);
export function rows_changed(result: Result): number;

// #ifndef DUCKDB_API_NO_DEPRECATED
// DUCKDB_C_API void *duckdb_column_data(duckdb_result *result, idx_t col);
// DUCKDB_C_API bool *duckdb_nullmask_data(duckdb_result *result, idx_t col);
// #endif

// DUCKDB_C_API const char *duckdb_result_error(duckdb_result *result);
// not exposed: query, execute_prepared, and execute_pending reject promise with error

// DUCKDB_C_API duckdb_error_type duckdb_result_error_type(duckdb_result *result);
// not exposed: query, execute_prepared, and execute_pending reject promise with error

// #ifndef DUCKDB_API_NO_DEPRECATED

// DUCKDB_C_API duckdb_data_chunk duckdb_result_get_chunk(duckdb_result result, idx_t chunk_index);
export function result_get_chunk(result: Result, chunkIndex: number): DataChunk;

// DUCKDB_C_API bool duckdb_result_is_streaming(duckdb_result result);
export function result_is_streaming(result: Result): boolean;

// DUCKDB_C_API idx_t duckdb_result_chunk_count(duckdb_result result);
export function result_chunk_count(result: Result): number;

// #endif

// DUCKDB_C_API duckdb_result_type duckdb_result_return_type(duckdb_result result);
export function result_return_type(result: Result): ResultType;

// #ifndef DUCKDB_API_NO_DEPRECATED
// DUCKDB_C_API bool duckdb_value_boolean(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API int8_t duckdb_value_int8(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API int16_t duckdb_value_int16(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API int32_t duckdb_value_int32(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API int64_t duckdb_value_int64(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API duckdb_hugeint duckdb_value_hugeint(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API duckdb_uhugeint duckdb_value_uhugeint(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API duckdb_decimal duckdb_value_decimal(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API uint8_t duckdb_value_uint8(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API uint16_t duckdb_value_uint16(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API uint32_t duckdb_value_uint32(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API uint64_t duckdb_value_uint64(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API float duckdb_value_float(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API double duckdb_value_double(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API duckdb_date duckdb_value_date(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API duckdb_time duckdb_value_time(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API duckdb_timestamp duckdb_value_timestamp(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API duckdb_interval duckdb_value_interval(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API char *duckdb_value_varchar(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API duckdb_string duckdb_value_string(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API char *duckdb_value_varchar_internal(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API duckdb_string duckdb_value_string_internal(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API duckdb_blob duckdb_value_blob(duckdb_result *result, idx_t col, idx_t row);
// DUCKDB_C_API bool duckdb_value_is_null(duckdb_result *result, idx_t col, idx_t row);
// #endif

// DUCKDB_C_API void *duckdb_malloc(size_t size);
// not exposed: only used internally

// DUCKDB_C_API void duckdb_free(void *ptr);
// not exposed: only user internally

// DUCKDB_C_API idx_t duckdb_vector_size();
export function vector_size(): number;

// DUCKDB_C_API bool duckdb_string_is_inlined(duckdb_string_t string);
// not exposed: handled internally

// DUCKDB_C_API uint32_t duckdb_string_t_length(duckdb_string_t string);
// not exposed: handled internally

// DUCKDB_C_API const char *duckdb_string_t_data(duckdb_string_t *string);
// not exposed: handled internally

// DUCKDB_C_API duckdb_date_struct duckdb_from_date(duckdb_date date);
export function from_date(date: Date_): DateParts;

// DUCKDB_C_API duckdb_date duckdb_to_date(duckdb_date_struct date);
export function to_date(parts: DateParts): Date_;

// DUCKDB_C_API bool duckdb_is_finite_date(duckdb_date date);
export function is_finite_date(date: Date_): boolean;

// DUCKDB_C_API duckdb_time_struct duckdb_from_time(duckdb_time time);
export function from_time(time: Time): TimeParts;

// DUCKDB_C_API duckdb_time_tz duckdb_create_time_tz(int64_t micros, int32_t offset);
export function create_time_tz(micros: number, offset: number): TimeTZ;

// DUCKDB_C_API duckdb_time_tz_struct duckdb_from_time_tz(duckdb_time_tz micros);
export function from_time_tz(time_tz: TimeTZ): TimeTZParts;

// DUCKDB_C_API duckdb_time duckdb_to_time(duckdb_time_struct time);
export function to_time(parts: TimeParts): Time;

// DUCKDB_C_API duckdb_timestamp_struct duckdb_from_timestamp(duckdb_timestamp ts);
export function from_timestamp(timestamp: Timestamp): TimestampParts;

// DUCKDB_C_API duckdb_timestamp duckdb_to_timestamp(duckdb_timestamp_struct ts);
export function to_timestamp(parts: TimestampParts): Timestamp;

// DUCKDB_C_API bool duckdb_is_finite_timestamp(duckdb_timestamp ts);
export function is_finite_timestamp(timestamp: Timestamp): boolean;

// DUCKDB_C_API bool duckdb_is_finite_timestamp_s(duckdb_timestamp_s ts);
export function is_finite_timestamp_s(timestampSeconds: TimestampSeconds): boolean;

// DUCKDB_C_API bool duckdb_is_finite_timestamp_ms(duckdb_timestamp_ms ts);
export function is_finite_timestamp_ms(timestampMilliseconds: TimestampMilliseconds): boolean;

// DUCKDB_C_API bool duckdb_is_finite_timestamp_ns(duckdb_timestamp_ns ts);
export function is_finite_timestamp_ns(timestampNanoseconds: TimestampNanoseconds): boolean;

// DUCKDB_C_API double duckdb_hugeint_to_double(duckdb_hugeint val);
export function hugeint_to_double(hugeint: bigint): number;

// DUCKDB_C_API duckdb_hugeint duckdb_double_to_hugeint(double val);
export function double_to_hugeint(double: number): bigint;

// DUCKDB_C_API double duckdb_uhugeint_to_double(duckdb_uhugeint val);
export function uhugeint_to_double(uhugeint: bigint): number;

// DUCKDB_C_API duckdb_uhugeint duckdb_double_to_uhugeint(double val);
export function double_to_uhugeint(double: number): bigint;

// DUCKDB_C_API duckdb_decimal duckdb_double_to_decimal(double val, uint8_t width, uint8_t scale);
export function double_to_decimal(double: number, width: number, scale: number): Decimal;

// DUCKDB_C_API double duckdb_decimal_to_double(duckdb_decimal val);
export function decimal_to_double(decimal: Decimal): number;

// DUCKDB_C_API duckdb_state duckdb_prepare(duckdb_connection connection, const char *query, duckdb_prepared_statement *out_prepared_statement);
export function prepare(connection: Connection, query: string): Promise<PreparedStatement>;

// DUCKDB_C_API void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement);
export function destroy_prepare_sync(prepared_statement: PreparedStatement): void;

// DUCKDB_C_API const char *duckdb_prepare_error(duckdb_prepared_statement prepared_statement);
// not exposed: prepare rejects promise with error

// DUCKDB_C_API idx_t duckdb_nparams(duckdb_prepared_statement prepared_statement);
export function nparams(prepared_statement: PreparedStatement): number;

// DUCKDB_C_API const char *duckdb_parameter_name(duckdb_prepared_statement prepared_statement, idx_t index);
export function parameter_name(prepared_statement: PreparedStatement, index: number): string;

// DUCKDB_C_API duckdb_type duckdb_param_type(duckdb_prepared_statement prepared_statement, idx_t param_idx);
export function param_type(prepared_statement: PreparedStatement, index: number): Type;

// DUCKDB_C_API duckdb_logical_type duckdb_param_logical_type(duckdb_prepared_statement prepared_statement, idx_t param_idx);
export function param_logical_type(prepared_statement: PreparedStatement, index: number): LogicalType;

// DUCKDB_C_API duckdb_state duckdb_clear_bindings(duckdb_prepared_statement prepared_statement);
export function clear_bindings(prepared_statement: PreparedStatement): void;

// DUCKDB_C_API duckdb_statement_type duckdb_prepared_statement_type(duckdb_prepared_statement statement);
export function prepared_statement_type(prepared_statement: PreparedStatement): StatementType;

// DUCKDB_C_API duckdb_state duckdb_bind_value(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_value val);
export function bind_value(prepared_statement: PreparedStatement, index: number, value: Value): void;

// DUCKDB_C_API duckdb_state duckdb_bind_parameter_index(duckdb_prepared_statement prepared_statement, idx_t *param_idx_out, const char *name);
export function bind_parameter_index(prepared_statement: PreparedStatement, name: string): number;

// DUCKDB_C_API duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val);
export function bind_boolean(prepared_statement: PreparedStatement, index: number, bool: boolean): void;

// DUCKDB_C_API duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val);
export function bind_int8(prepared_statement: PreparedStatement, index: number, int8: number): void;

// DUCKDB_C_API duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val);
export function bind_int16(prepared_statement: PreparedStatement, index: number, int16: number): void;

// DUCKDB_C_API duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t val);
export function bind_int32(prepared_statement: PreparedStatement, index: number, int32: number): void;

// DUCKDB_C_API duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t val);
export function bind_int64(prepared_statement: PreparedStatement, index: number, int64: bigint): void;

// DUCKDB_C_API duckdb_state duckdb_bind_hugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_hugeint val);
export function bind_hugeint(prepared_statement: PreparedStatement, index: number, hugeint: bigint): void;

// DUCKDB_C_API duckdb_state duckdb_bind_uhugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_uhugeint val);
export function bind_uhugeint(prepared_statement: PreparedStatement, index: number, uhugeint: bigint): void;

// DUCKDB_C_API duckdb_state duckdb_bind_decimal(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_decimal val);
export function bind_decimal(prepared_statement: PreparedStatement, index: number, decimal: Decimal): void;

// DUCKDB_C_API duckdb_state duckdb_bind_uint8(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint8_t val);
export function bind_uint8(prepared_statement: PreparedStatement, index: number, uint8: number): void;

// DUCKDB_C_API duckdb_state duckdb_bind_uint16(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint16_t val);
export function bind_uint16(prepared_statement: PreparedStatement, index: number, uint16: number): void;

// DUCKDB_C_API duckdb_state duckdb_bind_uint32(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint32_t val);
export function bind_uint32(prepared_statement: PreparedStatement, index: number, uint32: number): void;

// DUCKDB_C_API duckdb_state duckdb_bind_uint64(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint64_t val);
export function bind_uint64(prepared_statement: PreparedStatement, index: number, uint64: bigint): void;

// DUCKDB_C_API duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val);
export function bind_float(prepared_statement: PreparedStatement, index: number, float: number): void;

// DUCKDB_C_API duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val);
export function bind_double(prepared_statement: PreparedStatement, index: number, double: number): void;

// DUCKDB_C_API duckdb_state duckdb_bind_date(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_date val);
export function bind_date(prepared_statement: PreparedStatement, index: number, date: Date_): void;

// DUCKDB_C_API duckdb_state duckdb_bind_time(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_time val);
export function bind_time(prepared_statement: PreparedStatement, index: number, time: Time): void;

// DUCKDB_C_API duckdb_state duckdb_bind_timestamp(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_timestamp val);
export function bind_timestamp(prepared_statement: PreparedStatement, index: number, timestamp: Timestamp): void;

// DUCKDB_C_API duckdb_state duckdb_bind_timestamp_tz(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_timestamp val);
export function bind_timestamp_tz(prepared_statement: PreparedStatement, index: number, timestamp: Timestamp): void;

// DUCKDB_C_API duckdb_state duckdb_bind_interval(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_interval val);
export function bind_interval(prepared_statement: PreparedStatement, index: number, interval: Interval): void;

// DUCKDB_C_API duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val);
export function bind_varchar(prepared_statement: PreparedStatement, index: number, varchar: string): void;

// DUCKDB_C_API duckdb_state duckdb_bind_varchar_length(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val, idx_t length);
// not exposed: JS string includes length

// DUCKDB_C_API duckdb_state duckdb_bind_blob(duckdb_prepared_statement prepared_statement, idx_t param_idx, const void *data, idx_t length);
export function bind_blob(prepared_statement: PreparedStatement, index: number, data: Uint8Array): void;

// DUCKDB_C_API duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx);
export function bind_null(prepared_statement: PreparedStatement, index: number): void;

// DUCKDB_C_API duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement, duckdb_result *out_result);
export function execute_prepared(prepared_statement: PreparedStatement): Promise<Result>;

// #ifndef DUCKDB_API_NO_DEPRECATED

// DUCKDB_C_API duckdb_state duckdb_execute_prepared_streaming(duckdb_prepared_statement prepared_statement, duckdb_result *out_result);
export function execute_prepared_streaming(prepared_statement: PreparedStatement): Promise<Result>;

// #endif

// DUCKDB_C_API idx_t duckdb_extract_statements(duckdb_connection connection, const char *query, duckdb_extracted_statements *out_extracted_statements);
export function extract_statements(connection: Connection, query: string): Promise<ExtractedStatementsAndCount>;

// DUCKDB_C_API duckdb_state duckdb_prepare_extracted_statement(duckdb_connection connection, duckdb_extracted_statements extracted_statements, idx_t index, duckdb_prepared_statement *out_prepared_statement);
export function prepare_extracted_statement(connection: Connection, extracted_statements: ExtractedStatements, index: number): Promise<PreparedStatement>;

// DUCKDB_C_API const char *duckdb_extract_statements_error(duckdb_extracted_statements extracted_statements);
export function extract_statements_error(extracted_statements: ExtractedStatements): string;

// DUCKDB_C_API void duckdb_destroy_extracted(duckdb_extracted_statements *extracted_statements);
// not exposed: destroyed in finalizer

// DUCKDB_C_API duckdb_state duckdb_pending_prepared(duckdb_prepared_statement prepared_statement, duckdb_pending_result *out_result);
export function pending_prepared(prepared_statement: PreparedStatement): PendingResult;

// #ifndef DUCKDB_API_NO_DEPRECATED

// DUCKDB_C_API duckdb_state duckdb_pending_prepared_streaming(duckdb_prepared_statement prepared_statement, duckdb_pending_result *out_result);
export function pending_prepared_streaming(prepared_statement: PreparedStatement): PendingResult;

// #endif

// DUCKDB_C_API void duckdb_destroy_pending(duckdb_pending_result *pending_result);
// not exposed: destroyed in finalizer

// DUCKDB_C_API const char *duckdb_pending_error(duckdb_pending_result pending_result);
export function pending_error(pending_result: PendingResult): string;

// DUCKDB_C_API duckdb_pending_state duckdb_pending_execute_task(duckdb_pending_result pending_result);
export function pending_execute_task(pending_result: PendingResult): PendingState;

// DUCKDB_C_API duckdb_pending_state duckdb_pending_execute_check_state(duckdb_pending_result pending_result);
export function pending_execute_check_state(pending_resulit: PendingResult): PendingState;

// DUCKDB_C_API duckdb_state duckdb_execute_pending(duckdb_pending_result pending_result, duckdb_result *out_result);
export function execute_pending(pending_result: PendingResult): Promise<Result>;

// DUCKDB_C_API bool duckdb_pending_execution_is_finished(duckdb_pending_state pending_state);
export function pending_execution_is_finished(pending_state: PendingState): boolean;

// DUCKDB_C_API void duckdb_destroy_value(duckdb_value *value);
// not exposed: destroyed in finalizer

// DUCKDB_C_API duckdb_value duckdb_create_varchar(const char *text);
export function create_varchar(text: string): Value;

// DUCKDB_C_API duckdb_value duckdb_create_varchar_length(const char *text, idx_t length);
// not exposed: JS string includes length

// DUCKDB_C_API duckdb_value duckdb_create_bool(bool input);
export function create_bool(input: boolean): Value;

// DUCKDB_C_API duckdb_value duckdb_create_int8(int8_t input);
export function create_int8(input: number): Value;

// DUCKDB_C_API duckdb_value duckdb_create_uint8(uint8_t input);
export function create_uint8(input: number): Value;

// DUCKDB_C_API duckdb_value duckdb_create_int16(int16_t input);
export function create_int16(input: number): Value;

// DUCKDB_C_API duckdb_value duckdb_create_uint16(uint16_t input);
export function create_uint16(input: number): Value;

// DUCKDB_C_API duckdb_value duckdb_create_int32(int32_t input);
export function create_int32(input: number): Value;

// DUCKDB_C_API duckdb_value duckdb_create_uint32(uint32_t input);
export function create_uint32(input: number): Value;

// DUCKDB_C_API duckdb_value duckdb_create_uint64(uint64_t input);
export function create_uint64(input: bigint): Value;

// DUCKDB_C_API duckdb_value duckdb_create_int64(int64_t val);
export function create_int64(input: bigint): Value;

// DUCKDB_C_API duckdb_value duckdb_create_hugeint(duckdb_hugeint input);
export function create_hugeint(input: bigint): Value;

// DUCKDB_C_API duckdb_value duckdb_create_uhugeint(duckdb_uhugeint input);
export function create_uhugeint(input: bigint): Value;

// DUCKDB_C_API duckdb_value duckdb_create_varint(duckdb_varint input);
export function create_varint(input: bigint): Value;

// DUCKDB_C_API duckdb_value duckdb_create_decimal(duckdb_decimal input);
export function create_decimal(input: Decimal): Value;

// DUCKDB_C_API duckdb_value duckdb_create_float(float input);
export function create_float(input: number): Value;

// DUCKDB_C_API duckdb_value duckdb_create_double(double input);
export function create_double(input: number): Value;

// DUCKDB_C_API duckdb_value duckdb_create_date(duckdb_date input);
export function create_date(input: Date_): Value;

// DUCKDB_C_API duckdb_value duckdb_create_time(duckdb_time input);
export function create_time(input: Time): Value;

// DUCKDB_C_API duckdb_value duckdb_create_time_tz_value(duckdb_time_tz value);
export function create_time_tz_value(input: TimeTZ): Value;

// DUCKDB_C_API duckdb_value duckdb_create_timestamp(duckdb_timestamp input);
export function create_timestamp(input: Timestamp): Value;

// DUCKDB_C_API duckdb_value duckdb_create_timestamp_tz(duckdb_timestamp input);
export function create_timestamp_tz(input: Timestamp): Value;

// DUCKDB_C_API duckdb_value duckdb_create_timestamp_s(duckdb_timestamp_s input);
export function create_timestamp_s(input: TimestampSeconds): Value;

// DUCKDB_C_API duckdb_value duckdb_create_timestamp_ms(duckdb_timestamp_ms input);
export function create_timestamp_ms(input: TimestampMilliseconds): Value;

// DUCKDB_C_API duckdb_value duckdb_create_timestamp_ns(duckdb_timestamp_ns input);
export function create_timestamp_ns(input: TimestampNanoseconds): Value;

// DUCKDB_C_API duckdb_value duckdb_create_interval(duckdb_interval input);
export function create_interval(input: Interval): Value;

// DUCKDB_C_API duckdb_value duckdb_create_blob(const uint8_t *data, idx_t length);
export function create_blob(data: Uint8Array): Value;

// DUCKDB_C_API duckdb_value duckdb_create_bit(duckdb_bit input);
export function create_bit(data: Uint8Array): Value;

// DUCKDB_C_API duckdb_value duckdb_create_uuid(duckdb_uhugeint input);
export function create_uuid(input: bigint): Value;

// DUCKDB_C_API bool duckdb_get_bool(duckdb_value val);
export function get_bool(value: Value): boolean;

// DUCKDB_C_API int8_t duckdb_get_int8(duckdb_value val);
export function get_int8(value: Value): number;

// DUCKDB_C_API uint8_t duckdb_get_uint8(duckdb_value val);
export function get_uint8(value: Value): number;

// DUCKDB_C_API int16_t duckdb_get_int16(duckdb_value val);
export function get_int16(value: Value): number;

// DUCKDB_C_API uint16_t duckdb_get_uint16(duckdb_value val);
export function get_uint16(value: Value): number;

// DUCKDB_C_API int32_t duckdb_get_int32(duckdb_value val);
export function get_int32(value: Value): number;

// DUCKDB_C_API uint32_t duckdb_get_uint32(duckdb_value val);
export function get_uint32(value: Value): number;

// DUCKDB_C_API int64_t duckdb_get_int64(duckdb_value val);
export function get_int64(value: Value): bigint;

// DUCKDB_C_API uint64_t duckdb_get_uint64(duckdb_value val);
export function get_uint64(value: Value): bigint;

// DUCKDB_C_API duckdb_hugeint duckdb_get_hugeint(duckdb_value val);
export function get_hugeint(value: Value): bigint;

// DUCKDB_C_API duckdb_uhugeint duckdb_get_uhugeint(duckdb_value val);
export function get_uhugeint(value: Value): bigint;

// DUCKDB_C_API duckdb_varint duckdb_get_varint(duckdb_value val);
export function get_varint(value: Value): bigint;

// DUCKDB_C_API duckdb_decimal duckdb_get_decimal(duckdb_value val);
export function get_decimal(value: Value): Decimal;

// DUCKDB_C_API float duckdb_get_float(duckdb_value val);
export function get_float(value: Value): number;

// DUCKDB_C_API double duckdb_get_double(duckdb_value val);
export function get_double(value: Value): number;

// DUCKDB_C_API duckdb_date duckdb_get_date(duckdb_value val);
export function get_date(value: Value): Date_;

// DUCKDB_C_API duckdb_time duckdb_get_time(duckdb_value val);
export function get_time(value: Value): Time;

// DUCKDB_C_API duckdb_time_tz duckdb_get_time_tz(duckdb_value val);
export function get_time_tz(value: Value): TimeTZ;

// DUCKDB_C_API duckdb_timestamp duckdb_get_timestamp(duckdb_value val);
export function get_timestamp(value: Value): Timestamp;

// DUCKDB_C_API duckdb_timestamp duckdb_get_timestamp_tz(duckdb_value val);
export function get_timestamp_tz(value: Value): Timestamp;

// DUCKDB_C_API duckdb_timestamp_s duckdb_get_timestamp_s(duckdb_value val);
export function get_timestamp_s(value: Value): TimestampSeconds;

// DUCKDB_C_API duckdb_timestamp_ms duckdb_get_timestamp_ms(duckdb_value val);
export function get_timestamp_ms(value: Value): TimestampMilliseconds;

// DUCKDB_C_API duckdb_timestamp_ns duckdb_get_timestamp_ns(duckdb_value val);
export function get_timestamp_ns(value: Value): TimestampNanoseconds;

// DUCKDB_C_API duckdb_interval duckdb_get_interval(duckdb_value val);
export function get_interval(value: Value): Interval;

// DUCKDB_C_API duckdb_logical_type duckdb_get_value_type(duckdb_value val);
export function get_value_type(value: Value): LogicalType;

// DUCKDB_C_API duckdb_blob duckdb_get_blob(duckdb_value val);
export function get_blob(value: Value): Uint8Array;

// DUCKDB_C_API duckdb_bit duckdb_get_bit(duckdb_value val);
export function get_bit(value: Value): Uint8Array;

// DUCKDB_C_API duckdb_uhugeint duckdb_get_uuid(duckdb_value val);
export function get_uuid(value: Value): bigint;

// DUCKDB_C_API char *duckdb_get_varchar(duckdb_value value);
export function get_varchar(value: Value): string;

// DUCKDB_C_API duckdb_value duckdb_create_struct_value(duckdb_logical_type type, duckdb_value *values);
export function create_struct_value(logical_type: LogicalType, values: readonly Value[]): Value;

// DUCKDB_C_API duckdb_value duckdb_create_list_value(duckdb_logical_type type, duckdb_value *values, idx_t value_count);
export function create_list_value(logical_type: LogicalType, values: readonly Value[]): Value;

// DUCKDB_C_API duckdb_value duckdb_create_array_value(duckdb_logical_type type, duckdb_value *values, idx_t value_count);
export function create_array_value(logical_type: LogicalType, values: readonly Value[]): Value;

// DUCKDB_C_API duckdb_value duckdb_create_map_value(duckdb_logical_type map_type, duckdb_value *keys, duckdb_value *values, idx_t entry_count);
export function create_map_value(map_type: LogicalType, keys: readonly Value[], values: readonly Value[]): Value;

// DUCKDB_C_API duckdb_value duckdb_create_union_value(duckdb_logical_type union_type, idx_t tag_index, duckdb_value value);
export function create_union_value(union_type: LogicalType, tag_index: number, value: Value): Value;

// DUCKDB_C_API idx_t duckdb_get_map_size(duckdb_value value);
export function get_map_size(value: Value): number;

// DUCKDB_C_API duckdb_value duckdb_get_map_key(duckdb_value value, idx_t index);
export function get_map_key(value: Value, index: number): Value;

// DUCKDB_C_API duckdb_value duckdb_get_map_value(duckdb_value value, idx_t index);
export function get_map_value(value: Value, index: number): Value;

// DUCKDB_C_API bool duckdb_is_null_value(duckdb_value value);
export function is_null_value(value: Value): boolean;

// DUCKDB_C_API duckdb_value duckdb_create_null_value();
export function create_null_value(): Value;

// DUCKDB_C_API idx_t duckdb_get_list_size(duckdb_value value);
export function get_list_size(value: Value): number;

// DUCKDB_C_API duckdb_value duckdb_get_list_child(duckdb_value value, idx_t index);
export function get_list_child(value: Value, index: number): Value;

// DUCKDB_C_API duckdb_value duckdb_create_enum_value(duckdb_logical_type type, uint64_t value);
export function create_enum_value(logical_type: LogicalType, value: number): Value;

// DUCKDB_C_API uint64_t duckdb_get_enum_value(duckdb_value value);
export function get_enum_value(value: Value): number;

// DUCKDB_C_API duckdb_value duckdb_get_struct_child(duckdb_value value, idx_t index);
export function get_struct_child(value: Value, index: number): Value;

// DUCKDB_C_API char *duckdb_value_to_string(duckdb_value value);

// DUCKDB_C_API duckdb_logical_type duckdb_create_logical_type(duckdb_type type);
export function create_logical_type(type: Type): LogicalType;

// DUCKDB_C_API char *duckdb_logical_type_get_alias(duckdb_logical_type type);
export function logical_type_get_alias(logical_type: LogicalType): string | null;

// DUCKDB_C_API void duckdb_logical_type_set_alias(duckdb_logical_type type, const char *alias);
export function logical_type_set_alias(logical_type: LogicalType, alias: string): void;

// DUCKDB_C_API duckdb_logical_type duckdb_create_list_type(duckdb_logical_type type);
export function create_list_type(logical_type: LogicalType): LogicalType;

// DUCKDB_C_API duckdb_logical_type duckdb_create_array_type(duckdb_logical_type type, idx_t array_size);
export function create_array_type(logical_type: LogicalType, array_size: number): LogicalType;

// DUCKDB_C_API duckdb_logical_type duckdb_create_map_type(duckdb_logical_type key_type, duckdb_logical_type value_type);
export function create_map_type(key_type: LogicalType, value_type: LogicalType): LogicalType;

// DUCKDB_C_API duckdb_logical_type duckdb_create_union_type(duckdb_logical_type *member_types, const char **member_names, idx_t member_count);
export function create_union_type(member_types: readonly LogicalType[], member_names: readonly string[]): LogicalType;

// DUCKDB_C_API duckdb_logical_type duckdb_create_struct_type(duckdb_logical_type *member_types, const char **member_names, idx_t member_count);
export function create_struct_type(member_types: readonly LogicalType[], member_names: readonly string[]): LogicalType;

// DUCKDB_C_API duckdb_logical_type duckdb_create_enum_type(const char **member_names, idx_t member_count);
export function create_enum_type(member_names: readonly string[]): LogicalType;

// DUCKDB_C_API duckdb_logical_type duckdb_create_decimal_type(uint8_t width, uint8_t scale);
export function create_decimal_type(width: number, scale: number): LogicalType;

// DUCKDB_C_API duckdb_type duckdb_get_type_id(duckdb_logical_type type);
export function get_type_id(logical_type: LogicalType): Type;

// DUCKDB_C_API uint8_t duckdb_decimal_width(duckdb_logical_type type);
export function decimal_width(logical_type: LogicalType): number;

// DUCKDB_C_API uint8_t duckdb_decimal_scale(duckdb_logical_type type);
export function decimal_scale(logical_type: LogicalType): number;

// DUCKDB_C_API duckdb_type duckdb_decimal_internal_type(duckdb_logical_type type);
export function decimal_internal_type(logical_type: LogicalType): Type;

// DUCKDB_C_API duckdb_type duckdb_enum_internal_type(duckdb_logical_type type);
export function enum_internal_type(logical_type: LogicalType): Type;

// DUCKDB_C_API uint32_t duckdb_enum_dictionary_size(duckdb_logical_type type);
export function enum_dictionary_size(logical_type: LogicalType): number;

// DUCKDB_C_API char *duckdb_enum_dictionary_value(duckdb_logical_type type, idx_t index);
export function enum_dictionary_value(logical_type: LogicalType, index: number): string;

// DUCKDB_C_API duckdb_logical_type duckdb_list_type_child_type(duckdb_logical_type type);
export function list_type_child_type(logical_type: LogicalType): LogicalType;

// DUCKDB_C_API duckdb_logical_type duckdb_array_type_child_type(duckdb_logical_type type);
export function array_type_child_type(logical_type: LogicalType): LogicalType;

// DUCKDB_C_API idx_t duckdb_array_type_array_size(duckdb_logical_type type);
export function array_type_array_size(logical_type: LogicalType): number;

// DUCKDB_C_API duckdb_logical_type duckdb_map_type_key_type(duckdb_logical_type type);
export function map_type_key_type(logical_type: LogicalType): LogicalType;

// DUCKDB_C_API duckdb_logical_type duckdb_map_type_value_type(duckdb_logical_type type);
export function map_type_value_type(logical_type: LogicalType): LogicalType;

// DUCKDB_C_API idx_t duckdb_struct_type_child_count(duckdb_logical_type type);
export function struct_type_child_count(logical_type: LogicalType): number;

// DUCKDB_C_API char *duckdb_struct_type_child_name(duckdb_logical_type type, idx_t index);
export function struct_type_child_name(logical_type: LogicalType, index: number): string;

// DUCKDB_C_API duckdb_logical_type duckdb_struct_type_child_type(duckdb_logical_type type, idx_t index);
export function struct_type_child_type(logical_type: LogicalType, index: number): LogicalType;

// DUCKDB_C_API idx_t duckdb_union_type_member_count(duckdb_logical_type type);
export function union_type_member_count(logical_type: LogicalType): number;

// DUCKDB_C_API char *duckdb_union_type_member_name(duckdb_logical_type type, idx_t index);
export function union_type_member_name(logical_type: LogicalType, index: number): string;

// DUCKDB_C_API duckdb_logical_type duckdb_union_type_member_type(duckdb_logical_type type, idx_t index);
export function union_type_member_type(logical_type: LogicalType, index: number): LogicalType;

// DUCKDB_C_API void duckdb_destroy_logical_type(duckdb_logical_type *type);
// not exposed: destroyed in finalizer

// DUCKDB_C_API duckdb_state duckdb_register_logical_type(duckdb_connection con, duckdb_logical_type type, duckdb_create_type_info info);
// export function register_logical_type(connection: Connection, logical_type: LogicalType, info: CreateTypeInfo): void;

// DUCKDB_C_API duckdb_data_chunk duckdb_create_data_chunk(duckdb_logical_type *types, idx_t column_count);
export function create_data_chunk(logical_types: readonly LogicalType[]): DataChunk;

// DUCKDB_C_API void duckdb_destroy_data_chunk(duckdb_data_chunk *chunk);
// not exposed: destroyed in finalizer

// DUCKDB_C_API void duckdb_data_chunk_reset(duckdb_data_chunk chunk);
export function data_chunk_reset(chunk: DataChunk): void;

// DUCKDB_C_API idx_t duckdb_data_chunk_get_column_count(duckdb_data_chunk chunk);
export function data_chunk_get_column_count(chunk: DataChunk): number;

// DUCKDB_C_API duckdb_vector duckdb_data_chunk_get_vector(duckdb_data_chunk chunk, idx_t col_idx);
export function data_chunk_get_vector(chunk: DataChunk, column_index: number): Vector;

// DUCKDB_C_API idx_t duckdb_data_chunk_get_size(duckdb_data_chunk chunk);
export function data_chunk_get_size(chunk: DataChunk): number;

// DUCKDB_C_API void duckdb_data_chunk_set_size(duckdb_data_chunk chunk, idx_t size);
export function data_chunk_set_size(chunk: DataChunk, size: number): void;

// DUCKDB_C_API duckdb_vector duckdb_create_vector(duckdb_logical_type type, idx_t capacity);

// DUCKDB_C_API void duckdb_destroy_vector(duckdb_vector *vector);

// DUCKDB_C_API duckdb_logical_type duckdb_vector_get_column_type(duckdb_vector vector);
export function vector_get_column_type(vector: Vector): LogicalType;

// DUCKDB_C_API void *duckdb_vector_get_data(duckdb_vector vector);
export function vector_get_data(vector: Vector, byte_count: number): Uint8Array;

// DUCKDB_C_API uint64_t *duckdb_vector_get_validity(duckdb_vector vector);
export function vector_get_validity(vector: Vector, byte_count: number): Uint8Array;

// DUCKDB_C_API void duckdb_vector_ensure_validity_writable(duckdb_vector vector);
export function vector_ensure_validity_writable(vector: Vector): void;

// DUCKDB_C_API void duckdb_vector_assign_string_element(duckdb_vector vector, idx_t index, const char *str);
export function vector_assign_string_element(vector: Vector, index: number, str: string): void;

// DUCKDB_C_API void duckdb_vector_assign_string_element_len(duckdb_vector vector, idx_t index, const char *str, idx_t str_len);
export function vector_assign_string_element_len(vector: Vector, index: number, data: Uint8Array): void;

// DUCKDB_C_API duckdb_vector duckdb_list_vector_get_child(duckdb_vector vector);
export function list_vector_get_child(vector: Vector): Vector;

// DUCKDB_C_API idx_t duckdb_list_vector_get_size(duckdb_vector vector);
export function list_vector_get_size(vector: Vector): number;

// DUCKDB_C_API duckdb_state duckdb_list_vector_set_size(duckdb_vector vector, idx_t size);
export function list_vector_set_size(vector: Vector, size: number): void;

// DUCKDB_C_API duckdb_state duckdb_list_vector_reserve(duckdb_vector vector, idx_t required_capacity);
export function list_vector_reserve(vector: Vector, required_capacity: number): void;

// DUCKDB_C_API duckdb_vector duckdb_struct_vector_get_child(duckdb_vector vector, idx_t index);
export function struct_vector_get_child(vector: Vector, index: number): Vector;

// DUCKDB_C_API duckdb_vector duckdb_array_vector_get_child(duckdb_vector vector);
export function array_vector_get_child(vector: Vector): Vector;

// DUCKDB_C_API void duckdb_slice_vector(duckdb_vector vector, duckdb_selection_vector selection, idx_t len);

// DUCKDB_C_API void duckdb_vector_reference_value(duckdb_vector vector, duckdb_value value);

// DUCKDB_C_API void duckdb_vector_reference_vector(duckdb_vector to_vector, duckdb_vector from_vector);

// DUCKDB_C_API bool duckdb_validity_row_is_valid(uint64_t *validity, idx_t row);
export function validity_row_is_valid(validity: Uint8Array | null, row_index: number): boolean;

// DUCKDB_C_API void duckdb_validity_set_row_validity(uint64_t *validity, idx_t row, bool valid);
export function validity_set_row_validity(validity: Uint8Array, row_index: number, valid: boolean): void;

// DUCKDB_C_API void duckdb_validity_set_row_invalid(uint64_t *validity, idx_t row);
export function validity_set_row_invalid(validity: Uint8Array, row_index: number): void;

// DUCKDB_C_API void duckdb_validity_set_row_valid(uint64_t *validity, idx_t row);
export function validity_set_row_valid(validity: Uint8Array, row_index: number): void;

// DUCKDB_C_API duckdb_scalar_function duckdb_create_scalar_function();
export function create_scalar_function(): ScalarFunction;

// DUCKDB_C_API void duckdb_destroy_scalar_function(duckdb_scalar_function *scalar_function);
export function destroy_scalar_function_sync(scalar_function: ScalarFunction): void;

// DUCKDB_C_API void duckdb_scalar_function_set_name(duckdb_scalar_function scalar_function, const char *name);
export function scalar_function_set_name(scalar_function: ScalarFunction, name: string): void;

// DUCKDB_C_API void duckdb_scalar_function_set_varargs(duckdb_scalar_function scalar_function, duckdb_logical_type type);
export function scalar_function_set_varargs(scalar_function: ScalarFunction, logical_type: LogicalType): void;

// DUCKDB_C_API void duckdb_scalar_function_set_special_handling(duckdb_scalar_function scalar_function);
export function scalar_function_set_special_handling(scalar_function: ScalarFunction): void;

// DUCKDB_C_API void duckdb_scalar_function_set_volatile(duckdb_scalar_function scalar_function);
export function scalar_function_set_volatile(scalar_function: ScalarFunction): void;

// DUCKDB_C_API void duckdb_scalar_function_add_parameter(duckdb_scalar_function scalar_function, duckdb_logical_type type);
export function scalar_function_add_parameter(scalar_function: ScalarFunction, logical_type: LogicalType): void;

// DUCKDB_C_API void duckdb_scalar_function_set_return_type(duckdb_scalar_function scalar_function, duckdb_logical_type type);
export function scalar_function_set_return_type(scalar_function: ScalarFunction, logical_type: LogicalType): void;

// DUCKDB_C_API void duckdb_scalar_function_set_extra_info(duckdb_scalar_function scalar_function, void *extra_info, duckdb_delete_callback_t destroy);
export function scalar_function_set_extra_info(scalar_function: ScalarFunction, extra_info: object): void;

// DUCKDB_C_API void duckdb_scalar_function_set_bind(duckdb_scalar_function scalar_function, duckdb_scalar_function_bind_t bind);
// DUCKDB_C_API void duckdb_scalar_function_set_bind_data(duckdb_bind_info info, void *bind_data, duckdb_delete_callback_t destroy);
// DUCKDB_C_API void duckdb_scalar_function_bind_set_error(duckdb_bind_info info, const char *error);

// DUCKDB_C_API void duckdb_scalar_function_set_function(duckdb_scalar_function scalar_function, duckdb_scalar_function_t function);
export function scalar_function_set_function(scalar_function: ScalarFunction, func: ScalarFunctionMainFunction): void;

// DUCKDB_C_API duckdb_state duckdb_register_scalar_function(duckdb_connection con, duckdb_scalar_function scalar_function);
export function register_scalar_function(connection: Connection, scalar_function: ScalarFunction): void;

// DUCKDB_C_API void *duckdb_scalar_function_get_extra_info(duckdb_function_info info);
export function scalar_function_get_extra_info(function_info: FunctionInfo): object | undefined;

// DUCKDB_C_API void *duckdb_scalar_function_get_bind_data(duckdb_function_info info);
// DUCKDB_C_API void duckdb_scalar_function_get_client_context(duckdb_bind_info info, duckdb_client_context *out_context);

// DUCKDB_C_API void duckdb_scalar_function_set_error(duckdb_function_info info, const char *error);
export function scalar_function_set_error(function_info: FunctionInfo, error: string): void;

// DUCKDB_C_API duckdb_scalar_function_set duckdb_create_scalar_function_set(const char *name);
// DUCKDB_C_API void duckdb_destroy_scalar_function_set(duckdb_scalar_function_set *scalar_function_set);
// DUCKDB_C_API duckdb_state duckdb_add_scalar_function_to_set(duckdb_scalar_function_set set, duckdb_scalar_function function);
// DUCKDB_C_API duckdb_state duckdb_register_scalar_function_set(duckdb_connection con, duckdb_scalar_function_set set);

// DUCKDB_C_API duckdb_selection_vector duckdb_create_selection_vector(idx_t size);
// DUCKDB_C_API void duckdb_destroy_selection_vector(duckdb_selection_vector vector);
// DUCKDB_C_API sel_t *duckdb_selection_vector_get_data_ptr(duckdb_selection_vector vector);

// DUCKDB_C_API duckdb_aggregate_function duckdb_create_aggregate_function();
// DUCKDB_C_API void duckdb_destroy_aggregate_function(duckdb_aggregate_function *aggregate_function);
// DUCKDB_C_API void duckdb_aggregate_function_set_name(duckdb_aggregate_function aggregate_function, const char *name);
// DUCKDB_C_API void duckdb_aggregate_function_add_parameter(duckdb_aggregate_function aggregate_function, duckdb_logical_type type);
// DUCKDB_C_API void duckdb_aggregate_function_set_return_type(duckdb_aggregate_function aggregate_function, duckdb_logical_type type);
// DUCKDB_C_API void duckdb_aggregate_function_set_functions(duckdb_aggregate_function aggregate_function, duckdb_aggregate_state_size state_size, duckdb_aggregate_init_t state_init, duckdb_aggregate_update_t update, duckdb_aggregate_combine_t combine, duckdb_aggregate_finalize_t finalize);
// DUCKDB_C_API void duckdb_aggregate_function_set_destructor(duckdb_aggregate_function aggregate_function, duckdb_aggregate_destroy_t destroy);
// DUCKDB_C_API duckdb_state duckdb_register_aggregate_function(duckdb_connection con, duckdb_aggregate_function aggregate_function);
// DUCKDB_C_API void duckdb_aggregate_function_set_special_handling(duckdb_aggregate_function aggregate_function);
// DUCKDB_C_API void duckdb_aggregate_function_set_extra_info(duckdb_aggregate_function aggregate_function, void *extra_info, duckdb_delete_callback_t destroy);
// DUCKDB_C_API void *duckdb_aggregate_function_get_extra_info(duckdb_function_info info);
// DUCKDB_C_API void duckdb_aggregate_function_set_error(duckdb_function_info info, const char *error);

// DUCKDB_C_API duckdb_aggregate_function_set duckdb_create_aggregate_function_set(const char *name);
// DUCKDB_C_API void duckdb_destroy_aggregate_function_set(duckdb_aggregate_function_set *aggregate_function_set);
// DUCKDB_C_API duckdb_state duckdb_add_aggregate_function_to_set(duckdb_aggregate_function_set set, duckdb_aggregate_function function);
// DUCKDB_C_API duckdb_state duckdb_register_aggregate_function_set(duckdb_connection con, duckdb_aggregate_function_set set);

// DUCKDB_C_API duckdb_table_function duckdb_create_table_function();
// DUCKDB_C_API void duckdb_destroy_table_function(duckdb_table_function *table_function);
// DUCKDB_C_API void duckdb_table_function_set_name(duckdb_table_function table_function, const char *name);
// DUCKDB_C_API void duckdb_table_function_add_parameter(duckdb_table_function table_function, duckdb_logical_type type);
// DUCKDB_C_API void duckdb_table_function_add_named_parameter(duckdb_table_function table_function, const char *name, duckdb_logical_type type);
// DUCKDB_C_API void duckdb_table_function_set_extra_info(duckdb_table_function table_function, void *extra_info, duckdb_delete_callback_t destroy);
// DUCKDB_C_API void duckdb_table_function_set_bind(duckdb_table_function table_function, duckdb_table_function_bind_t bind);
// DUCKDB_C_API void duckdb_table_function_set_init(duckdb_table_function table_function, duckdb_table_function_init_t init);
// DUCKDB_C_API void duckdb_table_function_set_local_init(duckdb_table_function table_function, duckdb_table_function_init_t init);
// DUCKDB_C_API void duckdb_table_function_set_function(duckdb_table_function table_function, duckdb_table_function_t function);
// DUCKDB_C_API void duckdb_table_function_supports_projection_pushdown(duckdb_table_function table_function, bool pushdown);
// DUCKDB_C_API duckdb_state duckdb_register_table_function(duckdb_connection con, duckdb_table_function function);

// DUCKDB_C_API void *duckdb_bind_get_extra_info(duckdb_bind_info info);
// DUCKDB_C_API void duckdb_bind_add_result_column(duckdb_bind_info info, const char *name, duckdb_logical_type type);
// DUCKDB_C_API idx_t duckdb_bind_get_parameter_count(duckdb_bind_info info);
// DUCKDB_C_API duckdb_value duckdb_bind_get_parameter(duckdb_bind_info info, idx_t index);
// DUCKDB_C_API duckdb_value duckdb_bind_get_named_parameter(duckdb_bind_info info, const char *name);
// DUCKDB_C_API void duckdb_bind_set_bind_data(duckdb_bind_info info, void *bind_data, duckdb_delete_callback_t destroy);
// DUCKDB_C_API void duckdb_bind_set_cardinality(duckdb_bind_info info, idx_t cardinality, bool is_exact);
// DUCKDB_C_API void duckdb_bind_set_error(duckdb_bind_info info, const char *error);

// DUCKDB_C_API void *duckdb_init_get_extra_info(duckdb_init_info info);
// DUCKDB_C_API void *duckdb_init_get_bind_data(duckdb_init_info info);
// DUCKDB_C_API void duckdb_init_set_init_data(duckdb_init_info info, void *init_data, duckdb_delete_callback_t destroy);
// DUCKDB_C_API idx_t duckdb_init_get_column_count(duckdb_init_info info);
// DUCKDB_C_API idx_t duckdb_init_get_column_index(duckdb_init_info info, idx_t column_index);
// DUCKDB_C_API void duckdb_init_set_max_threads(duckdb_init_info info, idx_t max_threads);
// DUCKDB_C_API void duckdb_init_set_error(duckdb_init_info info, const char *error);

// DUCKDB_C_API void *duckdb_function_get_extra_info(duckdb_function_info info);
// DUCKDB_C_API void *duckdb_function_get_bind_data(duckdb_function_info info);
// DUCKDB_C_API void *duckdb_function_get_init_data(duckdb_function_info info);
// DUCKDB_C_API void *duckdb_function_get_local_init_data(duckdb_function_info info);
// DUCKDB_C_API void duckdb_function_set_error(duckdb_function_info info, const char *error);

// DUCKDB_C_API void duckdb_add_replacement_scan(duckdb_database db, duckdb_replacement_callback_t replacement, void *extra_data, duckdb_delete_callback_t delete_callback);
// DUCKDB_C_API void duckdb_replacement_scan_set_function_name(duckdb_replacement_scan_info info, const char *function_name);
// DUCKDB_C_API void duckdb_replacement_scan_add_parameter(duckdb_replacement_scan_info info, duckdb_value parameter);
// DUCKDB_C_API void duckdb_replacement_scan_set_error(duckdb_replacement_scan_info info, const char *error);

// DUCKDB_C_API duckdb_profiling_info duckdb_get_profiling_info(duckdb_connection connection);
// DUCKDB_C_API duckdb_value duckdb_profiling_info_get_value(duckdb_profiling_info info, const char *key);
// DUCKDB_C_API duckdb_value duckdb_profiling_info_get_metrics(duckdb_profiling_info info);
// DUCKDB_C_API idx_t duckdb_profiling_info_get_child_count(duckdb_profiling_info info);
// DUCKDB_C_API duckdb_profiling_info duckdb_profiling_info_get_child(duckdb_profiling_info info, idx_t index);

// DUCKDB_C_API duckdb_state duckdb_appender_create(duckdb_connection connection, const char *schema, const char *table, duckdb_appender *out_appender);
export function appender_create(connection: Connection, schema: string | null, table: string): Appender;

// DUCKDB_C_API duckdb_state duckdb_appender_create_ext(duckdb_connection connection, const char *catalog, const char *schema, const char *table, duckdb_appender *out_appender);
export function appender_create_ext(connection: Connection, catalog: string | null, schema: string | null, table: string): Appender;

// DUCKDB_C_API idx_t duckdb_appender_column_count(duckdb_appender appender);
export function appender_column_count(appender: Appender): number;

// DUCKDB_C_API duckdb_logical_type duckdb_appender_column_type(duckdb_appender appender, idx_t col_idx);
export function appender_column_type(appender: Appender, column_index: number): LogicalType;

// DUCKDB_C_API const char *duckdb_appender_error(duckdb_appender appender);
// not exposed: other appender functions throw

// DUCKDB_C_API duckdb_state duckdb_appender_flush(duckdb_appender appender);
export function appender_flush_sync(appender: Appender): void;

// DUCKDB_C_API duckdb_state duckdb_appender_close(duckdb_appender appender);
export function appender_close_sync(appender: Appender): void;

// DUCKDB_C_API duckdb_state duckdb_appender_destroy(duckdb_appender *appender);
// not exposed: destroyed in finalizer

// DUCKDB_C_API duckdb_state duckdb_appender_add_column(duckdb_appender appender, const char *name);
// DUCKDB_C_API duckdb_state duckdb_appender_clear_columns(duckdb_appender appender);

// DUCKDB_C_API duckdb_state duckdb_appender_begin_row(duckdb_appender appender);
// not exposed: no-op

// DUCKDB_C_API duckdb_state duckdb_appender_end_row(duckdb_appender appender);
export function appender_end_row(appender: Appender): void;

// DUCKDB_C_API duckdb_state duckdb_append_default(duckdb_appender appender);
export function append_default(appender: Appender): void;

// DUCKDB_C_API duckdb_state duckdb_append_default_to_chunk(duckdb_appender appender, duckdb_data_chunk chunk, idx_t col, idx_t row);

// DUCKDB_C_API duckdb_state duckdb_append_bool(duckdb_appender appender, bool value);
export function append_bool(appender: Appender, bool: boolean): void;

// DUCKDB_C_API duckdb_state duckdb_append_int8(duckdb_appender appender, int8_t value);
export function append_int8(appender: Appender, int8: number): void;

// DUCKDB_C_API duckdb_state duckdb_append_int16(duckdb_appender appender, int16_t value);
export function append_int16(appender: Appender, int16: number): void;

// DUCKDB_C_API duckdb_state duckdb_append_int32(duckdb_appender appender, int32_t value);
export function append_int32(appender: Appender, int32: number): void;

// DUCKDB_C_API duckdb_state duckdb_append_int64(duckdb_appender appender, int64_t value);
export function append_int64(appender: Appender, int64: bigint): void;

// DUCKDB_C_API duckdb_state duckdb_append_hugeint(duckdb_appender appender, duckdb_hugeint value);
export function append_hugeint(appender: Appender, hugeint: bigint): void;

// DUCKDB_C_API duckdb_state duckdb_append_uint8(duckdb_appender appender, uint8_t value);
export function append_uint8(appender: Appender, uint8: number): void;

// DUCKDB_C_API duckdb_state duckdb_append_uint16(duckdb_appender appender, uint16_t value);
export function append_uint16(appender: Appender, uint16: number): void;

// DUCKDB_C_API duckdb_state duckdb_append_uint32(duckdb_appender appender, uint32_t value);
export function append_uint32(appender: Appender, uint32: number): void;

// DUCKDB_C_API duckdb_state duckdb_append_uint64(duckdb_appender appender, uint64_t value);
export function append_uint64(appender: Appender, uint64: bigint): void;

// DUCKDB_C_API duckdb_state duckdb_append_uhugeint(duckdb_appender appender, duckdb_uhugeint value);
export function append_uhugeint(appender: Appender, uhugeint: bigint): void;

// DUCKDB_C_API duckdb_state duckdb_append_float(duckdb_appender appender, float value);
export function append_float(appender: Appender, float: number): void;

// DUCKDB_C_API duckdb_state duckdb_append_double(duckdb_appender appender, double value);
export function append_double(appender: Appender, double: number): void;

// DUCKDB_C_API duckdb_state duckdb_append_date(duckdb_appender appender, duckdb_date value);
export function append_date(appender: Appender, date: Date_): void;

// DUCKDB_C_API duckdb_state duckdb_append_time(duckdb_appender appender, duckdb_time value);
export function append_time(appender: Appender, time: Time): void;

// DUCKDB_C_API duckdb_state duckdb_append_timestamp(duckdb_appender appender, duckdb_timestamp value);
export function append_timestamp(appender: Appender, timestamp: Timestamp): void;

// DUCKDB_C_API duckdb_state duckdb_append_interval(duckdb_appender appender, duckdb_interval value);
export function append_interval(appender: Appender, interval: Interval): void;

// DUCKDB_C_API duckdb_state duckdb_append_varchar(duckdb_appender appender, const char *val);
export function append_varchar(appender: Appender, varchar: string): void;

// DUCKDB_C_API duckdb_state duckdb_append_varchar_length(duckdb_appender appender, const char *val, idx_t length);
// not exposed: JS string includes length

// DUCKDB_C_API duckdb_state duckdb_append_blob(duckdb_appender appender, const void *data, idx_t length);
export function append_blob(appender: Appender, data: Uint8Array): void;

// DUCKDB_C_API duckdb_state duckdb_append_null(duckdb_appender appender);
export function append_null(appender: Appender): void;

// DUCKDB_C_API duckdb_state duckdb_append_value(duckdb_appender appender, duckdb_value value);
export function append_value(appender: Appender, value: Value): void;

// DUCKDB_C_API duckdb_state duckdb_append_data_chunk(duckdb_appender appender, duckdb_data_chunk chunk);
export function append_data_chunk(appender: Appender, chunk: DataChunk): void;

// DUCKDB_C_API duckdb_state duckdb_table_description_create(duckdb_connection connection, const char *schema, const char *table, duckdb_table_description *out);
// DUCKDB_C_API duckdb_state duckdb_table_description_create_ext(duckdb_connection connection, const char *catalog, const char *schema, const char *table, duckdb_table_description *out);
// DUCKDB_C_API void duckdb_table_description_destroy(duckdb_table_description *table_description);
// DUCKDB_C_API const char *duckdb_table_description_error(duckdb_table_description table_description);
// DUCKDB_C_API duckdb_state duckdb_column_has_default(duckdb_table_description table_description, idx_t index, bool *out);
// DUCKDB_C_API char *duckdb_table_description_get_column_name(duckdb_table_description table_description, idx_t index);

// #ifndef DUCKDB_API_NO_DEPRECATED
// DUCKDB_C_API duckdb_state duckdb_query_arrow(duckdb_connection connection, const char *query, duckdb_arrow *out_result);
// DUCKDB_C_API duckdb_state duckdb_query_arrow_schema(duckdb_arrow result, duckdb_arrow_schema *out_schema);
// DUCKDB_C_API duckdb_state duckdb_prepared_arrow_schema(duckdb_prepared_statement prepared, duckdb_arrow_schema *out_schema);
// DUCKDB_C_API void duckdb_result_arrow_array(duckdb_result result, duckdb_data_chunk chunk, duckdb_arrow_array *out_array);
// DUCKDB_C_API duckdb_state duckdb_query_arrow_array(duckdb_arrow result, duckdb_arrow_array *out_array);
// DUCKDB_C_API idx_t duckdb_arrow_column_count(duckdb_arrow result);
// DUCKDB_C_API idx_t duckdb_arrow_row_count(duckdb_arrow result);
// DUCKDB_C_API idx_t duckdb_arrow_rows_changed(duckdb_arrow result);
// DUCKDB_C_API const char *duckdb_query_arrow_error(duckdb_arrow result);
// DUCKDB_C_API void duckdb_destroy_arrow(duckdb_arrow *result);
// DUCKDB_C_API void duckdb_destroy_arrow_stream(duckdb_arrow_stream *stream_p);
// DUCKDB_C_API duckdb_state duckdb_execute_prepared_arrow(duckdb_prepared_statement prepared_statement, duckdb_arrow *out_result);
// DUCKDB_C_API duckdb_state duckdb_arrow_scan(duckdb_connection connection, const char *table_name, duckdb_arrow_stream arrow);
// DUCKDB_C_API duckdb_state duckdb_arrow_array_scan(duckdb_connection connection, const char *table_name, duckdb_arrow_schema arrow_schema, duckdb_arrow_array arrow_array, duckdb_arrow_stream *out_stream);
// #endif

// DUCKDB_C_API void duckdb_execute_tasks(duckdb_database database, idx_t max_tasks);
// DUCKDB_C_API duckdb_task_state duckdb_create_task_state(duckdb_database database);
// DUCKDB_C_API void duckdb_execute_tasks_state(duckdb_task_state state);
// DUCKDB_C_API idx_t duckdb_execute_n_tasks_state(duckdb_task_state state, idx_t max_tasks);
// DUCKDB_C_API void duckdb_finish_execution(duckdb_task_state state);
// DUCKDB_C_API bool duckdb_task_state_is_finished(duckdb_task_state state);
// DUCKDB_C_API void duckdb_destroy_task_state(duckdb_task_state state);
// DUCKDB_C_API bool duckdb_execution_is_finished(duckdb_connection con);

// #ifndef DUCKDB_API_NO_DEPRECATED
// DUCKDB_C_API duckdb_data_chunk duckdb_stream_fetch_chunk(duckdb_result result);
// #endif

// DUCKDB_C_API duckdb_data_chunk duckdb_fetch_chunk(duckdb_result result);
export function fetch_chunk(result: Result): Promise<DataChunk | null>;

// DUCKDB_C_API duckdb_cast_function duckdb_create_cast_function();
// DUCKDB_C_API void duckdb_cast_function_set_source_type(duckdb_cast_function cast_function, duckdb_logical_type source_type);
// DUCKDB_C_API void duckdb_cast_function_set_target_type(duckdb_cast_function cast_function, duckdb_logical_type target_type);
// DUCKDB_C_API void duckdb_cast_function_set_implicit_cast_cost(duckdb_cast_function cast_function, int64_t cost);
// DUCKDB_C_API void duckdb_cast_function_set_function(duckdb_cast_function cast_function, duckdb_cast_function_t function);
// DUCKDB_C_API void duckdb_cast_function_set_extra_info(duckdb_cast_function cast_function, void *extra_info, duckdb_delete_callback_t destroy);
// DUCKDB_C_API void *duckdb_cast_function_get_extra_info(duckdb_function_info info);
// DUCKDB_C_API duckdb_cast_mode duckdb_cast_function_get_cast_mode(duckdb_function_info info);
// DUCKDB_C_API void duckdb_cast_function_set_error(duckdb_function_info info, const char *error);
// DUCKDB_C_API void duckdb_cast_function_set_row_error(duckdb_function_info info, const char *error, idx_t row, duckdb_vector output);
// DUCKDB_C_API duckdb_state duckdb_register_cast_function(duckdb_connection con, duckdb_cast_function cast_function);
// DUCKDB_C_API void duckdb_destroy_cast_function(duckdb_cast_function *cast_function);

// ADDED
/** 
 * Read a pointer from `array_buffer` at `pointer_offset`, then read and return `byte_count` bytes from that pointer.
 * 
 * Used to read from `duckdb_string_t`s with non-inlined data that are embedded in VARCHAR, BLOB, and BIT vectors.
 */
export function get_data_from_pointer(array_buffer: ArrayBuffer, pointer_offset: number, byte_count: number): Uint8Array;

// ADDED
/** 
 * Copy `source_byte_count` bytes from `source_buffer` at `source_byte_offset` into `target_vector` at `target_byte_offset`.
 * 
 * Used to write to data chunks.
 *
 * Performs an efficient-but-unsafe memory copy. Use with care.
 */
export function copy_data_to_vector(target_vector: Vector, target_byte_offset: number, source_buffer: ArrayBuffer, source_byte_offset: number, source_byte_count: number): void;

// ADDED
/** 
 * Copy `source_byte_count` bytes from `source_buffer` at `source_byte_offset` into the validity of `target_vector` at `target_byte_offset`.
 * 
 * Used to write to data chunks.
 *
 * Performs an efficient-but-unsafe memory copy. Use with care.
 */
export function copy_data_to_vector_validity(target_vector: Vector, target_byte_offset: number, source_buffer: ArrayBuffer, source_byte_offset: number, source_byte_count: number): void;
