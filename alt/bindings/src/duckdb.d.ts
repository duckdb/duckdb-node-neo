// Enums

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

export enum State {
	Success = 0,
	Error = 1,
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
  rows_processed: number;
  total_rows_to_process: number;
}

export interface Time {
  /** Microseconds since 00:00:00 */
  micros: number;
}
export interface TimeParts {
  hour: number;
  min: number;
  sec: number;
  micros: number;
}

export interface TimeTZ {
  /** 40 bits for micros, 24 bits for offset */
  bits: number; // or bigint, or buffer?
}
export interface TimeTZParts {
  time: TimeParts;
  offset: number;
}

export interface Timestamp {
	/** Microseconds since 1970-01-01 */
	micros: bigint;
}
export interface TimestampParts {
	date: DateParts;
	time: TimeParts;
}

export interface Vector {
	__duckdb_type: 'duckdb_vector';
}

// Types (explicit destroy)

export interface Appender {
	__duckdb_type: 'duckdb_appender';
}

export interface Config {
	__duckdb_type: 'duckdb_config';
}

export interface Connection {
	__duckdb_type: 'duckdb_connection';
}

export interface Database {
	__duckdb_type: 'duckdb_database';
}

export interface DataChunk {
	__duckdb_type: 'duckdb_data_chunk';
}

export interface ExtractedStatements {
	__duckdb_type: 'duckdb_extracted_statements';
}

export interface LogicalType {
	__duckdb_type: 'duckdb_logical_type';
}

export interface PendingResult {
	__duckdb_type: 'duckdb_pending_result';
}

export interface PreparedStatement {
	__duckdb_type: 'duckdb_config';
}

export interface Result {
	__duckdb_type: 'duckdb_result';
}

export interface Value {
	__duckdb_type: 'duckdb_value';
}

// Types (TypeScript only)

export interface ConfigFlag {
  name: string;
  description: string;
}


// Functions

// duckdb_state duckdb_open(const char *path, duckdb_database *out_database)
export function open(path?: string, config?: Config): Promise<Database>;

// duckdb_state duckdb_open_ext(const char *path, duckdb_database *out_database, duckdb_config config, char **out_error)
// not exposed: consolidated into open

// void duckdb_close(duckdb_database *database)
export function close(database: Database): Promise<void>;

// duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection)
export function connect(database: Database): Promise<Connection>;

// void duckdb_interrupt(duckdb_connection connection)
export function interrupt(connection: Connection): void;

// duckdb_query_progress_type duckdb_query_progress(duckdb_connection connection)
export function query_progress(connection: Connection): QueryProgress;

// void duckdb_disconnect(duckdb_connection *connection)
export function disconnect(connection: Connection): Promise<void>;

// const char *duckdb_library_version()
export function library_version(): string;

// duckdb_state duckdb_create_config(duckdb_config *out_config)
export function create_config(): Config;

// size_t duckdb_config_count()
export function config_count(): number;

// duckdb_state duckdb_get_config_flag(size_t index, const char **out_name, const char **out_description)
export function get_config_flag(index: number): ConfigFlag;

// duckdb_state duckdb_set_config(duckdb_config config, const char *name, const char *option)
export function set_config(config: Config, name: string, option: string): void;

// void duckdb_destroy_config(duckdb_config *config)
export function destroy_config(config: Config): void;

// duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out_result)
export function query(connection: Connection, query: string): Promise<Result>;

// void duckdb_destroy_result(duckdb_result *result)
export function destroy_result(result: Result): void;

// const char *duckdb_column_name(duckdb_result *result, idx_t col)
export function column_name(result: Result, column_index: number): string;

// duckdb_type duckdb_column_type(duckdb_result *result, idx_t col)
export function column_type(result: Result, column_index: number): Type;

// duckdb_statement_type duckdb_result_statement_type(duckdb_result result)
export function result_statement_type(result: Result): StatementType;

// duckdb_logical_type duckdb_column_logical_type(duckdb_result *result, idx_t col)
export function column_logical_type(result: Result, column_index: number): LogicalType;

// idx_t duckdb_column_count(duckdb_result *result)
export function column_count(result: Result): number;

// idx_t duckdb_rows_changed(duckdb_result *result)
export function rows_changed(result: Result): number;

// const char *duckdb_result_error(duckdb_result *result)
// not exposed: query rejects promise with error

// duckdb_result_type duckdb_result_return_type(duckdb_result result)
export function result_return_type(result: Result): ResultType;

// void *duckdb_malloc(size_t size)
// not exposed: only used internally

// void duckdb_free(void *ptr)
// not exposed: only user internally

// idx_t duckdb_vector_size()
export function vector_size(): number;

// bool duckdb_string_is_inlined(duckdb_string_t string)
// not exposed: handled internally

// duckdb_date_struct duckdb_from_date(duckdb_date date)
export function from_date(date: Date_): DateParts;

// duckdb_date duckdb_to_date(duckdb_date_struct date)
export function to_date(parts: DateParts): Date_;

// bool duckdb_is_finite_date(duckdb_date date)
export function is_finite_date(date: Date_): boolean;

// duckdb_time_struct duckdb_from_time(duckdb_time time)
export function from_time(time: Time): TimeParts;

// duckdb_time_tz duckdb_create_time_tz(int64_t micros, int32_t offset)
export function create_time_tz(micros: number, offset: number): TimeTZ;

// duckdb_time_tz_struct duckdb_from_time_tz(duckdb_time_tz micros)
export function from_time_tz(time_tz: TimeTZ): TimeTZParts;

// duckdb_time duckdb_to_time(duckdb_time_struct time)
export function to_time(parts: TimeParts): Time;

// duckdb_timestamp_struct duckdb_from_timestamp(duckdb_timestamp ts)
export function from_timestamp(timestamp: Timestamp): TimestampParts;

// duckdb_timestamp duckdb_to_timestamp(duckdb_timestamp_struct ts)
export function to_timestamp(parts: TimestampParts): Timestamp;

// bool duckdb_is_finite_timestamp(duckdb_timestamp ts)
export function is_finite_timestamp(timestamp: Timestamp): boolean;

// double duckdb_hugeint_to_double(duckdb_hugeint val)
export function hugeint_to_double(hugeint: bigint): number;

// duckdb_hugeint duckdb_double_to_hugeint(double val)
export function double_to_hugeint(double: number): bigint;

// double duckdb_uhugeint_to_double(duckdb_uhugeint val)
export function uhugeint_to_double(uhugeint: bigint): number;

// duckdb_uhugeint duckdb_double_to_uhugeint(double val)
export function double_to_uhugeint(double: number): bigint;

// duckdb_decimal duckdb_double_to_decimal(double val, uint8_t width, uint8_t scale)
export function double_to_decimal(double: number, width: number, scale: number): Decimal;

// double duckdb_decimal_to_double(duckdb_decimal val)
export function decimal_to_double(decimal: Decimal): number;

// duckdb_state duckdb_prepare(duckdb_connection connection, const char *query, duckdb_prepared_statement *out_prepared_statement)
export function prepare(connection: Connection, query: string): Promise<PreparedStatement>;

// void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement)
export function destroy_prepare(prepared_statement: PreparedStatement): void;

// const char *duckdb_prepare_error(duckdb_prepared_statement prepared_statement)
export function prepare_error(prepared_statement: PreparedStatement): string;

// idx_t duckdb_nparams(duckdb_prepared_statement prepared_statement)
export function nparams(prepared_statement: PreparedStatement): number;

// const char *duckdb_parameter_name(duckdb_prepared_statement prepared_statement, idx_t index)
export function parameter_name(prepared_statement: PreparedStatement, index: number): string;

// duckdb_type duckdb_param_type(duckdb_prepared_statement prepared_statement, idx_t param_idx)
export function param_type(prepared_statement: PreparedStatement, index: number): Type;

// duckdb_state duckdb_clear_bindings(duckdb_prepared_statement prepared_statement)
export function clear_bindings(prepared_statement: PreparedStatement): State;

// duckdb_statement_type duckdb_prepared_statement_type(duckdb_prepared_statement statement)
export function prepared_statement_type(prepared_statement: PreparedStatement): StatementType;

// duckdb_state duckdb_bind_value(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_value val)
export function bind_value(prepared_statement: PreparedStatement, index: number, value: Value): State;

// duckdb_state duckdb_bind_parameter_index(duckdb_prepared_statement prepared_statement, idx_t *param_idx_out, const char *name)
export function bind_parameter_index(prepared_statement: PreparedStatement, name: string): number;

// duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val)
export function bind_boolean(prepared_statement: PreparedStatement, index: number, bool: boolean): State;

// duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val)
export function bind_int8(prepared_statement: PreparedStatement, index: number, int8: number): State;

// duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val)
export function bind_int16(prepared_statement: PreparedStatement, index: number, int16: number): State;

// duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t val)
export function bind_int32(prepared_statement: PreparedStatement, index: number, int32: number): State;

// duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t val)
export function bind_int64(prepared_statement: PreparedStatement, index: number, int64: bigint): State;

// duckdb_state duckdb_bind_hugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_hugeint val)
export function bind_hugeint(prepared_statement: PreparedStatement, index: number, hugeint: bigint): State;

// duckdb_state duckdb_bind_uhugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_uhugeint val)
export function bind_uhugeint(prepared_statement: PreparedStatement, index: number, uhugeint: bigint): State;

// duckdb_state duckdb_bind_decimal(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_decimal val)
export function bind_decimal(prepared_statement: PreparedStatement, index: number, decimal: Decimal): State;

// duckdb_state duckdb_bind_uint8(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint8_t val)
export function bind_uint8(prepared_statement: PreparedStatement, index: number, uint8: number): State;

// duckdb_state duckdb_bind_uint16(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint16_t val)
export function bind_uint16(prepared_statement: PreparedStatement, index: number, uint16: number): State;

// duckdb_state duckdb_bind_uint32(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint32_t val)
export function bind_uint32(prepared_statement: PreparedStatement, index: number, uint32: number): State;

// duckdb_state duckdb_bind_uint64(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint64_t val)
export function bind_uint64(prepared_statement: PreparedStatement, index: number, uint64: bigint): State;

// duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val)
export function bind_float(prepared_statement: PreparedStatement, index: number, float: number): State;

// duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val)
export function bind_double(prepared_statement: PreparedStatement, index: number, double: number): State;

// duckdb_state duckdb_bind_date(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_date val)
export function bind_date(prepared_statement: PreparedStatement, index: number, date: Date_): State;

// duckdb_state duckdb_bind_time(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_time val)
export function bind_time(prepared_statement: PreparedStatement, index: number, time: Time): State;

// duckdb_state duckdb_bind_timestamp(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_timestamp val)
export function bind_timestamp(prepared_statement: PreparedStatement, index: number, timestamp: Timestamp): State;

// duckdb_state duckdb_bind_interval(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_interval val)
export function bind_interval(prepared_statement: PreparedStatement, index: number, interval: Interval): State;

// duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val)
export function bind_varchar(prepared_statement: PreparedStatement, index: number, varchar: string): State;

// duckdb_state duckdb_bind_varchar_length(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val, idx_t length)

// duckdb_state duckdb_bind_blob(duckdb_prepared_statement prepared_statement, idx_t param_idx, const void *data, idx_t length)
export function bind_blob(prepared_statement: PreparedStatement, index: number, data: Uint8Array): State;

// duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx)
export function bind_null(prepared_statement: PreparedStatement, index: number): State;

// duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement, duckdb_result *out_result)
export function execute_prepared(prepared_statement: PreparedStatement): Promise<Result>;

// idx_t duckdb_extract_statements(duckdb_connection connection, const char *query, duckdb_extracted_statements *out_extracted_statements)
export function extract_statements(connection: Connection, query: string): Promise<ExtractedStatements>;

// duckdb_state duckdb_prepare_extracted_statement(duckdb_connection connection, duckdb_extracted_statements extracted_statements, idx_t index, duckdb_prepared_statement *out_prepared_statement)
export function prepare_extracted_statement(connection: Connection, extracted_statements: ExtractedStatements, index: number): Promise<PreparedStatement>;

// const char *duckdb_extract_statements_error(duckdb_extracted_statements extracted_statements)
export function extract_statements_error(extracted_statements: ExtractedStatements): string;

// void duckdb_destroy_extracted(duckdb_extracted_statements *extracted_statements)
export function destroy_extracted(extracted_statements: ExtractedStatements): void;

// duckdb_state duckdb_pending_prepared(duckdb_prepared_statement prepared_statement, duckdb_pending_result *out_result)
export function pending_prepared(prepared_statement: PreparedStatement): PendingResult;

// void duckdb_destroy_pending(duckdb_pending_result *pending_result)
export function destroy_pending(pending_result: PendingResult): void;

// const char *duckdb_pending_error(duckdb_pending_result pending_result)
export function pending_error(pending_result: PendingResult): string;

// duckdb_pending_state duckdb_pending_execute_task(duckdb_pending_result pending_result)
export function pending_execute_task(pending_result: PendingResult): PendingState;

// duckdb_pending_state duckdb_pending_execute_check_state(duckdb_pending_result pending_result)
export function pending_execute_check_state(pending_resulit: PendingResult): PendingState;

// duckdb_state duckdb_execute_pending(duckdb_pending_result pending_result, duckdb_result *out_result)
export function execute_pending(pending_result: PendingResult): Promise<Result>;

// bool duckdb_pending_execution_is_finished(duckdb_pending_state pending_state)
export function pending_execution_is_finished(pending_state: PendingState): boolean;

// void duckdb_destroy_value(duckdb_value *value)
export function destroy_value(value: Value): void;

// duckdb_value duckdb_create_varchar(const char *text)
export function create_varchar(text: string): Value;

// duckdb_value duckdb_create_varchar_length(const char *text, idx_t length)

// duckdb_value duckdb_create_int64(int64_t val)
export function create_int64(int64: number): Value;

// duckdb_value duckdb_create_struct_value(duckdb_logical_type type, duckdb_value *values)
export function create_struct_value(logical_type: LogicalType, values: Value[]): Value;

// duckdb_value duckdb_create_list_value(duckdb_logical_type type, duckdb_value *values, idx_t value_count)
export function create_list_value(logical_type: LogicalType, values: Value[]): Value;

// duckdb_value duckdb_create_array_value(duckdb_logical_type type, duckdb_value *values, idx_t value_count)
export function create_array_value(logical_type: LogicalType, values: Value[]): Value;

// char *duckdb_get_varchar(duckdb_value value)
export function get_varchar(value: Value): string;

// int64_t duckdb_get_int64(duckdb_value value)
export function get_int64(value: Value): bigint;

// duckdb_logical_type duckdb_create_logical_type(duckdb_type type)
export function create_logical_type(type: Type): LogicalType;

// char *duckdb_logical_type_get_alias(duckdb_logical_type type)
export function logical_type_get_alias(logical_type: LogicalType): string;

// duckdb_logical_type duckdb_create_list_type(duckdb_logical_type type)
export function create_list_type(logical_type: LogicalType): LogicalType;

// duckdb_logical_type duckdb_create_array_type(duckdb_logical_type type, idx_t array_size)
export function create_array_type(logical_type: LogicalType, array_size: number): LogicalType;

// duckdb_logical_type duckdb_create_map_type(duckdb_logical_type key_type, duckdb_logical_type value_type)
export function create_map_type(key_type: LogicalType, value_type: LogicalType): LogicalType;

// duckdb_logical_type duckdb_create_union_type(duckdb_logical_type *member_types, const char **member_names, idx_t member_count)
export function create_union_type(member_types: LogicalType[], member_names: string[]): LogicalType;

// duckdb_logical_type duckdb_create_struct_type(duckdb_logical_type *member_types, const char **member_names, idx_t member_count)
export function create_struct_type(member_types: LogicalType[], member_names: string[]): LogicalType;

// duckdb_logical_type duckdb_create_enum_type(const char **member_names, idx_t member_count)
export function create_enum_type(member_names: string[]): LogicalType;

// duckdb_logical_type duckdb_create_decimal_type(uint8_t width, uint8_t scale)
export function create_decimal_type(width: number, scale: number): LogicalType;

// duckdb_type duckdb_get_type_id(duckdb_logical_type type)
export function get_type_id(logical_type: LogicalType): Type;

// uint8_t duckdb_decimal_width(duckdb_logical_type type)
export function decimal_width(logical_type: LogicalType): number;

// uint8_t duckdb_decimal_scale(duckdb_logical_type type)
export function decimal_scale(logical_type: LogicalType): number;

// duckdb_type duckdb_decimal_internal_type(duckdb_logical_type type)
export function decimal_internal_type(logical_type: LogicalType): Type;

// duckdb_type duckdb_enum_internal_type(duckdb_logical_type type)
export function enum_internal_type(logical_type: LogicalType): Type;

// uint32_t duckdb_enum_dictionary_size(duckdb_logical_type type)
export function enum_dictionary_size(logical_type: LogicalType): number;

// char *duckdb_enum_dictionary_value(duckdb_logical_type type, idx_t index)
export function enum_dictionary_value(logical_type: LogicalType, index: number): string;

// duckdb_logical_type duckdb_list_type_child_type(duckdb_logical_type type)
export function list_type_child_type(logical_type: LogicalType): LogicalType;

// duckdb_logical_type duckdb_array_type_child_type(duckdb_logical_type type)
export function array_type_child_type(logical_type: LogicalType): LogicalType;

// idx_t duckdb_array_type_array_size(duckdb_logical_type type)
export function array_type_array_size(logical_type: LogicalType): number;

// duckdb_logical_type duckdb_map_type_key_type(duckdb_logical_type type)
export function map_type_key_type(logical_type: LogicalType): LogicalType;

// duckdb_logical_type duckdb_map_type_value_type(duckdb_logical_type type)
export function map_type_value_type(logical_type: LogicalType): LogicalType;

// idx_t duckdb_struct_type_child_count(duckdb_logical_type type)
export function struct_type_child_count(logical_type: LogicalType): number;

// char *duckdb_struct_type_child_name(duckdb_logical_type type, idx_t index)
export function struct_type_child_name(logical_type: LogicalType, index: number): string;

// duckdb_logical_type duckdb_struct_type_child_type(duckdb_logical_type type, idx_t index)
export function struct_type_child_type(logical_type: LogicalType, index: number): LogicalType;

// idx_t duckdb_union_type_member_count(duckdb_logical_type type)
export function union_type_member_count(logical_type: LogicalType): number;

// char *duckdb_union_type_member_name(duckdb_logical_type type, idx_t index)
export function union_type_member_name(logical_type: LogicalType, index: number): string;

// duckdb_logical_type duckdb_union_type_member_type(duckdb_logical_type type, idx_t index)
export function union_type_member_type(logical_type: LogicalType, index: number): LogicalType;

// void duckdb_destroy_logical_type(duckdb_logical_type *type)
export function destroy_logical_type(logical_type: LogicalType): void;

// duckdb_data_chunk duckdb_create_data_chunk(duckdb_logical_type *types, idx_t column_count)
export function create_data_chunk(logical_types: LogicalType[]): DataChunk;

// void duckdb_destroy_data_chunk(duckdb_data_chunk *chunk)
export function destroy_data_chunk(chunk: DataChunk): void;

// void duckdb_data_chunk_reset(duckdb_data_chunk chunk)
export function data_chunk_reset(chunk: DataChunk): void;

// idx_t duckdb_data_chunk_get_column_count(duckdb_data_chunk chunk)
export function data_chunk_get_column_count(chunk: DataChunk): number;

// duckdb_vector duckdb_data_chunk_get_vector(duckdb_data_chunk chunk, idx_t col_idx)
export function data_chunk_get_vector(chunk: DataChunk, column_index: number): Vector;

// idx_t duckdb_data_chunk_get_size(duckdb_data_chunk chunk)
export function data_chunk_get_size(chunk: DataChunk): number;

// void duckdb_data_chunk_set_size(duckdb_data_chunk chunk, idx_t size)
export function data_chunk_set_size(chunk: DataChunk, size: number): void;

// duckdb_logical_type duckdb_vector_get_column_type(duckdb_vector vector)
export function vector_get_column_type(vector: Vector): LogicalType;

// void *duckdb_vector_get_data(duckdb_vector vector)
export function vector_get_data(vector: Vector, byteCount: number): Buffer;

// uint64_t *duckdb_vector_get_validity(duckdb_vector vector)
export function vector_get_validity(vector: Vector, byteCount: number): Buffer;

// void duckdb_vector_ensure_validity_writable(duckdb_vector vector)
export function vector_ensure_validity_writable(vector: Vector): void;

// void duckdb_vector_assign_string_element(duckdb_vector vector, idx_t index, const char *str)
export function vector_assign_string_element(vector: Vector, index: number, str: string): void;

// void duckdb_vector_assign_string_element_len(duckdb_vector vector, idx_t index, const char *str, idx_t str_len)

// duckdb_vector duckdb_list_vector_get_child(duckdb_vector vector)
export function list_vector_get_child(vector: Vector): Vector;

// idx_t duckdb_list_vector_get_size(duckdb_vector vector)
export function list_vector_get_size(vector: Vector): number;

// duckdb_state duckdb_list_vector_set_size(duckdb_vector vector, idx_t size)
export function list_vector_set_size(vector: Vector, size: number): State;

// duckdb_state duckdb_list_vector_reserve(duckdb_vector vector, idx_t required_capacity)
export function list_vector_reserve(vector: Vector, required_capacity: number): State;

// duckdb_vector duckdb_struct_vector_get_child(duckdb_vector vector, idx_t index)
export function struct_vector_get_child(vector: Vector, index: number): Vector;

// duckdb_vector duckdb_array_vector_get_child(duckdb_vector vector)
export function array_vector_get_child(vector: Vector): Vector;

// bool duckdb_validity_row_is_valid(uint64_t *validity, idx_t row)
export function validity_row_is_valid(validity: Buffer, row_index: number): boolean;

// void duckdb_validity_set_row_validity(uint64_t *validity, idx_t row, bool valid)
export function validity_set_row_validity(validity: Buffer, row_index: number, valid: boolean): void;

// void duckdb_validity_set_row_invalid(uint64_t *validity, idx_t row)
export function validity_set_row_invalid(validity: Buffer, row_index: number): void;

// void duckdb_validity_set_row_valid(uint64_t *validity, idx_t row)
export function validity_set_row_valid(validity: Buffer, row_index: number): void;

// duckdb_state duckdb_appender_create(duckdb_connection connection, const char *schema, const char *table, duckdb_appender *out_appender)
export function appender_create(connection: Connection, schema: string, table: string): Appender;

// idx_t duckdb_appender_column_count(duckdb_appender appender)
export function appender_column_count(appender: Appender): number;

// duckdb_logical_type duckdb_appender_column_type(duckdb_appender appender, idx_t col_idx)
export function appender_column_type(appender: Appender, column_index: number): LogicalType;

// const char *duckdb_appender_error(duckdb_appender appender)
export function appender_error(appender: Appender): string;

// duckdb_state duckdb_appender_flush(duckdb_appender appender)
export function appender_flush(appender: Appender): State;

// duckdb_state duckdb_appender_close(duckdb_appender appender)
export function appender_close(appender: Appender): State;

// duckdb_state duckdb_appender_destroy(duckdb_appender *appender)
export function appender_destroy(appender: Appender): State;

// duckdb_state duckdb_appender_begin_row(duckdb_appender appender)
export function appender_begin_row(appender: Appender): State;

// duckdb_state duckdb_appender_end_row(duckdb_appender appender)
export function appender_end_row(appender: Appender): State;

// duckdb_state duckdb_append_bool(duckdb_appender appender, bool value)
export function append_bool(appender: Appender, bool: boolean): State;

// duckdb_state duckdb_append_int8(duckdb_appender appender, int8_t value)
export function append_int8(appender: Appender, int8: number): State;

// duckdb_state duckdb_append_int16(duckdb_appender appender, int16_t value)
export function append_int16(appender: Appender, int16: number): State;

// duckdb_state duckdb_append_int32(duckdb_appender appender, int32_t value)
export function append_int32(appender: Appender, int32: number): State;

// duckdb_state duckdb_append_int64(duckdb_appender appender, int64_t value)
export function append_int64(appender: Appender, int64: bigint): State;

// duckdb_state duckdb_append_hugeint(duckdb_appender appender, duckdb_hugeint value)
export function append_hugeint(appender: Appender, hugeint: bigint): State;

// duckdb_state duckdb_append_uint8(duckdb_appender appender, uint8_t value)
export function append_uint8(appender: Appender, uint8: number): State;

// duckdb_state duckdb_append_uint16(duckdb_appender appender, uint16_t value)
export function append_uint16(appender: Appender, uint16: number): State;

// duckdb_state duckdb_append_uint32(duckdb_appender appender, uint32_t value)
export function append_uint32(appender: Appender, uint32: number): State;

// duckdb_state duckdb_append_uint64(duckdb_appender appender, uint64_t value)
export function append_uint64(appender: Appender, uint64: bigint): State;

// duckdb_state duckdb_append_uhugeint(duckdb_appender appender, duckdb_uhugeint value)
export function append_uhugeint(appender: Appender, uhugeint: bigint): State;

// duckdb_state duckdb_append_float(duckdb_appender appender, float value)
export function append_float(appender: Appender, float: number): State;

// duckdb_state duckdb_append_double(duckdb_appender appender, double value)
export function append_double(appender: Appender, double: number): State;

// duckdb_state duckdb_append_date(duckdb_appender appender, duckdb_date value)
export function append_date(appender: Appender, date: Date_): State;

// duckdb_state duckdb_append_time(duckdb_appender appender, duckdb_time value)
export function append_time(appender: Appender, time: Time): State;

// duckdb_state duckdb_append_timestamp(duckdb_appender appender, duckdb_timestamp value)
export function append_timestamp(appender: Appender, timestamp: Timestamp): State;

// duckdb_state duckdb_append_interval(duckdb_appender appender, duckdb_interval value)
export function append_interval(appender: Appender, interval: Interval): State;

// duckdb_state duckdb_append_varchar(duckdb_appender appender, const char *val)
export function append_varchar(appender: Appender, varchar: string): State;

// duckdb_state duckdb_append_varchar_length(duckdb_appender appender, const char *val, idx_t length)

// duckdb_state duckdb_append_blob(duckdb_appender appender, const void *data, idx_t length)
export function append_blob(appender: Appender, data: Uint8Array): State;

// duckdb_state duckdb_append_null(duckdb_appender appender)
export function append_null(appender: Appender): State;

// duckdb_state duckdb_append_data_chunk(duckdb_appender appender, duckdb_data_chunk chunk)
export function append_data_chunk(appender: Appender, chunk: DataChunk): State;

// duckdb_data_chunk duckdb_fetch_chunk(duckdb_result result)
export function fetch_chunk(result: Result): Promise<DataChunk>;
