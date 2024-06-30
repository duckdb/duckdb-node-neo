// Enums

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


// Types (no explicit destroy)

export interface Date_ {
  /**
   * Days are stored as days since 1970-01-01
   * 
   * Use the from_date/to_date functions to extract individual information
   */
  days: number;
}
export interface DateParts {
  year: number;
  month: number;
  day: number;
}

export interface Time {
  /**
   * Time is stored as microseconds since 00:00:00
   * 
   * Use the from_time/to_time function to extract individual information
   */
  micros: number;
}
export interface TimeParts {
  hour: number;
  min: number;
  sec: number;
  micros: number;
}

//! TIME_TZ is stored as 40 bits for int64_t micros, and 24 bits for int32_t offset
// typedef struct {
// 	uint64_t bits;
// } duckdb_time_tz;
// typedef struct {
// 	duckdb_time_struct time;
// 	int32_t offset;
// } duckdb_time_tz_struct;
export interface TimeTZ {
  /**
   * TIME_TZ is stored as 40 bits for int64_t micros, and 24 bits for int32_t offset
    */
  bits: number;
}
export interface TimeTZParts {
  time: TimeParts;
  offset: number;
}


// Types (explicit destroy)

export class Config {
}

export class Connection {
}

export class Database {
}

export class LogicalType {
}

export class Result {
}


// Types (TypeScript only)

export interface ConfigFlag {
  name: string;
  description: string;
}

export interface QueryProgress {
  percentage: number;
  rows_processed: number;
  total_rows_to_process: number;
}


// Functions

// duckdb_state duckdb_open(const char *path, duckdb_database *out_database)
export function open(path: string): Database;

// duckdb_state duckdb_open_ext(const char *path, duckdb_database *out_database, duckdb_config config, char **out_error)
export function open_ext(path: string, config: Config): Database;

// void duckdb_close(duckdb_database *database)
export function close(database: Database): void;

// duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection)
export function connect(database: Database): Connection;

// void duckdb_interrupt(duckdb_connection connection)
export function interrupt(connection: Connection): void;

// duckdb_query_progress_type duckdb_query_progress(duckdb_connection connection)
export function query_progress(connection: Connection): QueryProgress;

// void duckdb_disconnect(duckdb_connection *connection)
export function disconnect(connection: Connection): void;

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
export function query(connection: Connection, query: string): Result;

// void duckdb_destroy_result(duckdb_result *result)
export function destroy_result(result: Result): void;

// const char *duckdb_column_name(duckdb_result *result, idx_t col)
export function column_name(result: Result, col: number): string;

// duckdb_type duckdb_column_type(duckdb_result *result, idx_t col)
export function column_type(result: Result, col: number): Type;

// duckdb_statement_type duckdb_result_statement_type(duckdb_result result)
export function result_statement_type(result: Result): StatementType;

// duckdb_logical_type duckdb_column_logical_type(duckdb_result *result, idx_t col)
export function column_logical_type(result: Result, col: number): LogicalType;

// idx_t duckdb_column_count(duckdb_result *result)
export function column_count(result: Result): number;

// idx_t duckdb_rows_changed(duckdb_result *result)
export function rows_changed(result: Result): number;

// const char *duckdb_result_error(duckdb_result *result)
export function result_error(result: Result): string;

// duckdb_result_type duckdb_result_return_type(duckdb_result result)
export function result_return_type(result: Result): ResultType;

// void *duckdb_malloc(size_t size)

// void duckdb_free(void *ptr)

// idx_t duckdb_vector_size()
export function vector_size(): number;

// bool duckdb_string_is_inlined(duckdb_string_t string)

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
// duckdb_timestamp duckdb_to_timestamp(duckdb_timestamp_struct ts)
// bool duckdb_is_finite_timestamp(duckdb_timestamp ts)
// double duckdb_hugeint_to_double(duckdb_hugeint val)
// duckdb_hugeint duckdb_double_to_hugeint(double val)
// double duckdb_uhugeint_to_double(duckdb_uhugeint val)
// duckdb_uhugeint duckdb_double_to_uhugeint(double val)
// duckdb_decimal duckdb_double_to_decimal(double val, uint8_t width, uint8_t scale)
// double duckdb_decimal_to_double(duckdb_decimal val)
// duckdb_state duckdb_prepare(duckdb_connection connection, const char *query, duckdb_prepared_statement *out_prepared_statement)
// void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement)
// const char *duckdb_prepare_error(duckdb_prepared_statement prepared_statement)
// idx_t duckdb_nparams(duckdb_prepared_statement prepared_statement)
// const char *duckdb_parameter_name(duckdb_prepared_statement prepared_statement, idx_t index)
// duckdb_type duckdb_param_type(duckdb_prepared_statement prepared_statement, idx_t param_idx)
// duckdb_state duckdb_clear_bindings(duckdb_prepared_statement prepared_statement)
// duckdb_statement_type duckdb_prepared_statement_type(duckdb_prepared_statement statement)
// duckdb_state duckdb_bind_value(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_value val)
// duckdb_state duckdb_bind_parameter_index(duckdb_prepared_statement prepared_statement, idx_t *param_idx_out, const char *name)
// duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val)
// duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val)
// duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val)
// duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t val)
// duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t val)
// duckdb_state duckdb_bind_hugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_hugeint val)
// duckdb_state duckdb_bind_uhugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_uhugeint val)
// duckdb_state duckdb_bind_decimal(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_decimal val)
// duckdb_state duckdb_bind_uint8(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint8_t val)
// duckdb_state duckdb_bind_uint16(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint16_t val)
// duckdb_state duckdb_bind_uint32(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint32_t val)
// duckdb_state duckdb_bind_uint64(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint64_t val)
// duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val)
// duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val)
// duckdb_state duckdb_bind_date(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_date val)
// duckdb_state duckdb_bind_time(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_time val)
// duckdb_state duckdb_bind_timestamp(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_timestamp val)
// duckdb_state duckdb_bind_interval(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_interval val)
// duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val)
// duckdb_state duckdb_bind_varchar_length(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val, idx_t length)
// duckdb_state duckdb_bind_blob(duckdb_prepared_statement prepared_statement, idx_t param_idx, const void *data, idx_t length)
// duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx)
// duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement, duckdb_result *out_result)
// idx_t duckdb_extract_statements(duckdb_connection connection, const char *query, duckdb_extracted_statements *out_extracted_statements)
// duckdb_state duckdb_prepare_extracted_statement(duckdb_connection connection, duckdb_extracted_statements extracted_statements, idx_t index, duckdb_prepared_statement *out_prepared_statement)
// const char *duckdb_extract_statements_error(duckdb_extracted_statements extracted_statements)
// void duckdb_destroy_extracted(duckdb_extracted_statements *extracted_statements)
// duckdb_state duckdb_pending_prepared(duckdb_prepared_statement prepared_statement, duckdb_pending_result *out_result)
// void duckdb_destroy_pending(duckdb_pending_result *pending_result)
// const char *duckdb_pending_error(duckdb_pending_result pending_result)
// duckdb_pending_state duckdb_pending_execute_task(duckdb_pending_result pending_result)
// duckdb_pending_state duckdb_pending_execute_check_state(duckdb_pending_result pending_result)
// duckdb_state duckdb_execute_pending(duckdb_pending_result pending_result, duckdb_result *out_result)
// bool duckdb_pending_execution_is_finished(duckdb_pending_state pending_state)
// void duckdb_destroy_value(duckdb_value *value)
// duckdb_value duckdb_create_varchar(const char *text)
// duckdb_value duckdb_create_varchar_length(const char *text, idx_t length)
// duckdb_value duckdb_create_int64(int64_t val)
// duckdb_value duckdb_create_struct_value(duckdb_logical_type type, duckdb_value *values)
// duckdb_value duckdb_create_list_value(duckdb_logical_type type, duckdb_value *values, idx_t value_count)
// duckdb_value duckdb_create_array_value(duckdb_logical_type type, duckdb_value *values, idx_t value_count)
// char *duckdb_get_varchar(duckdb_value value)
// int64_t duckdb_get_int64(duckdb_value value)
// duckdb_logical_type duckdb_create_logical_type(duckdb_type type)
// char *duckdb_logical_type_get_alias(duckdb_logical_type type)
// duckdb_logical_type duckdb_create_list_type(duckdb_logical_type type)
// duckdb_logical_type duckdb_create_array_type(duckdb_logical_type type, idx_t array_size)
// duckdb_logical_type duckdb_create_map_type(duckdb_logical_type key_type, duckdb_logical_type value_type)
// duckdb_logical_type duckdb_create_union_type(duckdb_logical_type *member_types, const char **member_names, idx_t member_count)
// duckdb_logical_type duckdb_create_struct_type(duckdb_logical_type *member_types, const char **member_names, idx_t member_count)
// duckdb_logical_type duckdb_create_enum_type(const char **member_names, idx_t member_count)
// duckdb_logical_type duckdb_create_decimal_type(uint8_t width, uint8_t scale)
// duckdb_type duckdb_get_type_id(duckdb_logical_type type)
// uint8_t duckdb_decimal_width(duckdb_logical_type type)
// uint8_t duckdb_decimal_scale(duckdb_logical_type type)
// duckdb_type duckdb_decimal_internal_type(duckdb_logical_type type)
// duckdb_type duckdb_enum_internal_type(duckdb_logical_type type)
// uint32_t duckdb_enum_dictionary_size(duckdb_logical_type type)
// char *duckdb_enum_dictionary_value(duckdb_logical_type type, idx_t index)
// duckdb_logical_type duckdb_list_type_child_type(duckdb_logical_type type)
// duckdb_logical_type duckdb_array_type_child_type(duckdb_logical_type type)
// idx_t duckdb_array_type_array_size(duckdb_logical_type type)
// duckdb_logical_type duckdb_map_type_key_type(duckdb_logical_type type)
// duckdb_logical_type duckdb_map_type_value_type(duckdb_logical_type type)
// idx_t duckdb_struct_type_child_count(duckdb_logical_type type)
// char *duckdb_struct_type_child_name(duckdb_logical_type type, idx_t index)
// duckdb_logical_type duckdb_struct_type_child_type(duckdb_logical_type type, idx_t index)
// idx_t duckdb_union_type_member_count(duckdb_logical_type type)
// char *duckdb_union_type_member_name(duckdb_logical_type type, idx_t index)
// duckdb_logical_type duckdb_union_type_member_type(duckdb_logical_type type, idx_t index)
// void duckdb_destroy_logical_type(duckdb_logical_type *type)
// duckdb_data_chunk duckdb_create_data_chunk(duckdb_logical_type *types, idx_t column_count)
// void duckdb_destroy_data_chunk(duckdb_data_chunk *chunk)
// void duckdb_data_chunk_reset(duckdb_data_chunk chunk)
// idx_t duckdb_data_chunk_get_column_count(duckdb_data_chunk chunk)
// duckdb_vector duckdb_data_chunk_get_vector(duckdb_data_chunk chunk, idx_t col_idx)
// idx_t duckdb_data_chunk_get_size(duckdb_data_chunk chunk)
// void duckdb_data_chunk_set_size(duckdb_data_chunk chunk, idx_t size)
// duckdb_logical_type duckdb_vector_get_column_type(duckdb_vector vector)
// void *duckdb_vector_get_data(duckdb_vector vector)
// uint64_t *duckdb_vector_get_validity(duckdb_vector vector)
// void duckdb_vector_ensure_validity_writable(duckdb_vector vector)
// void duckdb_vector_assign_string_element(duckdb_vector vector, idx_t index, const char *str)
// void duckdb_vector_assign_string_element_len(duckdb_vector vector, idx_t index, const char *str, idx_t str_len)
// duckdb_vector duckdb_list_vector_get_child(duckdb_vector vector)
// idx_t duckdb_list_vector_get_size(duckdb_vector vector)
// duckdb_state duckdb_list_vector_set_size(duckdb_vector vector, idx_t size)
// duckdb_state duckdb_list_vector_reserve(duckdb_vector vector, idx_t required_capacity)
// duckdb_vector duckdb_struct_vector_get_child(duckdb_vector vector, idx_t index)
// duckdb_vector duckdb_array_vector_get_child(duckdb_vector vector)
// bool duckdb_validity_row_is_valid(uint64_t *validity, idx_t row)
// void duckdb_validity_set_row_validity(uint64_t *validity, idx_t row, bool valid)
// void duckdb_validity_set_row_invalid(uint64_t *validity, idx_t row)
// void duckdb_validity_set_row_valid(uint64_t *validity, idx_t row)
// duckdb_state duckdb_appender_create(duckdb_connection connection, const char *schema, const char *table, duckdb_appender *out_appender)
// idx_t duckdb_appender_column_count(duckdb_appender appender)
// duckdb_logical_type duckdb_appender_column_type(duckdb_appender appender, idx_t col_idx)
// const char *duckdb_appender_error(duckdb_appender appender)
// duckdb_state duckdb_appender_flush(duckdb_appender appender)
// duckdb_state duckdb_appender_close(duckdb_appender appender)
// duckdb_state duckdb_appender_destroy(duckdb_appender *appender)
// duckdb_state duckdb_appender_begin_row(duckdb_appender appender)
// duckdb_state duckdb_appender_end_row(duckdb_appender appender)
// duckdb_state duckdb_append_bool(duckdb_appender appender, bool value)
// duckdb_state duckdb_append_int8(duckdb_appender appender, int8_t value)
// duckdb_state duckdb_append_int16(duckdb_appender appender, int16_t value)
// duckdb_state duckdb_append_int32(duckdb_appender appender, int32_t value)
// duckdb_state duckdb_append_int64(duckdb_appender appender, int64_t value)
// duckdb_state duckdb_append_hugeint(duckdb_appender appender, duckdb_hugeint value)
// duckdb_state duckdb_append_uint8(duckdb_appender appender, uint8_t value)
// duckdb_state duckdb_append_uint16(duckdb_appender appender, uint16_t value)
// duckdb_state duckdb_append_uint32(duckdb_appender appender, uint32_t value)
// duckdb_state duckdb_append_uint64(duckdb_appender appender, uint64_t value)
// duckdb_state duckdb_append_uhugeint(duckdb_appender appender, duckdb_uhugeint value)
// duckdb_state duckdb_append_float(duckdb_appender appender, float value)
// duckdb_state duckdb_append_double(duckdb_appender appender, double value)
// duckdb_state duckdb_append_date(duckdb_appender appender, duckdb_date value)
// duckdb_state duckdb_append_time(duckdb_appender appender, duckdb_time value)
// duckdb_state duckdb_append_timestamp(duckdb_appender appender, duckdb_timestamp value)
// duckdb_state duckdb_append_interval(duckdb_appender appender, duckdb_interval value)
// duckdb_state duckdb_append_varchar(duckdb_appender appender, const char *val)
// duckdb_state duckdb_append_varchar_length(duckdb_appender appender, const char *val, idx_t length)
// duckdb_state duckdb_append_blob(duckdb_appender appender, const void *data, idx_t length)
// duckdb_state duckdb_append_null(duckdb_appender appender)
// duckdb_state duckdb_append_data_chunk(duckdb_appender appender, duckdb_data_chunk chunk)
// duckdb_data_chunk duckdb_fetch_chunk(duckdb_result result)
