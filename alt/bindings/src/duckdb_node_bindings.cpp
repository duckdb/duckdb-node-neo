#define NODE_ADDON_API_DISABLE_DEPRECATED
#include "napi.h"

#include "duckdb.h"

template<typename T>
Napi::External<T> CreateExternal(Napi::Env env, const napi_type_tag &type_tag, T *data) {
  auto external = Napi::External<T>::New(env, data);
  external.TypeTag(&type_tag);
  return external;
}

template<typename T>
T* GetDataFromExternal(Napi::Env env, const napi_type_tag &type_tag, Napi::Value value, const char *error_message) {
  auto external = value.As<Napi::External<T>>();
  if (!external.CheckTypeTag(&type_tag)) {
    throw Napi::Error::New(env, error_message);
  }
  return external.Data();
}

// The following type tags are generated using: uuidgen | sed -r -e 's/-//g' -e 's/(.{16})(.*)/0x\1, 0x\2/'

static const napi_type_tag ConfigTypeTag = {
  0x5963FBB9648B4D2A, 0xB41ADE86056218D1
};

Napi::External<_duckdb_config> CreateExternalForConfig(Napi::Env env, duckdb_config config) {
  return CreateExternal<_duckdb_config>(env, ConfigTypeTag, config);
}

duckdb_config GetConfigFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_config>(env, ConfigTypeTag, value, "Invalid config argument");
}

static const napi_type_tag ConnectionTypeTag = {
  0x922B9BF54AB04DFC, 0x8A258578D371DB71
};

Napi::External<_duckdb_connection> CreateExternalForConnection(Napi::Env env, duckdb_connection connection) {
  return CreateExternal<_duckdb_connection>(env, ConnectionTypeTag, connection);
}

duckdb_connection GetConnectionFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_connection>(env, ConnectionTypeTag, value, "Invalid connection argument");
}

static const napi_type_tag DatabaseTypeTag = {
  0x835A8533653C40D1, 0x83B3BE2B233BA8F3
};

Napi::External<_duckdb_database> CreateExternalForDatabase(Napi::Env env, duckdb_database database) {
  return CreateExternal<_duckdb_database>(env, DatabaseTypeTag, database);
}

duckdb_database GetDatabaseFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_database>(env, DatabaseTypeTag, value, "Invalid database argument");
}

static const napi_type_tag ResultTypeTag = {
  0x08F7FE3AE12345E5, 0x8733310DC29372D9
};

Napi::External<duckdb_result> CreateExternalForResult(Napi::Env env, duckdb_result *result_ptr) {
  return CreateExternal<duckdb_result>(env, ResultTypeTag, result_ptr);
}

duckdb_result *GetResultFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<duckdb_result>(env, ResultTypeTag, value, "Invalid result argument");
}

class PromiseWorker : public Napi::AsyncWorker {

public:

  PromiseWorker(Napi::Env env) : AsyncWorker(env), deferred_(Napi::Promise::Deferred::New(env)) {
  }

  Napi::Promise Promise() {
    return deferred_.Promise();
  }

protected:

  virtual Napi::Value Result() = 0;

  void OnOK() override {
		deferred_.Resolve(Result());
	}

	void OnError(const Napi::Error &e) override {
		deferred_.Reject(e.Value());
	}

private:

	Napi::Promise::Deferred deferred_;

};

class OpenWorker : public PromiseWorker {

public:

  OpenWorker(Napi::Env env, std::optional<std::string> path, duckdb_config config)
    : PromiseWorker(env), path_(path), config_(config) {
  }

protected:

  void Execute() override {
    const char *path = nullptr;
    if (path_) {
      path = path_->c_str();
    }
    if (config_ != nullptr) {
      char *error = nullptr;
      if (duckdb_open_ext(path, &database_, config_, &error)) {
        if (error != nullptr) {
          SetError(error);
          duckdb_free(error);
        } else {
          SetError("Failed to open");
        }
      }
    } else {
      if (duckdb_open(path, &database_)) {
        SetError("Failed to open");
      }
    }
  }

  Napi::Value Result() override {
    return CreateExternalForDatabase(Env(), database_);
  }

private:

  std::optional<std::string> path_;
  duckdb_config config_;
  duckdb_database database_;

};

class CloseWorker : public PromiseWorker {

public:

  CloseWorker(Napi::Env env, duckdb_database database)
    : PromiseWorker(env), database_(database) {
  }

protected:

  void Execute() override {
    duckdb_close(&database_);
  }

  Napi::Value Result() override {
    return Env().Undefined();
  }

private:

  duckdb_database database_;

};

class ConnectWorker : public PromiseWorker {

public:

  ConnectWorker(Napi::Env env, duckdb_database database)
    : PromiseWorker(env), database_(database) {
  }

protected:

  void Execute() override {
    if (duckdb_connect(database_, &connection_)) {
      SetError("Failed to connect");
    }
  }

  Napi::Value Result() override {
    return CreateExternalForConnection(Env(), connection_);
  }

private:

  duckdb_database database_;
  duckdb_connection connection_;

};

class DisconnectWorker : public PromiseWorker {

public:

  DisconnectWorker(Napi::Env env, duckdb_connection connection)
    : PromiseWorker(env), connection_(connection) {
  }

protected:

  void Execute() override {
    duckdb_disconnect(&connection_);
  }

  Napi::Value Result() override {
    return Env().Undefined();
  }

private:

  duckdb_connection connection_;

};

class QueryWorker : public PromiseWorker {

public:

  QueryWorker(Napi::Env env, duckdb_connection connection, std::string query)
    : PromiseWorker(env), connection_(connection), query_(query) {
  }

protected:

  void Execute() override {
    result_ptr_ = reinterpret_cast<duckdb_result*>(duckdb_malloc(sizeof(duckdb_result)));
    if (duckdb_query(connection_, query_.c_str(), result_ptr_)) {
      SetError(duckdb_result_error(result_ptr_));
      duckdb_free(result_ptr_);
      result_ptr_ = nullptr;
    }
  }

  Napi::Value Result() override {
    return CreateExternalForResult(Env(), result_ptr_);
  }

private:

  duckdb_connection connection_;
  std::string query_;
  duckdb_result *result_ptr_;

};

void DefineEnumMember(Napi::Object enumObj, const char *key, uint32_t value) {
  enumObj.Set(key, value);
  enumObj.Set(value, key);
}

Napi::Object CreateResultTypeEnum(Napi::Env env) {
  auto resultTypeEnum = Napi::Object::New(env);
  DefineEnumMember(resultTypeEnum, "INVALID", 0);
  DefineEnumMember(resultTypeEnum, "CHANGED_ROWS", 1);
  DefineEnumMember(resultTypeEnum, "NOTHING", 2);
  DefineEnumMember(resultTypeEnum, "QUERY_RESULT", 3);
  return resultTypeEnum;
}

Napi::Object CreateStatementTypeEnum(Napi::Env env) {
  auto statementTypeEnum = Napi::Object::New(env);
  DefineEnumMember(statementTypeEnum, "INVALID", 0);
	DefineEnumMember(statementTypeEnum, "SELECT", 1);
	DefineEnumMember(statementTypeEnum, "INSERT", 2);
	DefineEnumMember(statementTypeEnum, "UPDATE", 3);
	DefineEnumMember(statementTypeEnum, "EXPLAIN", 4);
	DefineEnumMember(statementTypeEnum, "DELETE", 5);
	DefineEnumMember(statementTypeEnum, "PREPARE", 6);
	DefineEnumMember(statementTypeEnum, "CREATE", 7);
	DefineEnumMember(statementTypeEnum, "EXECUTE", 8);
	DefineEnumMember(statementTypeEnum, "ALTER", 9);
	DefineEnumMember(statementTypeEnum, "TRANSACTION", 10);
	DefineEnumMember(statementTypeEnum, "COPY", 11);
	DefineEnumMember(statementTypeEnum, "ANALYZE", 12);
	DefineEnumMember(statementTypeEnum, "VARIABLE_SET", 13);
	DefineEnumMember(statementTypeEnum, "CREATE_FUNC", 14);
	DefineEnumMember(statementTypeEnum, "DROP", 15);
	DefineEnumMember(statementTypeEnum, "EXPORT", 16);
	DefineEnumMember(statementTypeEnum, "PRAGMA", 17);
	DefineEnumMember(statementTypeEnum, "VACUUM", 18);
	DefineEnumMember(statementTypeEnum, "CALL", 19);
	DefineEnumMember(statementTypeEnum, "SET", 20);
	DefineEnumMember(statementTypeEnum, "LOAD", 21);
	DefineEnumMember(statementTypeEnum, "RELATION", 22);
	DefineEnumMember(statementTypeEnum, "EXTENSION", 23);
	DefineEnumMember(statementTypeEnum, "LOGICAL_PLAN", 24);
	DefineEnumMember(statementTypeEnum, "ATTACH", 25);
	DefineEnumMember(statementTypeEnum, "DETACH", 26);
	DefineEnumMember(statementTypeEnum, "MULTI", 27);
  return statementTypeEnum;
}

Napi::Object CreateTypeEnum(Napi::Env env) {
  auto typeEnum = Napi::Object::New(env);
  DefineEnumMember(typeEnum, "INVALID", 0);
	DefineEnumMember(typeEnum, "BOOLEAN", 1);
	DefineEnumMember(typeEnum, "TINYINT", 2);
	DefineEnumMember(typeEnum, "SMALLINT", 3);
	DefineEnumMember(typeEnum, "INTEGER", 4);
	DefineEnumMember(typeEnum, "BIGINT", 5);
	DefineEnumMember(typeEnum, "UTINYINT", 6);
	DefineEnumMember(typeEnum, "USMALLINT", 7);
	DefineEnumMember(typeEnum, "UINTEGER", 8);
	DefineEnumMember(typeEnum, "UBIGINT", 9);
	DefineEnumMember(typeEnum, "FLOAT", 10);
	DefineEnumMember(typeEnum, "DOUBLE", 11);
	DefineEnumMember(typeEnum, "TIMESTAMP", 12);
	DefineEnumMember(typeEnum, "DATE", 13);
	DefineEnumMember(typeEnum, "TIME", 14);
	DefineEnumMember(typeEnum, "INTERVAL", 15);
	DefineEnumMember(typeEnum, "HUGEINT", 16);
	DefineEnumMember(typeEnum, "UHUGEINT", 32);
	DefineEnumMember(typeEnum, "VARCHAR", 17);
	DefineEnumMember(typeEnum, "BLOB", 18);
	DefineEnumMember(typeEnum, "DECIMAL", 19);
	DefineEnumMember(typeEnum, "TIMESTAMP_S", 20);
	DefineEnumMember(typeEnum, "TIMESTAMP_MS", 21);
	DefineEnumMember(typeEnum, "TIMESTAMP_NS", 22);
	DefineEnumMember(typeEnum, "ENUM", 23);
	DefineEnumMember(typeEnum, "LIST", 24);
	DefineEnumMember(typeEnum, "STRUCT", 25);
	DefineEnumMember(typeEnum, "MAP", 26);
	DefineEnumMember(typeEnum, "ARRAY", 33);
	DefineEnumMember(typeEnum, "UUID", 27);
	DefineEnumMember(typeEnum, "UNION", 28);
	DefineEnumMember(typeEnum, "BIT", 29);
	DefineEnumMember(typeEnum, "TIME_TZ", 30);
	DefineEnumMember(typeEnum, "TIMESTAMP_TZ", 31);
  return typeEnum;
}

class DuckDBNodeAddon : public Napi::Addon<DuckDBNodeAddon> {

public:

  DuckDBNodeAddon(Napi::Env env, Napi::Object exports) {
    DefineAddon(exports, {
      InstanceValue("ResultType", CreateResultTypeEnum(env)),
      InstanceValue("StatementType", CreateStatementTypeEnum(env)),
      InstanceValue("Type", CreateTypeEnum(env)),

      InstanceMethod("open", &DuckDBNodeAddon::open),
      InstanceMethod("close", &DuckDBNodeAddon::close),
      InstanceMethod("connect", &DuckDBNodeAddon::connect),

      InstanceMethod("disconnect", &DuckDBNodeAddon::disconnect),

      InstanceMethod("library_version", &DuckDBNodeAddon::library_version),
      InstanceMethod("create_config", &DuckDBNodeAddon::create_config),
      InstanceMethod("config_count", &DuckDBNodeAddon::config_count),
      InstanceMethod("get_config_flag", &DuckDBNodeAddon::get_config_flag),
      InstanceMethod("set_config", &DuckDBNodeAddon::set_config),
      InstanceMethod("destroy_config", &DuckDBNodeAddon::destroy_config),

      InstanceMethod("query", &DuckDBNodeAddon::query),
      InstanceMethod("destroy_result", &DuckDBNodeAddon::destroy_result),
      InstanceMethod("column_name", &DuckDBNodeAddon::column_name),
      InstanceMethod("column_type", &DuckDBNodeAddon::column_type),
      InstanceMethod("result_statement_type", &DuckDBNodeAddon::result_statement_type),

      InstanceMethod("column_count", &DuckDBNodeAddon::column_count),

      InstanceMethod("result_return_type", &DuckDBNodeAddon::result_return_type),
    });
  }

private:

  // duckdb_state duckdb_open(const char *path, duckdb_database *out_database)
  // function open(path: string): Promise<Database>
  Napi::Value open(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto pathValue = info[0];
    auto configValue = info[1];
    std::optional<std::string> path = std::nullopt;
    if (!pathValue.IsUndefined()) {
      path = pathValue.As<Napi::String>();
    }
    duckdb_config config = nullptr;
    if (!configValue.IsUndefined()) {
      config = GetConfigFromExternal(env, configValue);
    }
    auto worker = new OpenWorker(env, path, config);
    worker->Queue();
    return worker->Promise();
  }

  // duckdb_state duckdb_open_ext(const char *path, duckdb_database *out_database, duckdb_config config, char **out_error)
  // consolidated into open

  // void duckdb_close(duckdb_database *database)
  // function close(database: Database): Promise<void>
  Napi::Value close(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto database = GetDatabaseFromExternal(env, info[0]);
    auto worker = new CloseWorker(env, database);
    worker->Queue();
    return worker->Promise();
  }

  // duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection)
  // function connect(database: Database): Promise<Connection>
  Napi::Value connect(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto database = GetDatabaseFromExternal(env, info[0]);
    auto worker = new ConnectWorker(env, database);
    worker->Queue();
    return worker->Promise();
  }

  // void duckdb_interrupt(duckdb_connection connection)

  // duckdb_query_progress_type duckdb_query_progress(duckdb_connection connection)

  // void duckdb_disconnect(duckdb_connection *connection)
  // function disconnect(connection: Connection): Promise<void>
  Napi::Value disconnect(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto connection = GetConnectionFromExternal(env, info[0]);
    auto worker = new DisconnectWorker(env, connection);
    worker->Queue();
    return worker->Promise();
  }

  // const char *duckdb_library_version()
  // function library_version(): string
  Napi::Value library_version(const Napi::CallbackInfo& info) {
    return Napi::String::New(info.Env(), duckdb_library_version());
  }

  // duckdb_state duckdb_create_config(duckdb_config *out_config)
  // function create_config(): Config
  Napi::Value create_config(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    duckdb_config config;
    if (duckdb_create_config(&config)) {
      throw Napi::Error::New(env, "Failed to create config");
    }
    return CreateExternalForConfig(env, config);
  }

  // size_t duckdb_config_count()
  // function config_count(): number
  Napi::Value config_count(const Napi::CallbackInfo& info) {
    return Napi::Number::New(info.Env(), duckdb_config_count());
  }

  // duckdb_state duckdb_get_config_flag(size_t index, const char **out_name, const char **out_description)
  // function get_config_flag(index: number): ConfigFlag
  Napi::Value get_config_flag(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto index = info[0].As<Napi::Number>().Uint32Value();
    const char *name;
    const char *description;
    if (duckdb_get_config_flag(index, &name, &description)) {
      throw Napi::Error::New(env, "Config option not found");
    }
    auto config_flag = Napi::Object::New(env);
    config_flag.Set("name", name);
    config_flag.Set("description", description);
    return config_flag;
  }

  // duckdb_state duckdb_set_config(duckdb_config config, const char *name, const char *option)
  // function set_config(config: Config, name: string, option: string): void
  Napi::Value set_config(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto config = GetConfigFromExternal(env, info[0]);
    std::string name = info[1].As<Napi::String>();
    std::string option = info[2].As<Napi::String>();
    if (duckdb_set_config(config, name.c_str(), option.c_str())) {
      throw Napi::Error::New(env, "Failed to set config");
    }
    return env.Undefined();
  }

  // void duckdb_destroy_config(duckdb_config *config)
  // function destroy_config(config: Config): void
  Napi::Value destroy_config(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto config = GetConfigFromExternal(env, info[0]);
    duckdb_destroy_config(&config);
    return env.Undefined();
  }

  // duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out_result)
  // function query(connection: Connection, query: string): Promise<Result>
  Napi::Value query(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto connection = GetConnectionFromExternal(env, info[0]);
    std::string query = info[1].As<Napi::String>();
    auto worker = new QueryWorker(env, connection, query);
    worker->Queue();
    return worker->Promise();
  }


  // void duckdb_destroy_result(duckdb_result *result)
  // function destroy_result(result: Result): void
  Napi::Value destroy_result(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    duckdb_destroy_result(result_ptr);
    return env.Undefined();
  }

  // const char *duckdb_column_name(duckdb_result *result, idx_t col)
  // function column_name(result: Result, column_index: number): string
  Napi::Value column_name(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto column_index = info[1].As<Napi::Number>().Uint32Value();
    auto column_name = duckdb_column_name(result_ptr, column_index);
    return Napi::String::New(env, column_name);
  }

  // duckdb_type duckdb_column_type(duckdb_result *result, idx_t col)
  // function column_type(result: Result, column_index: number): Type
  Napi::Value column_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto column_index = info[1].As<Napi::Number>().Uint32Value();
    auto column_type = duckdb_column_type(result_ptr, column_index);
    return Napi::Number::New(env, column_type);
  }

  // duckdb_statement_type duckdb_result_statement_type(duckdb_result result)
  // function result_statement_type(result: Result): StatementType
  Napi::Value result_statement_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto statement_type = duckdb_result_statement_type(*result_ptr);
    return Napi::Number::New(env, statement_type);
  }

  // duckdb_logical_type duckdb_column_logical_type(duckdb_result *result, idx_t col)

  // idx_t duckdb_column_count(duckdb_result *result)
  // function column_count(result: Result): number
  Napi::Value column_count(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto column_count = duckdb_column_count(result_ptr);
    return Napi::Number::New(env, column_count);
  }

  // idx_t duckdb_rows_changed(duckdb_result *result)

  // const char *duckdb_result_error(duckdb_result *result)
  // query rejects promise with error

  // duckdb_result_type duckdb_result_return_type(duckdb_result result)
  // function result_return_type(result: Result): ResultType
  Napi::Value result_return_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto result_type = duckdb_result_return_type(*result_ptr);
    return Napi::Number::New(env, result_type);
  }

  // void *duckdb_malloc(size_t size)
  // not exposed; only used internally

  // void duckdb_free(void *ptr)
  // not exposed; only used internally

  // idx_t duckdb_vector_size()

  // bool duckdb_string_is_inlined(duckdb_string_t string)
  // not exposed

  // duckdb_date_struct duckdb_from_date(duckdb_date date)
  // duckdb_date duckdb_to_date(duckdb_date_struct date)
  // bool duckdb_is_finite_date(duckdb_date date)
  // duckdb_time_struct duckdb_from_time(duckdb_time time)
  // duckdb_time_tz duckdb_create_time_tz(int64_t micros, int32_t offset)
  // duckdb_time_tz_struct duckdb_from_time_tz(duckdb_time_tz micros)
  // duckdb_time duckdb_to_time(duckdb_time_struct time)
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

};

NODE_API_ADDON(DuckDBNodeAddon)
