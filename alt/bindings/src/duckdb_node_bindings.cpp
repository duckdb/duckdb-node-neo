#define NODE_ADDON_API_DISABLE_DEPRECATED
#define NODE_API_NO_EXTERNAL_BUFFERS_ALLOWED
#include "napi.h"

#include <optional>
#include <string>
#include <vector>

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

static const napi_type_tag DataChunkTypeTag = {
  0x2C7537AB063A4296, 0xB1E70F08B0BBD1A3
};

Napi::External<_duckdb_data_chunk> CreateExternalForDataChunk(Napi::Env env, duckdb_data_chunk chunk) {
  return CreateExternal<_duckdb_data_chunk>(env, DataChunkTypeTag, chunk);
}

duckdb_data_chunk GetDataChunkFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_data_chunk>(env, DataChunkTypeTag, value, "Invalid data chunk argument");
}

static const napi_type_tag LogicalTypeTypeTag = {
  0x78AF202191ED4A23, 0x8093715369592A2B
};

Napi::External<_duckdb_logical_type> CreateExternalForLogicalType(Napi::Env env, duckdb_logical_type logical_type) {
  return CreateExternal<_duckdb_logical_type>(env, LogicalTypeTypeTag, logical_type);
}

duckdb_logical_type GetLogicalTypeFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_logical_type>(env, LogicalTypeTypeTag, value, "Invalid logical type argument");
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

static const napi_type_tag VectorTypeTag = {
  0x9FE56DE8E3124D07, 0x9ABF31145EDE1C9E
};

Napi::External<_duckdb_vector> CreateExternalForVector(Napi::Env env, duckdb_vector vector) {
  return CreateExternal<_duckdb_vector>(env, VectorTypeTag, vector);
}

duckdb_vector GetVectorFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_vector>(env, VectorTypeTag, value, "Invalid vector argument");
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

class FetchWorker : public PromiseWorker {

public:

  FetchWorker(Napi::Env env, duckdb_result *result_ptr)
    : PromiseWorker(env), result_ptr_(result_ptr) {
  }

protected:

  void Execute() override {
    data_chunk_ = duckdb_fetch_chunk(*result_ptr_);
  }

  Napi::Value Result() override {
    return CreateExternalForDataChunk(Env(), data_chunk_);
  }

private:

  duckdb_result *result_ptr_;
  duckdb_data_chunk data_chunk_;

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
      // TODO: interrupt
      // TODO: query_progress
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
      // TODO: column_logical_type
      InstanceMethod("column_count", &DuckDBNodeAddon::column_count),
      InstanceMethod("rows_changed", &DuckDBNodeAddon::rows_changed),
      InstanceMethod("result_return_type", &DuckDBNodeAddon::result_return_type),

      InstanceMethod("vector_size", &DuckDBNodeAddon::vector_size),

      InstanceMethod("create_logical_type", &DuckDBNodeAddon::create_logical_type),
      InstanceMethod("logical_type_get_alias", &DuckDBNodeAddon::logical_type_get_alias),
      InstanceMethod("create_list_type", &DuckDBNodeAddon::create_list_type),
      InstanceMethod("create_array_type", &DuckDBNodeAddon::create_array_type),
      InstanceMethod("create_map_type", &DuckDBNodeAddon::create_map_type),
      // TODO: duckdb_create_union_type
      // TODO: duckdb_create_struct_type
      InstanceMethod("create_enum_type", &DuckDBNodeAddon::create_enum_type),
      InstanceMethod("create_decimal_type", &DuckDBNodeAddon::create_decimal_type),
      InstanceMethod("get_type_id", &DuckDBNodeAddon::get_type_id),
      InstanceMethod("decimal_width", &DuckDBNodeAddon::decimal_width),
      InstanceMethod("decimal_scale", &DuckDBNodeAddon::decimal_scale),
      InstanceMethod("decimal_internal_type", &DuckDBNodeAddon::decimal_internal_type),
      InstanceMethod("enum_internal_type", &DuckDBNodeAddon::enum_internal_type),
      InstanceMethod("enum_dictionary_size", &DuckDBNodeAddon::enum_dictionary_size),
      InstanceMethod("enum_dictionary_value", &DuckDBNodeAddon::enum_dictionary_value),
      InstanceMethod("list_type_child_type", &DuckDBNodeAddon::list_type_child_type),
      InstanceMethod("array_type_child_type", &DuckDBNodeAddon::array_type_child_type),
      InstanceMethod("array_type_array_size", &DuckDBNodeAddon::array_type_array_size),
      InstanceMethod("map_type_key_type", &DuckDBNodeAddon::map_type_key_type),
      InstanceMethod("map_type_value_type", &DuckDBNodeAddon::map_type_value_type),
      // TODO: ...
      InstanceMethod("destroy_logical_type", &DuckDBNodeAddon::destroy_logical_type),

      // TODO: duckdb_create_data_chunk
      InstanceMethod("destroy_data_chunk", &DuckDBNodeAddon::destroy_data_chunk),
      // TODO: data_chunk_reset
      InstanceMethod("data_chunk_get_column_count", &DuckDBNodeAddon::data_chunk_get_column_count),
      InstanceMethod("data_chunk_get_vector", &DuckDBNodeAddon::data_chunk_get_vector),
      InstanceMethod("data_chunk_get_size", &DuckDBNodeAddon::data_chunk_get_size),
      // TODO: data_chunk_set_size

      // TODO: duckdb_vector_get_column_type
      InstanceMethod("vector_get_data", &DuckDBNodeAddon::vector_get_data),
      InstanceMethod("vector_get_validity", &DuckDBNodeAddon::vector_get_validity),
      // TODO: duckdb_vector_ensure_validity_writable
      // TODO: duckdb_vector_assign_string_element
      InstanceMethod("list_vector_get_child", &DuckDBNodeAddon::list_vector_get_child),
      InstanceMethod("list_vector_get_size", &DuckDBNodeAddon::list_vector_get_size),
      // TODO: duckdb_list_vector_set_size
      // TODO: duckdb_list_vector_reserve
      InstanceMethod("struct_vector_get_child", &DuckDBNodeAddon::struct_vector_get_child),
      InstanceMethod("array_vector_get_child", &DuckDBNodeAddon::array_vector_get_child),
      InstanceMethod("validity_row_is_valid", &DuckDBNodeAddon::validity_row_is_valid),
      // TODO: duckdb_validity_set_row_validity
      // TODO: duckdb_validity_set_row_invalid
      // TODO: duckdb_validity_set_row_valid

      InstanceMethod("fetch_chunk", &DuckDBNodeAddon::fetch_chunk),

      InstanceMethod("get_data_from_pointer", &DuckDBNodeAddon::get_data_from_pointer),
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
  // not exposed: consolidated into open

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
  // TODO

  // duckdb_query_progress_type duckdb_query_progress(duckdb_connection connection)
  // TODO

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
    duckdb_free(result_ptr); // memory for duckdb_result struct is malloc'd in QueryWorker
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
  // TODO

  // idx_t duckdb_column_count(duckdb_result *result)
  // function column_count(result: Result): number
  Napi::Value column_count(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto column_count = duckdb_column_count(result_ptr);
    return Napi::Number::New(env, column_count);
  }

  // idx_t duckdb_rows_changed(duckdb_result *result)
  // function rows_changed(result: Result): number
  Napi::Value rows_changed(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto rows_changed = duckdb_rows_changed(result_ptr);
    return Napi::Number::New(env, rows_changed);
  }

  // const char *duckdb_result_error(duckdb_result *result)
  // not exposed: query rejects promise with error

  // duckdb_result_type duckdb_result_return_type(duckdb_result result)
  // function result_return_type(result: Result): ResultType
  Napi::Value result_return_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto result_type = duckdb_result_return_type(*result_ptr);
    return Napi::Number::New(env, result_type);
  }

  // void *duckdb_malloc(size_t size)
  // not exposed: only used internally

  // void duckdb_free(void *ptr)
  // not exposed: only used internally

  // idx_t duckdb_vector_size()
  // function vector_size(): number
  Napi::Value vector_size(const Napi::CallbackInfo& info) {
    return Napi::Number::New(info.Env(), duckdb_vector_size());
  }

  // bool duckdb_string_is_inlined(duckdb_string_t string)
  // not exposed: handled internally

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
  // function create_logical_type(type: Type): LogicalType
  Napi::Value create_logical_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto type = static_cast<duckdb_type>(info[0].As<Napi::Number>().Uint32Value());
    auto logical_type = duckdb_create_logical_type(type);
    return CreateExternalForLogicalType(env, logical_type);
  }

  // char *duckdb_logical_type_get_alias(duckdb_logical_type type)
  // function logical_type_get_alias(logical_type: LogicalType): string | null
  Napi::Value logical_type_get_alias(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto alias = duckdb_logical_type_get_alias(logical_type);
    if (!alias) {
      return env.Null();
    }
    auto str = Napi::String::New(env, alias); 
    duckdb_free(alias);
    return str;
  }

  // duckdb_logical_type duckdb_create_list_type(duckdb_logical_type type)
  // function create_list_type(logical_type: LogicalType): LogicalType
  Napi::Value create_list_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto child_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto list_logical_type = duckdb_create_list_type(child_logical_type);
    return CreateExternalForLogicalType(env, list_logical_type);
  }

  // duckdb_logical_type duckdb_create_array_type(duckdb_logical_type type, idx_t array_size)
  // function create_array_type(logical_type: LogicalType, array_size: number): LogicalType
  Napi::Value create_array_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto child_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto array_size = info[1].As<Napi::Number>().Uint32Value();
    auto array_logical_type = duckdb_create_array_type(child_logical_type, array_size);
    return CreateExternalForLogicalType(env, array_logical_type);
  }

  // duckdb_logical_type duckdb_create_map_type(duckdb_logical_type key_type, duckdb_logical_type value_type)
  // function create_map_type(key_type: LogicalType, value_type: LogicalType): LogicalType
  Napi::Value create_map_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto key_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto value_logical_type = GetLogicalTypeFromExternal(env, info[1]);
    auto map_logical_type = duckdb_create_map_type(key_logical_type, value_logical_type);
    return CreateExternalForLogicalType(env, map_logical_type);
  }

  // duckdb_logical_type duckdb_create_union_type(duckdb_logical_type *member_types, const char **member_names, idx_t member_count)
  // function create_union_type(member_types: LogicalType[], member_names: string[]): LogicalType


  // duckdb_logical_type duckdb_create_struct_type(duckdb_logical_type *member_types, const char **member_names, idx_t member_count)
  // function create_struct_type(member_types: LogicalType[], member_names: string[]): LogicalType


  // duckdb_logical_type duckdb_create_enum_type(const char **member_names, idx_t member_count)
  // function create_enum_type(member_names: string[]): LogicalType
  Napi::Value create_enum_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto member_names_array = info[0].As<Napi::Array>();
    auto member_count = member_names_array.Length();
    std::vector<std::string> member_names_strings(member_count);
    std::vector<const char *> member_names(member_count);
    for (uint32_t i = 0; i < member_count; i++) {
      member_names_strings[i] = member_names_array.Get(i).As<Napi::String>();
      member_names[i] = member_names_strings[i].c_str();
    }
    auto enum_logical_type = duckdb_create_enum_type(member_names.data(), member_count);
    return CreateExternalForLogicalType(env, enum_logical_type);
  }

  // duckdb_logical_type duckdb_create_decimal_type(uint8_t width, uint8_t scale)
  // function create_decimal_type(width: number, scale: number): LogicalType
  Napi::Value create_decimal_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto width = info[0].As<Napi::Number>().Uint32Value();
    auto scale = info[1].As<Napi::Number>().Uint32Value();
    auto decimal_logical_type = duckdb_create_decimal_type(width, scale);
    return CreateExternalForLogicalType(env, decimal_logical_type);
  }

  // duckdb_type duckdb_get_type_id(duckdb_logical_type type)
  // function get_type_id(logical_type: LogicalType): Type
  Napi::Value get_type_id(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto type = duckdb_get_type_id(logical_type);
    return Napi::Number::New(env, type);
  }

  // uint8_t duckdb_decimal_width(duckdb_logical_type type)
  // function decimal_width(logical_type: LogicalType): number
  Napi::Value decimal_width(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto decimal_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto width = duckdb_decimal_width(decimal_logical_type);
    return Napi::Number::New(env, width);
  }

  // uint8_t duckdb_decimal_scale(duckdb_logical_type type)
  // function decimal_scale(logical_type: LogicalType): number
  Napi::Value decimal_scale(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto decimal_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto width = duckdb_decimal_scale(decimal_logical_type);
    return Napi::Number::New(env, width);
  }

  // duckdb_type duckdb_decimal_internal_type(duckdb_logical_type type)
  // function decimal_internal_type(logical_type: LogicalType): Type
  Napi::Value decimal_internal_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto decimal_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto type = duckdb_decimal_internal_type(decimal_logical_type);
    return Napi::Number::New(env, type);
  }

  // duckdb_type duckdb_enum_internal_type(duckdb_logical_type type)
  // function enum_internal_type(logical_type: LogicalType): Type
  Napi::Value enum_internal_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto enum_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto type = duckdb_enum_internal_type(enum_logical_type);
    return Napi::Number::New(env, type);
  }

  // uint32_t duckdb_enum_dictionary_size(duckdb_logical_type type)
  // function enum_dictionary_size(logical_type: LogicalType): number
  Napi::Value enum_dictionary_size(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto enum_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto size = duckdb_enum_dictionary_size(enum_logical_type);
    return Napi::Number::New(env, size);
  }

  // char *duckdb_enum_dictionary_value(duckdb_logical_type type, idx_t index)
  // function enum_dictionary_value(logical_type: LogicalType, index: number): string
  Napi::Value enum_dictionary_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto enum_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = duckdb_enum_dictionary_value(enum_logical_type, index);
    return Napi::String::New(env, value);
  }

  // duckdb_logical_type duckdb_list_type_child_type(duckdb_logical_type type)
  // function list_type_child_type(logical_type: LogicalType): LogicalType
  Napi::Value list_type_child_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto list_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto child_logical_type = duckdb_list_type_child_type(list_logical_type);
    return CreateExternalForLogicalType(env, child_logical_type);
  }

  // duckdb_logical_type duckdb_array_type_child_type(duckdb_logical_type type)
  // function array_type_child_type(logical_type: LogicalType): LogicalType
  Napi::Value array_type_child_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto array_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto child_logical_type = duckdb_array_type_child_type(array_logical_type);
    return CreateExternalForLogicalType(env, child_logical_type);
  }

  // idx_t duckdb_array_type_array_size(duckdb_logical_type type)
  // function array_type_array_size(logical_type: LogicalType): number
  Napi::Value array_type_array_size(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto array_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto array_size = duckdb_array_type_array_size(array_logical_type);
    return Napi::Number::New(env, array_size);
  }

  // duckdb_logical_type duckdb_map_type_key_type(duckdb_logical_type type)
  // function map_type_key_type(logical_type: LogicalType): LogicalType
  Napi::Value map_type_key_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto map_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto key_logical_type = duckdb_map_type_key_type(map_logical_type);
    return CreateExternalForLogicalType(env, key_logical_type);
  }

  // duckdb_logical_type duckdb_map_type_value_type(duckdb_logical_type type)
  // function map_type_value_type(logical_type: LogicalType): LogicalType
  Napi::Value map_type_value_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto map_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto value_logical_type = duckdb_map_type_value_type(map_logical_type);
    return CreateExternalForLogicalType(env, value_logical_type);
  }

  // idx_t duckdb_struct_type_child_count(duckdb_logical_type type)
  // char *duckdb_struct_type_child_name(duckdb_logical_type type, idx_t index)
  // duckdb_logical_type duckdb_struct_type_child_type(duckdb_logical_type type, idx_t index)
  // idx_t duckdb_union_type_member_count(duckdb_logical_type type)
  // char *duckdb_union_type_member_name(duckdb_logical_type type, idx_t index)
  // duckdb_logical_type duckdb_union_type_member_type(duckdb_logical_type type, idx_t index)

  // void duckdb_destroy_logical_type(duckdb_logical_type *type)
  // function destroy_logical_type(logical_type: LogicalType): void
  Napi::Value destroy_logical_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto logical_type = GetLogicalTypeFromExternal(env, info[0]);
    duckdb_destroy_logical_type(&logical_type);
    return env.Undefined();
  }

  // duckdb_data_chunk duckdb_create_data_chunk(duckdb_logical_type *types, idx_t column_count)
  // TODO

  // void duckdb_destroy_data_chunk(duckdb_data_chunk *chunk)
  // function destroy_data_chunk(chunk: DataChunk): void
  Napi::Value destroy_data_chunk(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto chunk = GetDataChunkFromExternal(env, info[0]);
    duckdb_destroy_data_chunk(&chunk);
    return env.Undefined();
  }

  // void duckdb_data_chunk_reset(duckdb_data_chunk chunk)
  // TODO

  // idx_t duckdb_data_chunk_get_column_count(duckdb_data_chunk chunk)
  // function data_chunk_get_column_count(chunk: DataChunk): number
  Napi::Value data_chunk_get_column_count(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto chunk = GetDataChunkFromExternal(env, info[0]);
    auto column_count = duckdb_data_chunk_get_column_count(chunk);
    return Napi::Number::New(env, column_count);
  }

  // duckdb_vector duckdb_data_chunk_get_vector(duckdb_data_chunk chunk, idx_t col_idx)
  // function data_chunk_get_vector(chunk: DataChunk, column_index: number): Vector
  Napi::Value data_chunk_get_vector(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto chunk = GetDataChunkFromExternal(env, info[0]);
    auto column_index = info[1].As<Napi::Number>().Uint32Value();
    auto vector = duckdb_data_chunk_get_vector(chunk, column_index);
    return CreateExternalForVector(env, vector);
  }

  // idx_t duckdb_data_chunk_get_size(duckdb_data_chunk chunk)
  // function data_chunk_get_size(chunk: DataChunk): number
  Napi::Value data_chunk_get_size(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto chunk = GetDataChunkFromExternal(env, info[0]);
    auto size = duckdb_data_chunk_get_size(chunk);
    return Napi::Number::New(env, size);
  }

  // void duckdb_data_chunk_set_size(duckdb_data_chunk chunk, idx_t size)
  // TODO

  // duckdb_logical_type duckdb_vector_get_column_type(duckdb_vector vector)
  // TODO

  // void *duckdb_vector_get_data(duckdb_vector vector)
  // function vector_get_data(vector: Vector, byte_count: number): Uint8Array
  Napi::Value vector_get_data(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto byte_count = info[1].As<Napi::Number>().Uint32Value();
    void *data = duckdb_vector_get_data(vector);
    return Napi::Buffer<uint8_t>::NewOrCopy(env, reinterpret_cast<uint8_t*>(data), byte_count);
  }

  // uint64_t *duckdb_vector_get_validity(duckdb_vector vector)
  // function vector_get_validity(vector: Vector, byte_count: number): Uint8Array
  Napi::Value vector_get_validity(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto byte_count = info[1].As<Napi::Number>().Uint32Value();
    uint64_t *data = duckdb_vector_get_validity(vector);
    return Napi::Buffer<uint8_t>::NewOrCopy(env, reinterpret_cast<uint8_t*>(data), byte_count);
  }

  // void duckdb_vector_ensure_validity_writable(duckdb_vector vector)
  // TODO

  // void duckdb_vector_assign_string_element(duckdb_vector vector, idx_t index, const char *str)
  // TODO
  
  // void duckdb_vector_assign_string_element_len(duckdb_vector vector, idx_t index, const char *str, idx_t str_len)
  // not exposed: JS string includes length

  // duckdb_vector duckdb_list_vector_get_child(duckdb_vector vector)
  // function list_vector_get_child(vector: Vector): Vector
  Napi::Value list_vector_get_child(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto child = duckdb_list_vector_get_child(vector);
    return CreateExternalForVector(env, child);
  }

  // idx_t duckdb_list_vector_get_size(duckdb_vector vector)
  // function list_vector_get_size(vector: Vector): number
  Napi::Value list_vector_get_size(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto size = duckdb_list_vector_get_size(vector);
    return Napi::Number::New(env, size);
  }

  // duckdb_state duckdb_list_vector_set_size(duckdb_vector vector, idx_t size)
  // duckdb_state duckdb_list_vector_reserve(duckdb_vector vector, idx_t required_capacity)

  // duckdb_vector duckdb_struct_vector_get_child(duckdb_vector vector, idx_t index)
  // function struct_vector_get_child(vector: Vector, index: number): Vector
  Napi::Value struct_vector_get_child(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto child = duckdb_struct_vector_get_child(vector, index);
    return CreateExternalForVector(env, child);
  }

  // duckdb_vector duckdb_array_vector_get_child(duckdb_vector vector)
  // function array_vector_get_child(vector: Vector): Vector
  Napi::Value array_vector_get_child(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto child = duckdb_array_vector_get_child(vector);
    return CreateExternalForVector(env, child);
  }

  // bool duckdb_validity_row_is_valid(uint64_t *validity, idx_t row)
  // function validity_row_is_valid(validity: Uint8Array, row_index: number): boolean
  Napi::Value validity_row_is_valid(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto validity = reinterpret_cast<uint64_t*>(info[0].As<Napi::Uint8Array>().Data());
    auto row_index = info[1].As<Napi::Number>().Uint32Value();
    auto valid = duckdb_validity_row_is_valid(validity, row_index);
    return Napi::Boolean::New(env, valid);
  }

  // void duckdb_validity_set_row_validity(uint64_t *validity, idx_t row, bool valid)
  // TODO

  // void duckdb_validity_set_row_invalid(uint64_t *validity, idx_t row)
  // TODO

  // void duckdb_validity_set_row_valid(uint64_t *validity, idx_t row)
  // TODO

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
  // function fetch_chunk(result: Result): Promise<DataChunk>
  Napi::Value fetch_chunk(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto worker = new FetchWorker(env, result_ptr);
    worker->Queue();
    return worker->Promise();
  }

  // ADDED
  // function get_data_from_pointer(array_buffer: ArrayBuffer, pointer_offset: number, byte_count: number): Uint8Array
  Napi::Value get_data_from_pointer(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto data = reinterpret_cast<uint8_t*>(info[0].As<Napi::ArrayBuffer>().Data());
    auto pointer_offset = info[1].As<Napi::Number>().Uint32Value();
    auto byte_count = info[2].As<Napi::Number>().Uint32Value();
    auto pointer_pointer = reinterpret_cast<uint8_t**>(data + pointer_offset);
    auto pointer = *pointer_pointer;
    return Napi::Buffer<uint8_t>::NewOrCopy(env, pointer, byte_count);
  }

};

NODE_API_ADDON(DuckDBNodeAddon)
