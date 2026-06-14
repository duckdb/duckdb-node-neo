#pragma once

#include "napi_setup.h"
#include "duckdb.h"
#include <cstddef>
#include <memory>

// Externals

template<typename T, typename Finalizer>
Napi::External<T> CreateExternal(Napi::Env env, const napi_type_tag &type_tag, T *data, Finalizer finalizer) {
  auto external = Napi::External<T>::New(env, data, finalizer);
  external.TypeTag(&type_tag);
  return external;
}

template<typename T>
Napi::External<T> CreateExternalWithoutFinalizer(Napi::Env env, const napi_type_tag &type_tag, T *data) {
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

static const napi_type_tag AppenderTypeTag = {
  0x32E0AB3B83F74A89, 0xB785905D92D54996
};

inline void FinalizeAppender(Napi::BasicEnv, duckdb_appender appender) {
  if (appender) {
    duckdb_appender_destroy(&appender);
    appender = nullptr;
  }
}

inline Napi::External<_duckdb_appender> CreateExternalForAppender(Napi::Env env, duckdb_appender appender) {
  return CreateExternal<_duckdb_appender>(env, AppenderTypeTag, appender, FinalizeAppender);
}

inline duckdb_appender GetAppenderFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_appender>(env, AppenderTypeTag, value, "Invalid appender argument");
}

static const napi_type_tag BindInfoTypeTag = {
  0x9922F8468F9A43C0, 0xB2573B112B9600D8
};

inline Napi::External<_duckdb_bind_info> CreateExternalForBindInfoWithoutFinalizer(Napi::Env env, duckdb_bind_info bind_info) {
  // BindInfo objects are never explicitly created; they are passed in to function callbacks.
  return CreateExternalWithoutFinalizer<_duckdb_bind_info>(env, BindInfoTypeTag, bind_info);
}

inline duckdb_bind_info GetBindInfoFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_bind_info>(env, BindInfoTypeTag, value, "Invalid bind info argument");
}

static const napi_type_tag ClientContextTypeTag = {
  0x1E1738782ED94232, 0x867B024D1858DF3A
};

inline void FinalizeClientContext(Napi::BasicEnv, duckdb_client_context client_context) {
  duckdb_destroy_client_context(&client_context);
}

inline Napi::External<_duckdb_client_context> CreateExternalForClientContext(Napi::Env env, duckdb_client_context client_context) {
  return CreateExternal<_duckdb_client_context>(env, ClientContextTypeTag, client_context, FinalizeClientContext);
}

inline duckdb_client_context GetClientContextFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_client_context>(env, ClientContextTypeTag, value, "Invalid client context argument");
}

static const napi_type_tag ConfigTypeTag = {
  0x5963FBB9648B4D2A, 0xB41ADE86056218D1
};

inline void FinalizeConfig(Napi::BasicEnv, duckdb_config config) {
  if (config) {
    duckdb_destroy_config(&config);
    config = nullptr;
  }
}

inline Napi::External<_duckdb_config> CreateExternalForConfig(Napi::Env env, duckdb_config config) {
  return CreateExternal<_duckdb_config>(env, ConfigTypeTag, config, FinalizeConfig);
}

inline duckdb_config GetConfigFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_config>(env, ConfigTypeTag, value, "Invalid config argument");
}

static const napi_type_tag ConnectionTypeTag = {
  0x922B9BF54AB04DFC, 0x8A258578D371DB71
};

typedef struct {
  duckdb_connection connection;
} duckdb_connection_holder;

inline duckdb_connection_holder *CreateConnectionHolder(duckdb_connection connection) {
  auto connection_holder_ptr = reinterpret_cast<duckdb_connection_holder*>(duckdb_malloc(sizeof(duckdb_connection_holder)));
  connection_holder_ptr->connection = connection;
  return connection_holder_ptr;
}

inline void FinalizeConnectionHolder(Napi::BasicEnv, duckdb_connection_holder *connection_holder_ptr) {
  // duckdb_disconnect is a no-op if already disconnected
  duckdb_disconnect(&connection_holder_ptr->connection);
  duckdb_free(connection_holder_ptr);
}

inline Napi::External<duckdb_connection_holder> CreateExternalForConnection(Napi::Env env, duckdb_connection connection) {
  return CreateExternal<duckdb_connection_holder>(env, ConnectionTypeTag, CreateConnectionHolder(connection), FinalizeConnectionHolder);
}

inline duckdb_connection_holder *GetConnectionHolderFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<duckdb_connection_holder>(env, ConnectionTypeTag, value, "Invalid connection argument");
}

inline duckdb_connection GetConnectionFromExternal(Napi::Env env, Napi::Value value) {
  return GetConnectionHolderFromExternal(env, value)->connection;
}

static const napi_type_tag DatabaseTypeTag = {
  0x835A8533653C40D1, 0x83B3BE2B233BA8F3
};

typedef struct {
  duckdb_database database;
} duckdb_database_holder;

inline duckdb_database_holder *CreateDatabaseHolder(duckdb_database database) {
  auto database_holder_ptr = reinterpret_cast<duckdb_database_holder*>(duckdb_malloc(sizeof(duckdb_database_holder)));
  database_holder_ptr->database = database;
  return database_holder_ptr;
}

inline void FinalizeDatabaseHolder(Napi::BasicEnv, duckdb_database_holder *database_holder_ptr) {
  // duckdb_close is a no-op if already closed
  duckdb_close(&database_holder_ptr->database);
  duckdb_free(database_holder_ptr);
}

inline Napi::External<duckdb_database_holder> CreateExternalForDatabase(Napi::Env env, duckdb_database database) {
  return CreateExternal<duckdb_database_holder>(env, DatabaseTypeTag, CreateDatabaseHolder(database), FinalizeDatabaseHolder);
}

inline duckdb_database_holder *GetDatabaseHolderFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<duckdb_database_holder>(env, DatabaseTypeTag, value, "Invalid database argument");
}

inline duckdb_database GetDatabaseFromExternal(Napi::Env env, Napi::Value value) {
  return GetDatabaseHolderFromExternal(env, value)->database;
}

static const napi_type_tag DataChunkTypeTag = {
  0x2C7537AB063A4296, 0xB1E70F08B0BBD1A3
};

inline void FinalizeDataChunk(Napi::BasicEnv, duckdb_data_chunk chunk) {
  if (chunk) {
    duckdb_destroy_data_chunk(&chunk);
    chunk = nullptr;
  }
}

inline Napi::External<_duckdb_data_chunk> CreateExternalForDataChunk(Napi::Env env, duckdb_data_chunk chunk) {
  return CreateExternal<_duckdb_data_chunk>(env, DataChunkTypeTag, chunk, FinalizeDataChunk);
}

inline Napi::External<_duckdb_data_chunk> CreateExternalForDataChunkWithoutFinalizer(Napi::Env env, duckdb_data_chunk chunk) {
  return CreateExternalWithoutFinalizer<_duckdb_data_chunk>(env, DataChunkTypeTag, chunk);
}

inline duckdb_data_chunk GetDataChunkFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_data_chunk>(env, DataChunkTypeTag, value, "Invalid data chunk argument");
}

static const napi_type_tag ExtractedStatementsTypeTag = {
  0x59288E1C60C44EEB, 0xBFA35376EE0F04DD
};

inline void FinalizeExtractedStatements(Napi::BasicEnv, duckdb_extracted_statements extracted_statements) {
  if (extracted_statements) {
    duckdb_destroy_extracted(&extracted_statements);
    extracted_statements = nullptr;
  }
}

inline Napi::External<_duckdb_extracted_statements> CreateExternalForExtractedStatements(Napi::Env env, duckdb_extracted_statements extracted_statements) {
  return CreateExternal<_duckdb_extracted_statements>(env, ExtractedStatementsTypeTag, extracted_statements, FinalizeExtractedStatements);
}

inline duckdb_extracted_statements GetExtractedStatementsFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_extracted_statements>(env, ExtractedStatementsTypeTag, value, "Invalid extracted statements argument");
}

static const napi_type_tag FunctionInfoTypeTag = {
  0xB0E6739D698048EA, 0x9E79734E3E137AC3
};

inline Napi::External<_duckdb_function_info> CreateExternalForFunctionInfoWithoutFinalizer(Napi::Env env, duckdb_function_info function_info) {
  // FunctionInfo objects are never explicitly created; they are passed in to function callbacks.
  return CreateExternalWithoutFinalizer<_duckdb_function_info>(env, FunctionInfoTypeTag, function_info);
}

inline duckdb_function_info GetFunctionInfoFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_function_info>(env, FunctionInfoTypeTag, value, "Invalid function info argument");
}

static const napi_type_tag InstanceCacheTypeTag = {
  0x2F3346E30FB5457C, 0xB9201EE5112EEF9F
};

inline void FinalizeInstanceCache(Napi::BasicEnv, duckdb_instance_cache instance_cache) {
  duckdb_destroy_instance_cache(&instance_cache);
}

inline Napi::External<_duckdb_instance_cache> CreateExternalForInstanceCache(Napi::Env env, duckdb_instance_cache instance_cache) {
  return CreateExternal<_duckdb_instance_cache>(env, InstanceCacheTypeTag, instance_cache, FinalizeInstanceCache);
}

inline duckdb_instance_cache GetInstanceCacheFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_instance_cache>(env, InstanceCacheTypeTag, value, "Invalid instance cache argument");
}

static const napi_type_tag LogicalTypeTypeTag = {
  0x78AF202191ED4A23, 0x8093715369592A2B
};

inline void FinalizeLogicalType(Napi::BasicEnv, duckdb_logical_type logical_type) {
  duckdb_destroy_logical_type(&logical_type);
}

inline Napi::External<_duckdb_logical_type> CreateExternalForLogicalType(Napi::Env env, duckdb_logical_type logical_type) {
  return CreateExternal<_duckdb_logical_type>(env, LogicalTypeTypeTag, logical_type, FinalizeLogicalType);
}

inline Napi::External<_duckdb_logical_type> CreateExternalForLogicalTypeWithoutFinalizer(Napi::Env env, duckdb_logical_type logical_type) {
  return CreateExternalWithoutFinalizer<_duckdb_logical_type>(env, LogicalTypeTypeTag, logical_type);
}

inline duckdb_logical_type GetLogicalTypeFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_logical_type>(env, LogicalTypeTypeTag, value, "Invalid logical type argument");
}

static const napi_type_tag PendingResultTypeTag = {
  0x257E88ECE8294FEC, 0xB64963BBBD1DBB41
};

inline void FinalizePendingResult(Napi::BasicEnv, duckdb_pending_result pending_result) {
  if (pending_result) {
    duckdb_destroy_pending(&pending_result);
    pending_result = nullptr;
  }
}

inline Napi::External<_duckdb_pending_result> CreateExternalForPendingResult(Napi::Env env, duckdb_pending_result pending_result) {
  return CreateExternal<_duckdb_pending_result>(env, PendingResultTypeTag, pending_result, FinalizePendingResult);
}

inline duckdb_pending_result GetPendingResultFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_pending_result>(env, PendingResultTypeTag, value, "Invalid pending result argument");
}

static const napi_type_tag PreparedStatementTypeTag = {
  0xA8B03DAD16D34416, 0x9735A7E1F2A1240C
};

typedef struct {
  duckdb_prepared_statement prepared;
} duckdb_prepared_statement_holder;

inline duckdb_prepared_statement_holder *CreatePreparedStatementHolder(duckdb_prepared_statement prepared) {
  auto prepared_statement_holder_ptr = reinterpret_cast<duckdb_prepared_statement_holder*>(duckdb_malloc(sizeof(duckdb_prepared_statement_holder)));
  prepared_statement_holder_ptr->prepared = prepared;
  return prepared_statement_holder_ptr;
}

inline void FinalizePreparedStatementHolder(Napi::BasicEnv, duckdb_prepared_statement_holder *prepared_statement_holder_ptr) {
  // duckdb_destroy_prepare is a no-op if already destroyed
  duckdb_destroy_prepare(&prepared_statement_holder_ptr->prepared);
  duckdb_free(prepared_statement_holder_ptr);
}

inline Napi::External<duckdb_prepared_statement_holder> CreateExternalForPreparedStatement(Napi::Env env, duckdb_prepared_statement prepared_statement) {
  return CreateExternal<duckdb_prepared_statement_holder>(env, PreparedStatementTypeTag, CreatePreparedStatementHolder(prepared_statement), FinalizePreparedStatementHolder);
}

inline duckdb_prepared_statement_holder *GetPreparedStatementHolderFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<duckdb_prepared_statement_holder>(env, PreparedStatementTypeTag, value, "Invalid prepared statement argument");
}

inline duckdb_prepared_statement GetPreparedStatementFromExternal(Napi::Env env, Napi::Value value) {
  return GetPreparedStatementHolderFromExternal(env, value)->prepared;
}

static const napi_type_tag ResultTypeTag = {
  0x08F7FE3AE12345E5, 0x8733310DC29372D9
};

inline void FinalizeResult(Napi::BasicEnv, duckdb_result *result_ptr) {
  if (result_ptr) {
    duckdb_destroy_result(result_ptr);
    duckdb_free(result_ptr); // memory for duckdb_result struct is malloc'd in QueryWorker, ExecutePreparedWorker, or ExecutePendingWorker.
    result_ptr = nullptr;
  }
}

inline Napi::External<duckdb_result> CreateExternalForResult(Napi::Env env, duckdb_result *result_ptr) {
  return CreateExternal<duckdb_result>(env, ResultTypeTag, result_ptr, FinalizeResult);
}

inline duckdb_result *GetResultFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<duckdb_result>(env, ResultTypeTag, value, "Invalid result argument");
}

static const napi_type_tag ScalarFunctionTypeTag = {
  0x95D48B7051D14994, 0x9F883D7DF5DEA86D
};

using ScalarFunctionBindTSFNContext = std::nullptr_t;
struct ScalarFunctionBindTSFNData;
void ScalarFunctionBindTSFNCallback(Napi::Env env, Napi::Function callback, ScalarFunctionBindTSFNContext *context, ScalarFunctionBindTSFNData *data);
using ScalarFunctionBindTSFN = Napi::TypedThreadSafeFunction<ScalarFunctionBindTSFNContext, ScalarFunctionBindTSFNData, ScalarFunctionBindTSFNCallback>;

using ScalarFunctionMainTSFNContext = std::nullptr_t;
struct ScalarFunctionMainTSFNData;
void ScalarFunctionMainTSFNCallback(Napi::Env env, Napi::Function callback, ScalarFunctionMainTSFNContext *context, ScalarFunctionMainTSFNData *data);
using ScalarFunctionMainTSFN = Napi::TypedThreadSafeFunction<ScalarFunctionMainTSFNContext, ScalarFunctionMainTSFNData, ScalarFunctionMainTSFNCallback>;

struct ScalarFunctionInternalExtraInfo {
  std::unique_ptr<ScalarFunctionBindTSFN> bind_tsfn;
  std::unique_ptr<ScalarFunctionMainTSFN> main_tsfn;
  std::unique_ptr<Napi::ObjectReference> user_extra_info_ref;

  ScalarFunctionInternalExtraInfo() {}

  ~ScalarFunctionInternalExtraInfo() {
    if (bool(bind_tsfn)) {
      bind_tsfn->Release();
    }
    if (bool(main_tsfn)) {
      main_tsfn->Release();
    }
  }

  void SetBindFunction(Napi::Env env, Napi::Function func) {
    if (bool(bind_tsfn)) {
      bind_tsfn->Release();
    }
    bind_tsfn = std::make_unique<ScalarFunctionBindTSFN>(ScalarFunctionBindTSFN::New(env, func, "ScalarFunctionBind", 0, 1));
  }

  void SetMainFunction(Napi::Env env, Napi::Function func) {
    if (bool(main_tsfn)) {
      main_tsfn->Release();
    }
    main_tsfn = std::make_unique<ScalarFunctionMainTSFN>(ScalarFunctionMainTSFN::New(env, func, "ScalarFunctionMain", 0, 1));
  }

  void SetUserExtraInfo(Napi::Object user_extra_info) {
    user_extra_info_ref = std::make_unique<Napi::ObjectReference>(user_extra_info.IsUndefined() ? Napi::ObjectReference() : Napi::Persistent(user_extra_info));
  }
};

inline void DeleteScalarFunctionInternalExtraInfo(ScalarFunctionInternalExtraInfo *internal_extra_info) {
  delete internal_extra_info;
}

struct ScalarFunctionHolder {
  duckdb_scalar_function scalar_function;
  ScalarFunctionInternalExtraInfo *internal_extra_info;

  ScalarFunctionHolder(duckdb_scalar_function scalar_function_in): scalar_function(scalar_function_in), internal_extra_info(nullptr) {}

  ~ScalarFunctionHolder() {
    // duckdb_destroy_scalar_function is a no-op if already destroyed
    duckdb_destroy_scalar_function(&scalar_function);
  }

  ScalarFunctionInternalExtraInfo *EnsureInternalExtraInfo() {
    if (!internal_extra_info) {
      internal_extra_info = new ScalarFunctionInternalExtraInfo();
      duckdb_scalar_function_set_extra_info(scalar_function, internal_extra_info, reinterpret_cast<duckdb_delete_callback_t>(DeleteScalarFunctionInternalExtraInfo));
    }
    return internal_extra_info;
  }
};

inline ScalarFunctionHolder *CreateScalarFunctionHolder(duckdb_scalar_function scalar_function) {
  return new ScalarFunctionHolder(scalar_function);
}

inline void FinalizeScalarFunctionHolder(Napi::BasicEnv, ScalarFunctionHolder *holder) {
  delete holder;
}

inline Napi::External<ScalarFunctionHolder> CreateExternalForScalarFunction(Napi::Env env, duckdb_scalar_function scalar_function) {
  return CreateExternal<ScalarFunctionHolder>(env, ScalarFunctionTypeTag, CreateScalarFunctionHolder(scalar_function), FinalizeScalarFunctionHolder);
}

inline ScalarFunctionHolder *GetScalarFunctionHolderFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<ScalarFunctionHolder>(env, ScalarFunctionTypeTag, value, "Invalid scalar function argument");
}

inline duckdb_scalar_function GetScalarFunctionFromExternal(Napi::Env env, Napi::Value value) {
  return GetScalarFunctionHolderFromExternal(env, value)->scalar_function;
}

static const napi_type_tag ValueTypeTag = {
  0xC60F36613BF14E93, 0xBAA92848936FAA25
};

inline void FinalizeValue(Napi::BasicEnv, duckdb_value value) {
  if (value) {
    duckdb_destroy_value(&value);
    value = nullptr;
  }
}

inline Napi::External<_duckdb_value> CreateExternalForValue(Napi::Env env, duckdb_value value) {
  return CreateExternal<_duckdb_value>(env, ValueTypeTag, value, FinalizeValue);
}

inline duckdb_value GetValueFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_value>(env, ValueTypeTag, value, "Invalid value argument");
}

static const napi_type_tag VectorTypeTag = {
  0x9FE56DE8E3124D07, 0x9ABF31145EDE1C9E
};

inline Napi::External<_duckdb_vector> CreateExternalForVectorWithoutFinalizer(Napi::Env env, duckdb_vector vector) {
  // Vectors live as long as their containing data chunk; they cannot be explicitly destroyed.
  return CreateExternalWithoutFinalizer<_duckdb_vector>(env, VectorTypeTag, vector);
}

inline duckdb_vector GetVectorFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_vector>(env, VectorTypeTag, value, "Invalid vector argument");
}
