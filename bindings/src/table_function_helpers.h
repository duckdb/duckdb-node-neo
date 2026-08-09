#pragma once

#include "externals.h"
#include "duckdb_thread_callback.h"
#include "type_tags.h"
#include "napi_ref_reaper.h"
#include <memory>

// Table functions
//
// Everything specific to the table function family lives here, including its
// external: the object behind that external is TableFunctionHolder, which owns
// family-specific state and so does not belong in externals.h. This mirrors
// scalar_function_helpers.h.

// Info externals
//
// duckdb_bind_info and duckdb_function_info are the same C types the scalar
// family uses, and duckdb_init_info is shared with scalar function init. The
// struct behind each differs per family and is reinterpret_cast with no check, so
// each family tags its own; see the note in type_tags.h.
//
// None of these is ever created explicitly; all are passed in to callbacks.

inline Napi::External<_duckdb_bind_info> CreateExternalForTableFunctionBindInfo(Napi::Env env, duckdb_bind_info bind_info) {
  return CreateExternalWithoutFinalizer<_duckdb_bind_info>(env, TableFunctionBindInfoTypeTag, bind_info);
}

inline duckdb_bind_info GetTableFunctionBindInfoFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_bind_info>(env, TableFunctionBindInfoTypeTag, value, "Invalid table function bind info argument");
}

inline Napi::External<_duckdb_init_info> CreateExternalForTableFunctionInitInfo(Napi::Env env, duckdb_init_info init_info) {
  return CreateExternalWithoutFinalizer<_duckdb_init_info>(env, TableFunctionInitInfoTypeTag, init_info);
}

inline duckdb_init_info GetTableFunctionInitInfoFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_init_info>(env, TableFunctionInitInfoTypeTag, value, "Invalid table function init info argument");
}

inline Napi::External<_duckdb_function_info> CreateExternalForTableFunctionInfo(Napi::Env env, duckdb_function_info function_info) {
  return CreateExternalWithoutFinalizer<_duckdb_function_info>(env, TableFunctionInfoTypeTag, function_info);
}

inline duckdb_function_info GetTableFunctionInfoFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_function_info>(env, TableFunctionInfoTypeTag, value, "Invalid table function info argument");
}

// Callbacks
//
// Init and local init take the same arguments and report errors the same way.
// They still need distinct traits, so that the two callbacks are distinct types
// and each keeps its own thread-safe function.

struct TableFunctionBindCallbackTraits {
  using Payload = duckdb_bind_info;

  static const char *ResourceName() {
    return "TableFunctionBind";
  }

  static void Call(Napi::Env env, Napi::Function callback, const Payload &payload) {
    callback.Call(
      env.Undefined(),
      {
        CreateExternalForTableFunctionBindInfo(env, payload)
      }
    );
  }

  static void SetError(const Payload &payload, const char *message) {
    duckdb_bind_set_error(payload, message);
  }
};

struct TableFunctionInitCallbackTraits {
  using Payload = duckdb_init_info;

  static const char *ResourceName() {
    return "TableFunctionInit";
  }

  static void Call(Napi::Env env, Napi::Function callback, const Payload &payload) {
    callback.Call(
      env.Undefined(),
      {
        CreateExternalForTableFunctionInitInfo(env, payload)
      }
    );
  }

  static void SetError(const Payload &payload, const char *message) {
    duckdb_init_set_error(payload, message);
  }
};

struct TableFunctionLocalInitCallbackTraits {
  using Payload = duckdb_init_info;

  static const char *ResourceName() {
    return "TableFunctionLocalInit";
  }

  static void Call(Napi::Env env, Napi::Function callback, const Payload &payload) {
    callback.Call(
      env.Undefined(),
      {
        CreateExternalForTableFunctionInitInfo(env, payload)
      }
    );
  }

  static void SetError(const Payload &payload, const char *message) {
    duckdb_init_set_error(payload, message);
  }
};

struct TableFunctionMainCallbackTraits {
  struct Payload {
    duckdb_function_info info;
    duckdb_data_chunk output;
  };

  static const char *ResourceName() {
    return "TableFunctionMain";
  }

  static void Call(Napi::Env env, Napi::Function callback, const Payload &payload) {
    callback.Call(
      env.Undefined(),
      {
        CreateExternalForTableFunctionInfo(env, payload.info),
        CreateExternalForDataChunkWithoutFinalizer(env, payload.output)
      }
    );
  }

  static void SetError(const Payload &payload, const char *message) {
    duckdb_function_set_error(payload.info, message);
  }
};

// Extra info

struct TableFunctionInternalExtraInfo {
  DuckDBThreadCallback<TableFunctionBindCallbackTraits> bind_callback;
  DuckDBThreadCallback<TableFunctionInitCallbackTraits> init_callback;
  DuckDBThreadCallback<TableFunctionLocalInitCallbackTraits> local_init_callback;
  DuckDBThreadCallback<TableFunctionMainCallbackTraits> main_callback;
  std::shared_ptr<ManagedObjectReference> user_extra_info_ref;

  explicit TableFunctionInternalExtraInfo(const std::shared_ptr<NapiRefReaper> &env_state)
    : bind_callback(env_state), init_callback(env_state), local_init_callback(env_state), main_callback(env_state) {}

  void SetBindFunction(Napi::Env env, Napi::Function func) {
    bind_callback.Set(env, func);
  }

  void SetInitFunction(Napi::Env env, Napi::Function func) {
    init_callback.Set(env, func);
  }

  void SetLocalInitFunction(Napi::Env env, Napi::Function func) {
    local_init_callback.Set(env, func);
  }

  void SetMainFunction(Napi::Env env, Napi::Function func) {
    main_callback.Set(env, func);
  }

  void SetUserExtraInfo(const std::shared_ptr<NapiRefReaper> &reaper, Napi::Object user_extra_info) {
    user_extra_info_ref = user_extra_info.IsUndefined() ? nullptr : MakeManagedObjectReference(reaper, user_extra_info);
  }
};

inline void DeleteTableFunctionInternalExtraInfo(TableFunctionInternalExtraInfo *internal_extra_info) {
  delete internal_extra_info;
}

// External

struct TableFunctionHolder {
  duckdb_table_function table_function;
  TableFunctionInternalExtraInfo *internal_extra_info;

  TableFunctionHolder(duckdb_table_function table_function_in): table_function(table_function_in), internal_extra_info(nullptr) {}

  ~TableFunctionHolder() {
    // duckdb_destroy_table_function is a no-op if already destroyed
    duckdb_destroy_table_function(&table_function);
  }

  TableFunctionInternalExtraInfo *EnsureInternalExtraInfo(const std::shared_ptr<NapiRefReaper> &env_state) {
    if (!internal_extra_info) {
      internal_extra_info = new TableFunctionInternalExtraInfo(env_state);
      duckdb_table_function_set_extra_info(table_function, internal_extra_info, reinterpret_cast<duckdb_delete_callback_t>(DeleteTableFunctionInternalExtraInfo));
    }
    return internal_extra_info;
  }
};

inline TableFunctionHolder *CreateTableFunctionHolder(duckdb_table_function table_function) {
  return new TableFunctionHolder(table_function);
}

inline void FinalizeTableFunctionHolder(Napi::BasicEnv, TableFunctionHolder *holder) {
  delete holder;
}

inline Napi::External<TableFunctionHolder> CreateExternalForTableFunction(Napi::Env env, duckdb_table_function table_function) {
  return CreateExternal<TableFunctionHolder>(env, TableFunctionTypeTag, CreateTableFunctionHolder(table_function), FinalizeTableFunctionHolder);
}

inline TableFunctionHolder *GetTableFunctionHolderFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<TableFunctionHolder>(env, TableFunctionTypeTag, value, "Invalid table function argument");
}

inline duckdb_table_function GetTableFunctionFromExternal(Napi::Env env, Napi::Value value) {
  return GetTableFunctionHolderFromExternal(env, value)->table_function;
}

// Bind data and init data
//
// Both hold a user object through the reaper, so that the reference is destroyed
// on the JS thread whatever thread DuckDB tears the query down on. Unlike scalar
// functions there is no copy callback to implement: table function bind data is
// never copied by the C API.
//
// Init data is used for both the global init and the per-thread local init, which
// DuckDB keeps separate but stores the same way.

struct TableFunctionInternalBindData {
  std::shared_ptr<ManagedObjectReference> user_bind_data_ref;

  void SetUserBindData(const std::shared_ptr<NapiRefReaper> &reaper, Napi::Object user_bind_data) {
    user_bind_data_ref = user_bind_data.IsUndefined() ? nullptr : MakeManagedObjectReference(reaper, user_bind_data);
  }
};

inline void DeleteTableFunctionInternalBindData(TableFunctionInternalBindData *internal_bind_data) {
  delete internal_bind_data;
}

struct TableFunctionInternalInitData {
  std::shared_ptr<ManagedObjectReference> user_init_data_ref;

  void SetUserInitData(const std::shared_ptr<NapiRefReaper> &reaper, Napi::Object user_init_data) {
    user_init_data_ref = user_init_data.IsUndefined() ? nullptr : MakeManagedObjectReference(reaper, user_init_data);
  }
};

inline void DeleteTableFunctionInternalInitData(TableFunctionInternalInitData *internal_init_data) {
  delete internal_init_data;
}

// Entry points handed to DuckDB

inline TableFunctionInternalExtraInfo *GetTableFunctionInternalExtraInfoFromBindInfo(duckdb_bind_info bind_info) {
  return reinterpret_cast<TableFunctionInternalExtraInfo*>(duckdb_bind_get_extra_info(bind_info));
}

inline TableFunctionInternalExtraInfo *GetTableFunctionInternalExtraInfoFromInitInfo(duckdb_init_info init_info) {
  return reinterpret_cast<TableFunctionInternalExtraInfo*>(duckdb_init_get_extra_info(init_info));
}

inline TableFunctionInternalExtraInfo *GetTableFunctionInternalExtraInfoFromFunctionInfo(duckdb_function_info function_info) {
  return reinterpret_cast<TableFunctionInternalExtraInfo*>(duckdb_function_get_extra_info(function_info));
}

inline void TableFunctionBindFunction(duckdb_bind_info info) {
  GetTableFunctionInternalExtraInfoFromBindInfo(info)->bind_callback.Invoke(info);
}

inline void TableFunctionInitFunction(duckdb_init_info info) {
  GetTableFunctionInternalExtraInfoFromInitInfo(info)->init_callback.Invoke(info);
}

inline void TableFunctionLocalInitFunction(duckdb_init_info info) {
  GetTableFunctionInternalExtraInfoFromInitInfo(info)->local_init_callback.Invoke(info);
}

inline void TableFunctionMainFunction(duckdb_function_info info, duckdb_data_chunk output) {
  GetTableFunctionInternalExtraInfoFromFunctionInfo(info)->main_callback.Invoke({info, output});
}
