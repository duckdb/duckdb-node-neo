#pragma once

#include "externals.h"
#include "duckdb_thread_callback.h"
#include "type_tags.h"
#include "napi_ref_reaper.h"
#include <memory>

// Scalar functions
//
// Everything specific to the scalar function family lives here, including its
// external: the object behind that external is ScalarFunctionHolder, which owns
// family-specific state and so does not belong in externals.h. Later function
// families should follow the same shape.

// Callbacks

struct ScalarFunctionBindCallbackTraits {
  using Payload = duckdb_bind_info;

  static const char *ResourceName() {
    return "ScalarFunctionBind";
  }

  static void Call(Napi::Env env, Napi::Function callback, const Payload &payload) {
    callback.Call(
      env.Undefined(),
      {
        CreateExternalForBindInfoWithoutFinalizer(env, payload)
      }
    );
  }

  static void SetError(const Payload &payload, const char *message) {
    duckdb_scalar_function_bind_set_error(payload, message);
  }
};

struct ScalarFunctionMainCallbackTraits {
  struct Payload {
    duckdb_function_info info;
    duckdb_data_chunk input;
    duckdb_vector output;
  };

  static const char *ResourceName() {
    return "ScalarFunctionMain";
  }

  static void Call(Napi::Env env, Napi::Function callback, const Payload &payload) {
    callback.Call(
      env.Undefined(),
      {
        CreateExternalForFunctionInfoWithoutFinalizer(env, payload.info),
        CreateExternalForDataChunkWithoutFinalizer(env, payload.input),
        CreateExternalForVectorWithoutFinalizer(env, payload.output)
      }
    );
  }

  static void SetError(const Payload &payload, const char *message) {
    duckdb_scalar_function_set_error(payload.info, message);
  }
};

// Extra info

struct ScalarFunctionInternalExtraInfo {
  DuckDBThreadCallback<ScalarFunctionBindCallbackTraits> bind_callback;
  DuckDBThreadCallback<ScalarFunctionMainCallbackTraits> main_callback;
  std::shared_ptr<ManagedObjectReference> user_extra_info_ref;

  explicit ScalarFunctionInternalExtraInfo(const std::shared_ptr<NapiRefReaper> &env_state)
    : bind_callback(env_state), main_callback(env_state) {}

  void SetBindFunction(Napi::Env env, Napi::Function func) {
    bind_callback.Set(env, func);
  }

  void SetMainFunction(Napi::Env env, Napi::Function func) {
    main_callback.Set(env, func);
  }

  void SetUserExtraInfo(const std::shared_ptr<NapiRefReaper> &reaper, Napi::Object user_extra_info) {
    user_extra_info_ref = user_extra_info.IsUndefined() ? nullptr : MakeManagedObjectReference(reaper, user_extra_info);
  }
};

inline void DeleteScalarFunctionInternalExtraInfo(ScalarFunctionInternalExtraInfo *internal_extra_info) {
  delete internal_extra_info;
}

// External

struct ScalarFunctionHolder {
  duckdb_scalar_function scalar_function;
  ScalarFunctionInternalExtraInfo *internal_extra_info;

  ScalarFunctionHolder(duckdb_scalar_function scalar_function_in): scalar_function(scalar_function_in), internal_extra_info(nullptr) {}

  ~ScalarFunctionHolder() {
    // duckdb_destroy_scalar_function is a no-op if already destroyed
    duckdb_destroy_scalar_function(&scalar_function);
  }

  ScalarFunctionInternalExtraInfo *EnsureInternalExtraInfo(const std::shared_ptr<NapiRefReaper> &env_state) {
    if (!internal_extra_info) {
      internal_extra_info = new ScalarFunctionInternalExtraInfo(env_state);
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

// Bind data

struct ScalarFunctionInternalBindData {
  std::shared_ptr<ManagedObjectReference> user_bind_data_ref;

  void SetUserBindData(const std::shared_ptr<NapiRefReaper> &reaper, Napi::Object user_bind_data) {
    user_bind_data_ref = user_bind_data.IsUndefined() ? nullptr : MakeManagedObjectReference(reaper, user_bind_data);
  }
};

// Called by DuckDB when the bound expression is destroyed, on whatever thread
// tears down the plan. Dropping the last reference hands it to the reaper, which
// deletes it on the JS thread.
inline void DeleteScalarFunctionInternalBindData(ScalarFunctionInternalBindData *internal_bind_data) {
  delete internal_bind_data;
}

// Called by DuckDB when the bound expression is copied, also on an arbitrary
// thread. Sharing the reference makes this a refcount bump, so no N-API call is
// made here.
//
// No test covers this: 18 query shapes were tried (common subexpressions, filter
// pushdown, joins, set operations, windows, subqueries, CTAS, repeated execution
// of a prepared statement) and DuckDB 1.5.5 did not copy the bind data for any of
// them. Registering the copy callback is still correct, and sharing the reference
// means the path is safe if it is ever reached.
inline ScalarFunctionInternalBindData *CopyScalarFunctionInternalBindData(ScalarFunctionInternalBindData *internal_bind_data) {
  if (!internal_bind_data) {
    return nullptr;
  }
  auto new_internal_bind_data = new ScalarFunctionInternalBindData();
  new_internal_bind_data->user_bind_data_ref = internal_bind_data->user_bind_data_ref;
  return new_internal_bind_data;
}

// Entry points handed to DuckDB

inline ScalarFunctionInternalExtraInfo *GetScalarFunctionInternalExtraInfoFromBindInfo(duckdb_bind_info bind_info) {
  return reinterpret_cast<ScalarFunctionInternalExtraInfo*>(duckdb_scalar_function_bind_get_extra_info(bind_info));
}

inline void ScalarFunctionBindFunction(duckdb_bind_info info) {
  GetScalarFunctionInternalExtraInfoFromBindInfo(info)->bind_callback.Invoke(info);
}

inline ScalarFunctionInternalExtraInfo *GetScalarFunctionInternalExtraInfoFromFunctionInfo(duckdb_function_info function_info) {
  return reinterpret_cast<ScalarFunctionInternalExtraInfo*>(duckdb_scalar_function_get_extra_info(function_info));
}

inline void ScalarFunctionMainFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
  GetScalarFunctionInternalExtraInfoFromFunctionInfo(info)->main_callback.Invoke({info, input, output});
}
