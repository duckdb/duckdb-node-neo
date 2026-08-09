#pragma once

#include "externals.h"
#include <memory>

// Scalar functions

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

// Bind callback

inline const char *ScalarFunctionBindCallbackTraits::ResourceName() {
  return "ScalarFunctionBind";
}

inline void ScalarFunctionBindCallbackTraits::Call(Napi::Env env, Napi::Function callback, const Payload &payload) {
  callback.Call(
    env.Undefined(),
    {
      CreateExternalForBindInfoWithoutFinalizer(env, payload)
    }
  );
}

inline void ScalarFunctionBindCallbackTraits::SetError(const Payload &payload, const char *message) {
  duckdb_scalar_function_bind_set_error(payload, message);
}

inline ScalarFunctionInternalExtraInfo *GetScalarFunctionInternalExtraInfoFromBindInfo(duckdb_bind_info bind_info) {
  return reinterpret_cast<ScalarFunctionInternalExtraInfo*>(duckdb_scalar_function_bind_get_extra_info(bind_info));
}

inline void ScalarFunctionBindFunction(duckdb_bind_info info) {
  GetScalarFunctionInternalExtraInfoFromBindInfo(info)->bind_callback.Invoke(info);
}

// Main callback

inline const char *ScalarFunctionMainCallbackTraits::ResourceName() {
  return "ScalarFunctionMain";
}

inline void ScalarFunctionMainCallbackTraits::Call(Napi::Env env, Napi::Function callback, const Payload &payload) {
  callback.Call(
    env.Undefined(),
    {
      CreateExternalForFunctionInfoWithoutFinalizer(env, payload.info),
      CreateExternalForDataChunkWithoutFinalizer(env, payload.input),
      CreateExternalForVectorWithoutFinalizer(env, payload.output)
    }
  );
}

inline void ScalarFunctionMainCallbackTraits::SetError(const Payload &payload, const char *message) {
  duckdb_scalar_function_set_error(payload.info, message);
}

inline ScalarFunctionInternalExtraInfo *GetScalarFunctionInternalExtraInfoFromFunctionInfo(duckdb_function_info function_info) {
  return reinterpret_cast<ScalarFunctionInternalExtraInfo*>(duckdb_scalar_function_get_extra_info(function_info));
}

inline void ScalarFunctionMainFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
  GetScalarFunctionInternalExtraInfoFromFunctionInfo(info)->main_callback.Invoke({info, input, output});
}
