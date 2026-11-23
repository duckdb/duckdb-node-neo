#pragma once

#include "externals.h"
#include "duckdb_thread_callback.h"
#include "type_tags.h"
#include "napi_ref_reaper.h"
#include <memory>

// Copy functions
//
// Everything specific to the copy function family lives here, including its
// external: the object behind that external is CopyFunctionHolder, which owns
// family-specific state and so does not belong in externals.h. This mirrors
// table_function_helpers.h.
//
// A copy function runs `COPY ... TO` in four callbacks: bind sees the columns
// and options, global init sees the file path and sets up global state, sink
// receives each chunk of rows, and finalize runs once all rows are in.

// Info externals
//
// Unlike the scalar and table families, each copy function callback has its own
// C type for its info, so no two families share a handle type here. Each still
// gets its own tag so that passing the wrong info to an accessor throws instead
// of being reinterpret_cast.
//
// None of these is ever created explicitly; all are passed in to callbacks.

inline Napi::External<_duckdb_copy_function_bind_info> CreateExternalForCopyFunctionBindInfo(Napi::Env env, duckdb_copy_function_bind_info bind_info) {
  return CreateExternalWithoutFinalizer<_duckdb_copy_function_bind_info>(env, CopyFunctionBindInfoTypeTag, bind_info);
}

inline duckdb_copy_function_bind_info GetCopyFunctionBindInfoFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_copy_function_bind_info>(env, CopyFunctionBindInfoTypeTag, value, "Invalid copy function bind info argument");
}

inline Napi::External<_duckdb_copy_function_global_init_info> CreateExternalForCopyFunctionGlobalInitInfo(Napi::Env env, duckdb_copy_function_global_init_info init_info) {
  return CreateExternalWithoutFinalizer<_duckdb_copy_function_global_init_info>(env, CopyFunctionGlobalInitInfoTypeTag, init_info);
}

inline duckdb_copy_function_global_init_info GetCopyFunctionGlobalInitInfoFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_copy_function_global_init_info>(env, CopyFunctionGlobalInitInfoTypeTag, value, "Invalid copy function global init info argument");
}

inline Napi::External<_duckdb_copy_function_sink_info> CreateExternalForCopyFunctionSinkInfo(Napi::Env env, duckdb_copy_function_sink_info sink_info) {
  return CreateExternalWithoutFinalizer<_duckdb_copy_function_sink_info>(env, CopyFunctionSinkInfoTypeTag, sink_info);
}

inline duckdb_copy_function_sink_info GetCopyFunctionSinkInfoFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_copy_function_sink_info>(env, CopyFunctionSinkInfoTypeTag, value, "Invalid copy function sink info argument");
}

inline Napi::External<_duckdb_copy_function_finalize_info> CreateExternalForCopyFunctionFinalizeInfo(Napi::Env env, duckdb_copy_function_finalize_info finalize_info) {
  return CreateExternalWithoutFinalizer<_duckdb_copy_function_finalize_info>(env, CopyFunctionFinalizeInfoTypeTag, finalize_info);
}

inline duckdb_copy_function_finalize_info GetCopyFunctionFinalizeInfoFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_copy_function_finalize_info>(env, CopyFunctionFinalizeInfoTypeTag, value, "Invalid copy function finalize info argument");
}

// Callbacks

struct CopyFunctionBindCallbackTraits {
  using Payload = duckdb_copy_function_bind_info;

  static const char *ResourceName() {
    return "CopyFunctionBind";
  }

  static void Call(Napi::Env env, Napi::Function callback, const Payload &payload) {
    callback.Call(
      env.Undefined(),
      {
        CreateExternalForCopyFunctionBindInfo(env, payload)
      }
    );
  }

  static void SetError(const Payload &payload, const char *message) {
    duckdb_copy_function_bind_set_error(payload, message);
  }
};

struct CopyFunctionGlobalInitCallbackTraits {
  using Payload = duckdb_copy_function_global_init_info;

  static const char *ResourceName() {
    return "CopyFunctionGlobalInit";
  }

  static void Call(Napi::Env env, Napi::Function callback, const Payload &payload) {
    callback.Call(
      env.Undefined(),
      {
        CreateExternalForCopyFunctionGlobalInitInfo(env, payload)
      }
    );
  }

  static void SetError(const Payload &payload, const char *message) {
    duckdb_copy_function_global_init_set_error(payload, message);
  }
};

struct CopyFunctionSinkCallbackTraits {
  struct Payload {
    duckdb_copy_function_sink_info info;
    duckdb_data_chunk input;
  };

  static const char *ResourceName() {
    return "CopyFunctionSink";
  }

  static void Call(Napi::Env env, Napi::Function callback, const Payload &payload) {
    callback.Call(
      env.Undefined(),
      {
        CreateExternalForCopyFunctionSinkInfo(env, payload.info),
        CreateExternalForDataChunkWithoutFinalizer(env, payload.input)
      }
    );
  }

  static void SetError(const Payload &payload, const char *message) {
    duckdb_copy_function_sink_set_error(payload.info, message);
  }
};

struct CopyFunctionFinalizeCallbackTraits {
  using Payload = duckdb_copy_function_finalize_info;

  static const char *ResourceName() {
    return "CopyFunctionFinalize";
  }

  static void Call(Napi::Env env, Napi::Function callback, const Payload &payload) {
    callback.Call(
      env.Undefined(),
      {
        CreateExternalForCopyFunctionFinalizeInfo(env, payload)
      }
    );
  }

  static void SetError(const Payload &payload, const char *message) {
    duckdb_copy_function_finalize_set_error(payload, message);
  }
};

// Extra info

struct CopyFunctionInternalExtraInfo {
  DuckDBThreadCallback<CopyFunctionBindCallbackTraits> bind_callback;
  DuckDBThreadCallback<CopyFunctionGlobalInitCallbackTraits> global_init_callback;
  DuckDBThreadCallback<CopyFunctionSinkCallbackTraits> sink_callback;
  DuckDBThreadCallback<CopyFunctionFinalizeCallbackTraits> finalize_callback;
  std::shared_ptr<ManagedObjectReference> user_extra_info_ref;

  explicit CopyFunctionInternalExtraInfo(const std::shared_ptr<NapiRefReaper> &env_state)
    : bind_callback(env_state), global_init_callback(env_state), sink_callback(env_state), finalize_callback(env_state) {}

  void SetBindFunction(Napi::Env env, Napi::Function func) {
    bind_callback.Set(env, func);
  }

  void SetGlobalInitFunction(Napi::Env env, Napi::Function func) {
    global_init_callback.Set(env, func);
  }

  void SetSinkFunction(Napi::Env env, Napi::Function func) {
    sink_callback.Set(env, func);
  }

  void SetFinalizeFunction(Napi::Env env, Napi::Function func) {
    finalize_callback.Set(env, func);
  }

  void SetUserExtraInfo(const std::shared_ptr<NapiRefReaper> &reaper, Napi::Object user_extra_info) {
    user_extra_info_ref = user_extra_info.IsUndefined() ? nullptr : MakeManagedObjectReference(reaper, user_extra_info);
  }
};

inline void DeleteCopyFunctionInternalExtraInfo(CopyFunctionInternalExtraInfo *internal_extra_info) {
  delete internal_extra_info;
}

// External

struct CopyFunctionHolder {
  duckdb_copy_function copy_function;
  CopyFunctionInternalExtraInfo *internal_extra_info;

  CopyFunctionHolder(duckdb_copy_function copy_function_in): copy_function(copy_function_in), internal_extra_info(nullptr) {}

  ~CopyFunctionHolder() {
    // duckdb_destroy_copy_function is a no-op if already destroyed
    duckdb_destroy_copy_function(&copy_function);
  }

  CopyFunctionInternalExtraInfo *EnsureInternalExtraInfo(const std::shared_ptr<NapiRefReaper> &env_state) {
    if (!internal_extra_info) {
      internal_extra_info = new CopyFunctionInternalExtraInfo(env_state);
      duckdb_copy_function_set_extra_info(copy_function, internal_extra_info, reinterpret_cast<duckdb_delete_callback_t>(DeleteCopyFunctionInternalExtraInfo));
    }
    return internal_extra_info;
  }
};

inline CopyFunctionHolder *CreateCopyFunctionHolder(duckdb_copy_function copy_function) {
  return new CopyFunctionHolder(copy_function);
}

inline void FinalizeCopyFunctionHolder(Napi::BasicEnv, CopyFunctionHolder *holder) {
  delete holder;
}

inline Napi::External<CopyFunctionHolder> CreateExternalForCopyFunction(Napi::Env env, duckdb_copy_function copy_function) {
  return CreateExternal<CopyFunctionHolder>(env, CopyFunctionTypeTag, CreateCopyFunctionHolder(copy_function), FinalizeCopyFunctionHolder);
}

inline CopyFunctionHolder *GetCopyFunctionHolderFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<CopyFunctionHolder>(env, CopyFunctionTypeTag, value, "Invalid copy function argument");
}

inline duckdb_copy_function GetCopyFunctionFromExternal(Napi::Env env, Napi::Value value) {
  return GetCopyFunctionHolderFromExternal(env, value)->copy_function;
}

// Bind data and global state
//
// Both hold a user object through the reaper, so that the reference is destroyed
// on the JS thread whatever thread DuckDB tears the copy down on. The C API never
// copies either, so there is no copy callback to implement.

struct CopyFunctionInternalBindData {
  std::shared_ptr<ManagedObjectReference> user_bind_data_ref;

  void SetUserBindData(const std::shared_ptr<NapiRefReaper> &reaper, Napi::Object user_bind_data) {
    user_bind_data_ref = user_bind_data.IsUndefined() ? nullptr : MakeManagedObjectReference(reaper, user_bind_data);
  }
};

inline void DeleteCopyFunctionInternalBindData(CopyFunctionInternalBindData *internal_bind_data) {
  delete internal_bind_data;
}

struct CopyFunctionInternalGlobalState {
  std::shared_ptr<ManagedObjectReference> user_global_state_ref;

  void SetUserGlobalState(const std::shared_ptr<NapiRefReaper> &reaper, Napi::Object user_global_state) {
    user_global_state_ref = user_global_state.IsUndefined() ? nullptr : MakeManagedObjectReference(reaper, user_global_state);
  }
};

inline void DeleteCopyFunctionInternalGlobalState(CopyFunctionInternalGlobalState *internal_global_state) {
  delete internal_global_state;
}

// Entry points handed to DuckDB

inline CopyFunctionInternalExtraInfo *GetCopyFunctionInternalExtraInfoFromBindInfo(duckdb_copy_function_bind_info bind_info) {
  return reinterpret_cast<CopyFunctionInternalExtraInfo*>(duckdb_copy_function_bind_get_extra_info(bind_info));
}

inline CopyFunctionInternalExtraInfo *GetCopyFunctionInternalExtraInfoFromGlobalInitInfo(duckdb_copy_function_global_init_info init_info) {
  return reinterpret_cast<CopyFunctionInternalExtraInfo*>(duckdb_copy_function_global_init_get_extra_info(init_info));
}

inline CopyFunctionInternalExtraInfo *GetCopyFunctionInternalExtraInfoFromSinkInfo(duckdb_copy_function_sink_info sink_info) {
  return reinterpret_cast<CopyFunctionInternalExtraInfo*>(duckdb_copy_function_sink_get_extra_info(sink_info));
}

inline CopyFunctionInternalExtraInfo *GetCopyFunctionInternalExtraInfoFromFinalizeInfo(duckdb_copy_function_finalize_info finalize_info) {
  return reinterpret_cast<CopyFunctionInternalExtraInfo*>(duckdb_copy_function_finalize_get_extra_info(finalize_info));
}

inline void CopyFunctionBindFunction(duckdb_copy_function_bind_info info) {
  GetCopyFunctionInternalExtraInfoFromBindInfo(info)->bind_callback.Invoke(info);
}

inline void CopyFunctionGlobalInitFunction(duckdb_copy_function_global_init_info info) {
  GetCopyFunctionInternalExtraInfoFromGlobalInitInfo(info)->global_init_callback.Invoke(info);
}

inline void CopyFunctionSinkFunction(duckdb_copy_function_sink_info info, duckdb_data_chunk input) {
  GetCopyFunctionInternalExtraInfoFromSinkInfo(info)->sink_callback.Invoke({info, input});
}

inline void CopyFunctionFinalizeFunction(duckdb_copy_function_finalize_info info) {
  GetCopyFunctionInternalExtraInfoFromFinalizeInfo(info)->finalize_callback.Invoke(info);
}
