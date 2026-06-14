#pragma once

#include "externals.h"
#include <condition_variable>
#include <memory>
#include <mutex>

// Scalar functions

struct ScalarFunctionInternalBindData {
  std::unique_ptr<Napi::ObjectReference> user_bind_data_ref;

  void SetUserBindData(Napi::Object user_bind_data) {
    user_bind_data_ref = std::make_unique<Napi::ObjectReference>(user_bind_data.IsUndefined() ? Napi::ObjectReference() : Napi::Persistent(user_bind_data));
  }
};

inline void DeleteScalarFunctionInternalBindData(ScalarFunctionInternalBindData *internal_bind_data) {
  delete internal_bind_data;
}

inline ScalarFunctionInternalBindData *CopyScalarFunctionInternalBindData(ScalarFunctionInternalBindData *internal_bind_data) {
  if (!internal_bind_data) {
    return nullptr;
  }
  auto new_internal_bind_data = new ScalarFunctionInternalBindData();
  if (internal_bind_data->user_bind_data_ref && !internal_bind_data->user_bind_data_ref->IsEmpty()) {
    new_internal_bind_data->SetUserBindData(internal_bind_data->user_bind_data_ref->Value());
  }
  return new_internal_bind_data;
}

struct ScalarFunctionBindTSFNData {
  duckdb_bind_info info;
  std::condition_variable *cv;
  std::mutex *cv_mutex;
  bool done;
};

inline void ScalarFunctionBindTSFNCallback(Napi::Env env, Napi::Function callback, ScalarFunctionBindTSFNContext *context, ScalarFunctionBindTSFNData *data) {
  if (env != nullptr) {
    if (callback != nullptr) {
      try {
        callback.Call(
          env.Undefined(),
          {
            CreateExternalForBindInfoWithoutFinalizer(env, data->info)
          }
        );
      } catch (const Napi::Error &err) {
        duckdb_scalar_function_bind_set_error(data->info, err.Message().c_str());
      }
    }
  }
  {
    std::lock_guard lk(*data->cv_mutex);
    data->done = true;
    data->cv->notify_one();
  } 
}

inline ScalarFunctionInternalExtraInfo *GetScalarFunctionInternalExtraInfoFromBindInfo(duckdb_bind_info bind_info) {
  return reinterpret_cast<ScalarFunctionInternalExtraInfo*>(duckdb_scalar_function_bind_get_extra_info(bind_info));
}

inline void ScalarFunctionBindFunction(duckdb_bind_info info) {
  auto internal_extra_info = GetScalarFunctionInternalExtraInfoFromBindInfo(info);
  auto data = reinterpret_cast<ScalarFunctionBindTSFNData*>(duckdb_malloc(sizeof(ScalarFunctionBindTSFNData)));
  data->info = info;
  data->cv = new std::condition_variable;
  data->cv_mutex = new std::mutex;
  data->done = false;
  // The "blocking" part of this call only waits for queue space, not for the JS function call to complete.
  // Since we specify no limit to the queue space, it in fact never blocks.
  auto status = internal_extra_info->bind_tsfn->BlockingCall(data);
  if (status == napi_ok) {
    // Wait for the JS function call to complete.
    std::unique_lock<std::mutex> lk(*data->cv_mutex);
    data->cv->wait(lk, [&]{ return data->done; });
  } else {
    duckdb_scalar_function_bind_set_error(info, "BlockingCall returned not ok");
  }
  delete data->cv;
  delete data->cv_mutex;
  duckdb_free(data);
}

struct ScalarFunctionMainTSFNData {
  duckdb_function_info info;
  duckdb_data_chunk input;
  duckdb_vector output;
  std::condition_variable *cv;
  std::mutex *cv_mutex;
  bool done;
};

inline void ScalarFunctionMainTSFNCallback(Napi::Env env, Napi::Function callback, ScalarFunctionMainTSFNContext *context, ScalarFunctionMainTSFNData *data) {
  if (env != nullptr) {
    if (callback != nullptr) {
      try {
        callback.Call(
          env.Undefined(),
          {
            CreateExternalForFunctionInfoWithoutFinalizer(env, data->info),
            CreateExternalForDataChunkWithoutFinalizer(env, data->input),
            CreateExternalForVectorWithoutFinalizer(env, data->output)
          }
        );
      } catch (const Napi::Error &err) {
        duckdb_scalar_function_set_error(data->info, err.Message().c_str());
      }
    }
  }
  {
    std::lock_guard lk(*data->cv_mutex);
    data->done = true;
    data->cv->notify_one();
  }
}

inline ScalarFunctionInternalExtraInfo *GetScalarFunctionInternalExtraInfoFromFunctionInfo(duckdb_function_info function_info) {
  return reinterpret_cast<ScalarFunctionInternalExtraInfo*>(duckdb_scalar_function_get_extra_info(function_info));
}

inline void ScalarFunctionMainFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
  auto internal_extra_info = GetScalarFunctionInternalExtraInfoFromFunctionInfo(info);
  auto data = reinterpret_cast<ScalarFunctionMainTSFNData*>(duckdb_malloc(sizeof(ScalarFunctionMainTSFNData)));
  data->info = info;
  data->input = input;
  data->output = output;
  data->cv = new std::condition_variable;
  data->cv_mutex = new std::mutex;
  data->done = false;
  // The "blocking" part of this call only waits for queue space, not for the JS function call to complete.
  // Since we specify no limit to the queue space, it in fact never blocks.
  auto status = internal_extra_info->main_tsfn->BlockingCall(data);
  if (status == napi_ok) {
    // Wait for the JS function call to complete.
    std::unique_lock<std::mutex> lk(*data->cv_mutex);
    data->cv->wait(lk, [&]{ return data->done; });
  } else {
    duckdb_scalar_function_set_error(info, "BlockingCall returned not ok");
  }
  delete data->cv;
  delete data->cv_mutex;
  duckdb_free(data);
}
