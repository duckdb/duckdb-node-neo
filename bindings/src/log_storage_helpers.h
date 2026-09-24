#pragma once

#include "conversion_helpers.h"
#include "duckdb_thread_callback.h"
#include "napi_ref_reaper.h"
#include "type_tags.h"
#include <atomic>
#include <memory>
#include <string>

struct LogStorageWriteLogEntryPayload {
  std::shared_ptr<ManagedObjectReference> user_extra_data_ref;
  duckdb_timestamp timestamp;
  std::string level;
  std::string log_type;
  std::string message;
};

struct LogStorageWriteLogEntryCallbackTraits {
  // DuckDBThreadCallback's payload slot is raw storage, so keep the value passed
  // through it trivial. Invoke below owns the copied strings until dispatch has
  // completed.
  using Payload = LogStorageWriteLogEntryPayload *;

  static const char *ResourceName() {
    return "LogStorageWriteLogEntry";
  }

  static void Call(Napi::Env env, Napi::Function callback, const Payload &payload) {
    Napi::Value user_extra_data = env.Undefined();
    if (payload->user_extra_data_ref) {
      user_extra_data = payload->user_extra_data_ref->ref.Value();
    }
    callback.Call(
      env.Undefined(),
      {
        user_extra_data,
        MakeTimestampObject(env, payload->timestamp),
        Napi::String::New(env, payload->level),
        Napi::String::New(env, payload->log_type),
        Napi::String::New(env, payload->message)
      }
    );
  }

  // The logging C API has no error channel. Exceptions from a JS logger must
  // not escape across the C callback boundary.
  static void SetError(const Payload &, const char *) {}
};

struct LogStorageInternalExtraData {
  std::atomic<size_t> reference_count {1};
  std::shared_ptr<NapiRefReaper> ref_reaper;
  DuckDBThreadCallback<LogStorageWriteLogEntryCallbackTraits> write_log_entry_callback;
  std::shared_ptr<ManagedObjectReference> write_log_entry_ref;
  std::shared_ptr<ManagedObjectReference> user_extra_data_ref;

  explicit LogStorageInternalExtraData(const std::shared_ptr<NapiRefReaper> &ref_reaper_in)
    : ref_reaper(ref_reaper_in), write_log_entry_callback(ref_reaper_in) {}

  void AddReference() {
    reference_count.fetch_add(1, std::memory_order_relaxed);
  }

  void Release() {
    if (reference_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      delete this;
    }
  }

  void SetWriteLogEntry(Napi::Env env, Napi::Function callback) {
    write_log_entry_callback.Set(env, callback);
    write_log_entry_ref = MakeManagedObjectReference(ref_reaper, callback);
  }

  void SetUserExtraData(Napi::Value user_extra_data) {
    user_extra_data_ref = user_extra_data.IsUndefined()
      ? nullptr
      : MakeManagedObjectReference(ref_reaper, user_extra_data.As<Napi::Object>());
  }

  void Invoke(LogStorageWriteLogEntryPayload payload) {
    if (ref_reaper->OnJSThread()) {
      if (!write_log_entry_ref) {
        return;
      }
      try {
        auto callback = write_log_entry_ref->ref.Value().As<Napi::Function>();
        auto payload_pointer = &payload;
        LogStorageWriteLogEntryCallbackTraits::Call(callback.Env(), callback, payload_pointer);
      } catch (const Napi::Error &) {
        // The C callback has nowhere to report an error.
      }
      return;
    }
    write_log_entry_callback.Invoke(&payload);
  }
};

inline void DeleteLogStorageInternalExtraData(void *extra_data) {
  reinterpret_cast<LogStorageInternalExtraData *>(extra_data)->Release();
}

inline void LogStorageWriteLogEntry(void *extra_data, duckdb_timestamp *timestamp, const char *level,
                                    const char *log_type, const char *message) {
  auto internal_extra_data = reinterpret_cast<LogStorageInternalExtraData *>(extra_data);
  internal_extra_data->Invoke({
    internal_extra_data->user_extra_data_ref,
    *timestamp,
    level,
    log_type,
    message
  });
}

struct LogStorageHolder {
  duckdb_log_storage log_storage;
  LogStorageInternalExtraData *internal_extra_data = nullptr;
  bool has_name = false;
  bool has_write_log_entry = false;

  explicit LogStorageHolder(duckdb_log_storage log_storage_in): log_storage(log_storage_in) {}

  ~LogStorageHolder() {
    duckdb_destroy_log_storage(&log_storage);
  }

  LogStorageInternalExtraData *EnsureInternalExtraData(const std::shared_ptr<NapiRefReaper> &ref_reaper) {
    if (!internal_extra_data) {
      internal_extra_data = new LogStorageInternalExtraData(ref_reaper);
      duckdb_log_storage_set_extra_data(log_storage, internal_extra_data, DeleteLogStorageInternalExtraData);
    }
    return internal_extra_data;
  }

  bool IsConfigured() const {
    return internal_extra_data && has_name && has_write_log_entry;
  }

  void PrepareRegistration() {
    internal_extra_data->AddReference();
  }

  void CompleteRegistration() {
    // DuckDB clears its configuration wrapper's pointer after success without
    // invoking the deleter. Release that wrapper reference here; the registered
    // storage retains the reference added by PrepareRegistration.
    internal_extra_data->Release();
    internal_extra_data = nullptr;
  }
};

inline void FinalizeLogStorageHolder(Napi::BasicEnv, LogStorageHolder *holder) {
  delete holder;
}

inline Napi::External<LogStorageHolder> CreateExternalForLogStorage(Napi::Env env, duckdb_log_storage log_storage) {
  return CreateExternal<LogStorageHolder>(
    env,
    LogStorageTypeTag,
    new LogStorageHolder(log_storage),
    FinalizeLogStorageHolder
  );
}

inline LogStorageHolder *GetLogStorageHolderFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<LogStorageHolder>(env, LogStorageTypeTag, value, "Invalid log storage argument");
}
