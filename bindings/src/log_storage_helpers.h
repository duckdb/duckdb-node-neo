#pragma once

#include "conversion_helpers.h"
#include "napi_ref_reaper.h"
#include "type_tags.h"
#include <atomic>
#include <memory>
#include <string>

struct LogStorageWriteLogEntryPayload {
  std::shared_ptr<ManagedObjectReference> callback_ref;
  std::shared_ptr<ManagedObjectReference> user_extra_data_ref;
  duckdb_timestamp timestamp;
  std::string level;
  std::string log_type;
  std::string message;
};

// The log manager holds its mutex during the native callback. JS must always
// run later, even when the entry originated on the JS thread.
inline void DispatchLogStorageEntry(Napi::Env env, Napi::Function callback, std::nullptr_t *,
                                    LogStorageWriteLogEntryPayload *entry) {
  std::unique_ptr<LogStorageWriteLogEntryPayload> payload(entry);
  // Node drains queued payloads with a null env during teardown.
  if (env == nullptr || callback == nullptr) {
    return;
  }
  try {
    callback.Call(env.Undefined(), {
      payload->user_extra_data_ref ? payload->user_extra_data_ref->ref.Value() : env.Undefined(),
      MakeTimestampObject(env, payload->timestamp),
      Napi::String::New(env, payload->level),
      Napi::String::New(env, payload->log_type),
      Napi::String::New(env, payload->message)
    });
  } catch (const Napi::Error &) {
    // Preserve the V1 logger's swallowed-exception behavior: there is no C
    // error channel, and a failed logger must not poison later JS delivery.
    if (env.IsExceptionPending()) {
      env.GetAndClearPendingException();
    }
  }
}

struct LogStorageInternalExtraData {
  std::atomic<size_t> reference_count {1};
  std::shared_ptr<NapiRefReaper> ref_reaper;
  using TSFN = Napi::TypedThreadSafeFunction<std::nullptr_t, LogStorageWriteLogEntryPayload,
                                             DispatchLogStorageEntry>;
  std::unique_ptr<TSFN> write_log_entry_callback;
  std::shared_ptr<ManagedObjectReference> write_log_entry_ref;
  std::shared_ptr<ManagedObjectReference> user_extra_data_ref;

  explicit LogStorageInternalExtraData(const std::shared_ptr<NapiRefReaper> &ref_reaper_in)
    : ref_reaper(ref_reaper_in) {}

  ~LogStorageInternalExtraData() {
    ReleaseCallback();
  }

  void ReleaseCallback() {
    if (write_log_entry_callback && ref_reaper->EnvIsAlive()) {
      write_log_entry_callback->Release();
    }
    write_log_entry_callback.reset();
  }

  void AddReference() {
    reference_count.fetch_add(1, std::memory_order_relaxed);
  }

  void Release() {
    if (reference_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      delete this;
    }
  }

  void SetWriteLogEntry(Napi::Env env, Napi::Function callback) {
    ReleaseCallback();
    write_log_entry_callback = std::make_unique<TSFN>(
      TSFN::New(env, callback, "LogStorageWriteLogEntry", 0, 1));
    write_log_entry_callback->Unref(env);
    write_log_entry_ref = MakeManagedObjectReference(ref_reaper, callback);
  }

  void SetUserExtraData(Napi::Value user_extra_data) {
    user_extra_data_ref = user_extra_data.IsUndefined()
      ? nullptr
      : MakeManagedObjectReference(ref_reaper, user_extra_data.As<Napi::Object>());
  }

  void Invoke(LogStorageWriteLogEntryPayload payload) {
    if (!write_log_entry_callback) {
      return;
    }
    auto entry = std::make_unique<LogStorageWriteLogEntryPayload>(std::move(payload));
    // The queue owns the copied strings and managed references until delivery
    // or teardown. A queue failure leaves ownership here for immediate cleanup.
    if (write_log_entry_callback->NonBlockingCall(entry.get()) == napi_ok) {
      entry.release();
    }
  }
};

inline void DeleteLogStorageInternalExtraData(void *extra_data) {
  reinterpret_cast<LogStorageInternalExtraData *>(extra_data)->Release();
}

inline void LogStorageWriteLogEntry(void *extra_data, duckdb_timestamp *timestamp, const char *level,
                                    const char *log_type, const char *message) {
  auto internal_extra_data = reinterpret_cast<LogStorageInternalExtraData *>(extra_data);
  internal_extra_data->Invoke({
    internal_extra_data->write_log_entry_ref,
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
  bool registered = false;

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

  bool IsRegistered() const {
    return registered;
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
    registered = true;
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
