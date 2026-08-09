#pragma once

#include "napi_setup.h"
#include "napi_ref_reaper.h"
#include <condition_variable>
#include <memory>
#include <mutex>

// A JS function that DuckDB invokes from its own threads.
//
// DuckDB calls the bind and main callbacks of a scalar function -- and, for other
// function families, init and local init as well -- on worker threads. JS can
// only run on the JS thread, so each call is handed to a thread-safe function and
// the calling DuckDB thread blocks until it has run.
//
// Three lifetime rules are encoded here. Each of them was a bug before it was a
// rule, and each fails silently until it doesn't, so new function families should
// use this rather than hand-rolling the same sequence:
//
//  1. Thread-safe functions are created referenced, and a referenced one holds the
//     event loop open. One that outlives a query would keep the process alive, so
//     it is unreferenced immediately. Nothing is lost by doing so: a call in
//     flight is always inside a query, and that query's async worker holds the
//     loop open on its own.
//  2. Node destroys thread-safe functions during env teardown, before it runs
//     finalizers. Releasing one from a finalizer after that point is a
//     use-after-free, so the release is skipped once the env is gone. Nothing
//     leaks by skipping it, since Node has already destroyed them.
//  3. Replacing a callback releases the one it replaces, so repeatedly setting a
//     callback does not accumulate thread-safe functions.
//
// Traits supplies the per-callback details:
//   using Payload = ...;                  // arguments for one call; copyable
//   static const char *ResourceName();
//   static void Call(Napi::Env, Napi::Function, const Payload &);
//   static void SetError(const Payload &, const char *message);

// One in-flight call. Lives on the calling DuckDB thread's stack: Invoke does not
// return until the dispatch below has signalled, including when it is draining.
template <typename Traits>
struct DuckDBThreadCallbackCall {
  typename Traits::Payload payload;
  std::condition_variable cv;
  std::mutex mutex;
  bool done = false;
};

template <typename Traits>
void DuckDBThreadCallbackDispatch(Napi::Env env, Napi::Function callback, std::nullptr_t *,
                                  DuckDBThreadCallbackCall<Traits> *call) {
  // env is null when the thread-safe function is draining during teardown. The
  // JS function cannot run then, but the waiting DuckDB thread must still be
  // released, so the signalling below is unconditional.
  if (env != nullptr && callback != nullptr) {
    try {
      Traits::Call(env, callback, call->payload);
    } catch (const Napi::Error &error) {
      Traits::SetError(call->payload, error.Message().c_str());
    }
  }
  {
    std::lock_guard<std::mutex> lock(call->mutex);
    call->done = true;
  }
  call->cv.notify_one();
}

template <typename Traits>
class DuckDBThreadCallback {

public:

  using TSFN = Napi::TypedThreadSafeFunction<std::nullptr_t, DuckDBThreadCallbackCall<Traits>,
                                             DuckDBThreadCallbackDispatch<Traits>>;

  explicit DuckDBThreadCallback(std::shared_ptr<NapiRefReaper> env_state_in)
    : env_state(std::move(env_state_in)) {}

  ~DuckDBThreadCallback() {
    Release();
  }

  DuckDBThreadCallback(const DuckDBThreadCallback &) = delete;
  DuckDBThreadCallback &operator=(const DuckDBThreadCallback &) = delete;

  bool IsSet() const {
    return bool(tsfn);
  }

  // Called on the JS thread.
  void Set(Napi::Env env, Napi::Function func) {
    Release();
    tsfn = std::make_unique<TSFN>(TSFN::New(env, func, Traits::ResourceName(), 0, 1));
    tsfn->Unref(env); // Rule 1.
  }

  // Called on a DuckDB thread. Blocks until the JS function has run.
  void Invoke(typename Traits::Payload payload) {
    if (!tsfn) {
      return;
    }
    DuckDBThreadCallbackCall<Traits> call;
    call.payload = payload;
    // The "blocking" part of BlockingCall only waits for queue space, and the
    // queue is unlimited, so it never actually blocks. Waiting for the JS
    // function to run is the wait below.
    if (tsfn->BlockingCall(&call) != napi_ok) {
      Traits::SetError(payload, "BlockingCall returned not ok");
      return;
    }
    std::unique_lock<std::mutex> lock(call.mutex);
    call.cv.wait(lock, [&call] { return call.done; });
  }

private:

  void Release() {
    // Rule 2: after the env has begun tearing down, Node has already destroyed
    // this thread-safe function.
    if (tsfn && env_state && env_state->EnvIsAlive()) {
      tsfn->Release();
    }
    tsfn.reset();
  }

  std::shared_ptr<NapiRefReaper> env_state;
  std::unique_ptr<TSFN> tsfn;

};
