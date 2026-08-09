#pragma once

#include "napi_setup.h"
#include <atomic>
#include <memory>
#include <mutex>
#include <thread>

#ifdef DUCKDB_NODE_INSTRUMENT_NAPI_REFS
#include <cstdio>
#include <cstdlib>
#endif

// N-API reference lifetime management
//
// User-supplied objects (scalar function extra info & bind data, and the
// equivalents for other function families) are held across the boundary as
// Napi::ObjectReferences, but the structs holding them are owned by DuckDB and
// destroyed by DuckDB's delete callbacks. Those callbacks run on whatever thread
// tears down the plan, which is routinely a DuckDB worker thread, not the JS
// thread.
//
// Destroying a Napi::ObjectReference calls napi_delete_reference, which mutates
// V8's global handle table. N-API requires every call except the thread-safe
// function family to happen on the thread owning the napi_env, and Node does not
// lock the isolate, so doing this from a worker thread is a data race.
//
// NapiRefReaper resolves that: references are always destroyed on the JS thread,
// either inline (when the delete callback already runs there) or by handing them
// to a thread-safe function that does the deletion on the JS thread.

// A Napi::ObjectReference that must only ever be destroyed on the JS thread.
//
// Instances are always constructed on the JS thread, so under
// DUCKDB_NODE_INSTRUMENT_NAPI_REFS the construction thread doubles as the
// expected destruction thread, and any mismatch aborts with a usable stack.
struct ManagedObjectReference {
  Napi::ObjectReference ref;

  explicit ManagedObjectReference(Napi::ObjectReference &&ref_in) : ref(std::move(ref_in)) {}

  ~ManagedObjectReference() {
#ifdef DUCKDB_NODE_INSTRUMENT_NAPI_REFS
    if (std::this_thread::get_id() != created_on) {
      fprintf(stderr, "FATAL: Napi::ObjectReference destroyed off the JS thread.\n");
      fflush(stderr);
      abort();
    }
#endif
  }

  ManagedObjectReference(const ManagedObjectReference &) = delete;
  ManagedObjectReference &operator=(const ManagedObjectReference &) = delete;

#ifdef DUCKDB_NODE_INSTRUMENT_NAPI_REFS
private:
  std::thread::id created_on = std::this_thread::get_id();
#endif
};

// Destroys ManagedObjectReferences on the JS thread, from any calling thread.
//
// One instance is owned by the addon (and therefore scoped to a napi_env, which
// matters under worker_threads, where the addon is instantiated per env). It is
// held by shared_ptr, and every managed reference's deleter captures that
// shared_ptr, so the reaper always outlives the references that depend on it.
class NapiRefReaper {

public:

  explicit NapiRefReaper(Napi::Env env) : js_thread_id(std::this_thread::get_id()), alive(true) {
    tsfn = Napi::ThreadSafeFunction::New(
      env,
      Napi::Function::New(env, [](const Napi::CallbackInfo&) {}), // never called; the work is in the call's own callback
      "DuckDBNapiRefReaper",
      0, // unlimited queue: releasing a reference must never block a DuckDB thread
      1  // initial thread count
    );
    // Reaping references should not by itself keep the process alive.
    tsfn.Unref(env);
  }

  bool OnJSThread() const {
    return std::this_thread::get_id() == js_thread_id;
  }

  // Called on the JS thread when the addon is torn down.
  void Shutdown() {
    std::lock_guard<std::mutex> lock(mutex);
    if (!alive) {
      return;
    }
    alive = false;
    tsfn.Release();
  }

  // Takes ownership of the reference. Safe to call from any thread.
  void Destroy(ManagedObjectReference *managed_ref) {
    if (!managed_ref) {
      return;
    }
    if (OnJSThread()) {
      // Shutdown also runs on the JS thread, so it cannot be racing with this.
      // It can already have run, though, in which case the env is going away and
      // the reference must be leaked rather than deleted.
      if (alive) {
        delete managed_ref;
      } else {
        LeakReference();
      }
      return;
    }
    // Holding the mutex across the check and the call is what stands in for the
    // per-thread Acquire/Release that N-API's thread-safe function contract
    // otherwise expects: it keeps Shutdown's Release from dropping the last
    // thread count while a call is in flight. Callers must not move the call
    // out of the lock.
    std::lock_guard<std::mutex> lock(mutex);
    if (!alive) {
      LeakReference();
      return;
    }
    auto status = tsfn.NonBlockingCall([managed_ref](Napi::Env, Napi::Function) { delete managed_ref; });
    if (status != napi_ok) {
      // A failed call means the thread-safe function is closing. Node closes it
      // from its own env cleanup hook, independently of Shutdown, and N-API
      // forbids using it afterwards because it may already have been freed.
      // Mark the reaper dead so no other thread calls into it.
      alive = false;
      LeakReference();
    }
  }

private:

  // The reference can no longer be deleted safely from any thread, so leaking it
  // is the only correct option. This happens only once the env is going away,
  // where the underlying JS object is already unreachable.
  static void LeakReference() {
#ifdef DUCKDB_NODE_INSTRUMENT_NAPI_REFS
    fprintf(stderr, "INFO: leaked a Napi::ObjectReference during teardown instead of destroying it.\n");
    fflush(stderr);
#endif
  }

  std::thread::id js_thread_id;
  Napi::ThreadSafeFunction tsfn;
  std::mutex mutex;
  // Written under the mutex, but read without it on the JS thread fast path.
  std::atomic<bool> alive;

};

// Creates a reference to the given object whose destruction is routed through
// the reaper. The deleter holds the reaper alive for as long as any reference
// created from it survives.
//
// The resulting reference is never empty, so holders can treat a null
// shared_ptr as "no user object" without also checking IsEmpty().
inline std::shared_ptr<ManagedObjectReference> MakeManagedObjectReference(const std::shared_ptr<NapiRefReaper> &reaper, Napi::Object object) {
  return std::shared_ptr<ManagedObjectReference>(
    new ManagedObjectReference(Napi::Persistent(object)),
    [reaper](ManagedObjectReference *managed_ref) { reaper->Destroy(managed_ref); }
  );
}
