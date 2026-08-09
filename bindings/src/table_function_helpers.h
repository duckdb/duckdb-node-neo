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

// Extra info

struct TableFunctionInternalExtraInfo {
  std::shared_ptr<ManagedObjectReference> user_extra_info_ref;

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

  TableFunctionInternalExtraInfo *EnsureInternalExtraInfo() {
    if (!internal_extra_info) {
      internal_extra_info = new TableFunctionInternalExtraInfo();
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
