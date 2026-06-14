#pragma once

#include "conversion_helpers.h"
#include "externals.h"
#include "bindings_config.h"
#include <optional>
#include <string>

// Promise workers

class PromiseWorker : public Napi::AsyncWorker {

public:

  PromiseWorker(Napi::Env env) : AsyncWorker(env), deferred_(Napi::Promise::Deferred::New(env)) {
  }

  Napi::Promise Promise() {
    return deferred_.Promise();
  }

protected:

  virtual Napi::Value Result() = 0;

  void OnOK() override {
		deferred_.Resolve(Result());
	}

	void OnError(const Napi::Error &e) override {
		deferred_.Reject(e.Value());
	}

private:

	Napi::Promise::Deferred deferred_;

};

class GetOrCreateFromCacheWorker : public PromiseWorker {

public:

  GetOrCreateFromCacheWorker(Napi::Env env, Napi::Value instanceCacheValue, std::optional<std::string> path, Napi::Value configValue)
      : PromiseWorker(env),
      instance_cache_(GetInstanceCacheFromExternal(env, instanceCacheValue)),
      instanceCacheValueRef_(MakeValueRef(instanceCacheValue)),
      path_(path),
      config_(configValue.IsUndefined() ? nullptr : GetConfigFromExternal(env, configValue)),
      configValueRef_(MakeValueRef(configValue))
    {
    }

protected:

  void Execute() override {
    const char *path = nullptr;
    if (path_) {
      path = path_->c_str();
    }
    duckdb_config cfg = config_;
    // If no config was provided, create one with the default duckdb_api value.
    if (cfg == nullptr) {
      if (duckdb_create_config(&cfg)) {
        duckdb_destroy_config(&cfg);
        SetError("Failed to create config");
        return;
      }
      if (duckdb_set_config(cfg, "duckdb_api", DEFAULT_DUCKDB_API)) {
        SetError("Failed to set duckdb_api");
        return;
      }
    }
    char *error = nullptr;
    if (duckdb_get_or_create_from_cache(instance_cache_, path, &database_, cfg, &error)) {
      if (error != nullptr) {
        SetError(error);
        duckdb_free(error);
      } else {
        SetError("Failed to get or create from cache");
      }
    }
  }

  Napi::Value Result() override {
    return CreateExternalForDatabase(Env(), database_);
  }

private:

  duckdb_instance_cache instance_cache_;
  Napi::Reference<Napi::Value> instanceCacheValueRef_;
  std::optional<std::string> path_;
  duckdb_config config_;
  Napi::Reference<Napi::Value> configValueRef_;
  duckdb_database database_ = nullptr;

};

class OpenWorker : public PromiseWorker {

public:

  OpenWorker(Napi::Env env, std::optional<std::string> path, Napi::Value configValue)
    : PromiseWorker(env),
    path_(path),
    config_(configValue.IsUndefined() ? nullptr : GetConfigFromExternal(env, configValue)),
    configValueRef_(MakeValueRef(configValue))
  {
  }

protected:

  void Execute() override {
    const char *path = nullptr;
    if (path_) {
      path = path_->c_str();
    }
    duckdb_config cfg = config_;
    // If no config was provided, create one with the default duckdb_api value.
    if (cfg == nullptr) {
      if (duckdb_create_config(&cfg)) {
        duckdb_destroy_config(&cfg);
        SetError("Failed to create config");
        return;
      }
      if (duckdb_set_config(cfg, "duckdb_api", DEFAULT_DUCKDB_API)) {
        SetError("Failed to set duckdb_api");
        return;
      }
    }
    char *error = nullptr;
    if (duckdb_open_ext(path, &database_, cfg, &error)) {
      if (error != nullptr) {
        SetError(error);
        duckdb_free(error);
      } else {
        SetError("Failed to open");
      }
    }
  }

  Napi::Value Result() override {
    return CreateExternalForDatabase(Env(), database_);
  }

private:

  std::optional<std::string> path_;
  duckdb_config config_;
  Napi::Reference<Napi::Value> configValueRef_;
  duckdb_database database_ = nullptr;

};

class ConnectWorker : public PromiseWorker {

public:

  ConnectWorker(Napi::Env env, Napi::Value databaseValue)
    : PromiseWorker(env),
    database_(GetDatabaseFromExternal(env, databaseValue)),
    databaseValueRef_(MakeValueRef(databaseValue))
  {
  }

protected:

  void Execute() override {
    if (!database_) {
      SetError("Failed to connect: instance closed");
      return;
    }
    if (duckdb_connect(database_, &connection_)) {
      SetError("Failed to connect");
    }
  }

  Napi::Value Result() override {
    return CreateExternalForConnection(Env(), connection_);
  }

private:

  duckdb_database database_;
  Napi::Reference<Napi::Value> databaseValueRef_;
  duckdb_connection connection_ = nullptr;

};

class QueryWorker : public PromiseWorker {

public:

  QueryWorker(Napi::Env env, Napi::Value connectionValue, std::string query)
    : PromiseWorker(env),
    connection_(GetConnectionFromExternal(env, connectionValue)),
    connectionValueRef_(MakeValueRef(connectionValue)),
    query_(query)
  {
  }

protected:

  void Execute() override {
    if (!connection_) {
      SetError("Failed to query: connection disconnected");
      return;
    }
    result_ptr_ = reinterpret_cast<duckdb_result*>(duckdb_malloc(sizeof(duckdb_result)));
    result_ptr_->internal_data = nullptr;
    result_ptr_->deprecated_columns = nullptr;
    if (duckdb_query(connection_, query_.c_str(), result_ptr_)) {
      auto error = duckdb_result_error(result_ptr_);
      if (error) {
        SetError(error);
      } else {
        SetError("Failed to query");
      }
      duckdb_destroy_result(result_ptr_);
      duckdb_free(result_ptr_);
      result_ptr_ = nullptr;
    }
  }

  Napi::Value Result() override {
    return CreateExternalForResult(Env(), result_ptr_);
  }

private:

  duckdb_connection connection_;
  Napi::Reference<Napi::Value> connectionValueRef_;
  std::string query_;
  duckdb_result *result_ptr_ = nullptr;

};

class PrepareWorker : public PromiseWorker {

public:

  PrepareWorker(Napi::Env env, Napi::Value connectionValue, std::string query)
    : PromiseWorker(env),
    connection_(GetConnectionFromExternal(env, connectionValue)),
    connectionValueRef_(MakeValueRef(connectionValue)),
    query_(query)
  {
  }

protected:

  void Execute() override {
    if (!connection_) {
      SetError("Failed to prepare: connection disconnected");
      return;
    }
    if (duckdb_prepare(connection_, query_.c_str(), &prepared_statement_)) {
      if (prepared_statement_) {
        SetError(duckdb_prepare_error(prepared_statement_));
        duckdb_destroy_prepare(&prepared_statement_);
      } else {
        SetError("Failed to prepare");
      }
    }
  }

  Napi::Value Result() override {
    return CreateExternalForPreparedStatement(Env(), prepared_statement_);
  }

private:

  duckdb_connection connection_;
  Napi::Reference<Napi::Value> connectionValueRef_;
  std::string query_;
  duckdb_prepared_statement prepared_statement_ = nullptr;

};

class ExecutePreparedWorker : public PromiseWorker {

public:

  ExecutePreparedWorker(Napi::Env env, Napi::Value preparedStatementValue)
    : PromiseWorker(env),
    prepared_statement_(GetPreparedStatementFromExternal(env, preparedStatementValue)),
    preparedStatementValueRef_(MakeValueRef(preparedStatementValue))
  {
  }

protected:

  void Execute() override {
    result_ptr_ = reinterpret_cast<duckdb_result*>(duckdb_malloc(sizeof(duckdb_result)));
    result_ptr_->internal_data = nullptr;
    result_ptr_->deprecated_columns = nullptr;
    if (duckdb_execute_prepared(prepared_statement_, result_ptr_)) {
      auto error = duckdb_result_error(result_ptr_);
      if (error) {
        SetError(error);
      } else {
        SetError("Failed to execute prepared statement");
      }
      duckdb_destroy_result(result_ptr_);
      duckdb_free(result_ptr_);
      result_ptr_ = nullptr;
    }
  }

  Napi::Value Result() override {
    return CreateExternalForResult(Env(), result_ptr_);
  }

private:

  duckdb_prepared_statement prepared_statement_;
  Napi::Reference<Napi::Value> preparedStatementValueRef_;
  duckdb_result *result_ptr_ = nullptr;

};

class ExecutePreparedStreamingWorker : public PromiseWorker {

public:

  ExecutePreparedStreamingWorker(Napi::Env env, Napi::Value preparedStatementValue)
    : PromiseWorker(env),
    prepared_statement_(GetPreparedStatementFromExternal(env, preparedStatementValue)),
    preparedStatementValueRef_(MakeValueRef(preparedStatementValue))
  {
  }

protected:

  void Execute() override {
    result_ptr_ = reinterpret_cast<duckdb_result*>(duckdb_malloc(sizeof(duckdb_result)));
    result_ptr_->internal_data = nullptr;
    result_ptr_->deprecated_columns = nullptr;
    if (duckdb_execute_prepared_streaming(prepared_statement_, result_ptr_)) {
      auto error = duckdb_result_error(result_ptr_);
      if (error) {
        SetError(error);
      } else {
        SetError("Failed to execute prepared statement");
      }
      duckdb_destroy_result(result_ptr_);
      duckdb_free(result_ptr_);
      result_ptr_ = nullptr;
    }
  }

  Napi::Value Result() override {
    return CreateExternalForResult(Env(), result_ptr_);
  }

private:

  duckdb_prepared_statement prepared_statement_;
  Napi::Reference<Napi::Value> preparedStatementValueRef_;
  duckdb_result *result_ptr_;

};

class ExtractStatementsWorker : public PromiseWorker {

public:

  ExtractStatementsWorker(Napi::Env env, Napi::Value connectionValue, std::string query)
    : PromiseWorker(env),
    connection_(GetConnectionFromExternal(env, connectionValue)),
    connectionValueRef_(MakeValueRef(connectionValue)),
    query_(query)
  {
  }

protected:

  void Execute() override {
    if (!connection_) {
      SetError("Failed to extract statements: connection disconnected");
      return;
    }
    statement_count_ = duckdb_extract_statements(connection_, query_.c_str(), &extracted_statements_);
  }

  Napi::Value Result() override {
    auto extracted_statements_and_count_obj = Napi::Object::New(Env());
    extracted_statements_and_count_obj.Set("extracted_statements", CreateExternalForExtractedStatements(Env(), extracted_statements_));
    extracted_statements_and_count_obj.Set("statement_count", Napi::Number::New(Env(), statement_count_));
    return extracted_statements_and_count_obj;
  }

private:

  duckdb_connection connection_;
  Napi::Reference<Napi::Value> connectionValueRef_;
  std::string query_;
  duckdb_extracted_statements extracted_statements_ = nullptr;
  idx_t statement_count_ = 0;

};

class PrepareExtractedStatementWorker : public PromiseWorker {

public:

  PrepareExtractedStatementWorker(Napi::Env env, Napi::Value connectionValue, Napi::Value extractedStatementsValue, idx_t index)
    : PromiseWorker(env),
    connection_(GetConnectionFromExternal(env, connectionValue)),
    connectionValueRef_(MakeValueRef(connectionValue)),
    extracted_statements_(GetExtractedStatementsFromExternal(env, extractedStatementsValue)),
    extractedStatementsValueRef_(MakeValueRef(extractedStatementsValue)),
    index_(index)
  {
  }

protected:

  void Execute() override {
    if (!connection_) {
      SetError("Failed to prepare extracted statement: connection disconnected");
      return;
    }
    if (duckdb_prepare_extracted_statement(connection_, extracted_statements_, index_, &prepared_statement_)) {
      if (prepared_statement_) {
        SetError(duckdb_prepare_error(prepared_statement_));
        duckdb_destroy_prepare(&prepared_statement_);
      } else {
        SetError("Failed to prepare extracted statement");
      }
    }
  }

  Napi::Value Result() override {
    return CreateExternalForPreparedStatement(Env(), prepared_statement_);
  }

private:

  duckdb_connection connection_;
  Napi::Reference<Napi::Value> connectionValueRef_;
  duckdb_extracted_statements extracted_statements_;
  Napi::Reference<Napi::Value> extractedStatementsValueRef_;
  idx_t index_;
  duckdb_prepared_statement prepared_statement_ = nullptr;

};

class ExecutePendingWorker : public PromiseWorker {

public:

  ExecutePendingWorker(Napi::Env env, Napi::Value pendingResultValue)
    : PromiseWorker(env),
    pending_result_(GetPendingResultFromExternal(env, pendingResultValue)),
    pendingResultValueRef_(MakeValueRef(pendingResultValue))
  {
  }

protected:

  void Execute() override {
    result_ptr_ = reinterpret_cast<duckdb_result*>(duckdb_malloc(sizeof(duckdb_result)));
    result_ptr_->internal_data = nullptr;
    result_ptr_->deprecated_columns = nullptr;
    if (duckdb_execute_pending(pending_result_, result_ptr_)) {
      auto error = duckdb_result_error(result_ptr_);
      if (error) {
        SetError(error);
      } else {
        SetError("Failed to execute pending result");
      }
      duckdb_destroy_result(result_ptr_);
      duckdb_free(result_ptr_);
      result_ptr_ = nullptr;
    }
  }

  Napi::Value Result() override {
    return CreateExternalForResult(Env(), result_ptr_);
  }

private:

  duckdb_pending_result pending_result_;
  Napi::Reference<Napi::Value> pendingResultValueRef_;
  duckdb_result *result_ptr_ = nullptr;

};

class FetchWorker : public PromiseWorker {

public:

  FetchWorker(Napi::Env env, Napi::Value resultValue)
    : PromiseWorker(env),
    result_ptr_(GetResultFromExternal(env, resultValue)),
    resultValueRef_(MakeValueRef(resultValue))
  {
  }

protected:

  void Execute() override {
    data_chunk_ = duckdb_fetch_chunk(*result_ptr_);
  }

  Napi::Value Result() override {
    return CreateExternalForDataChunk(Env(), data_chunk_);
  }

private:

  duckdb_result *result_ptr_;
  Napi::Reference<Napi::Value> resultValueRef_;
  duckdb_data_chunk data_chunk_ = nullptr;

};
