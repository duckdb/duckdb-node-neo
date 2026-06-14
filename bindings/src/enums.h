#pragma once

#include "napi_setup.h"
#include "duckdb.h"

// Enums

inline void DefineEnumMember(Napi::Object enumObj, const char *key, uint32_t value) {
  enumObj.Set(key, value);
  enumObj.Set(value, key);
}

inline Napi::Object CreatePendingStateEnum(Napi::Env env) {
  auto pendingStateEnum = Napi::Object::New(env);
  DefineEnumMember(pendingStateEnum, "RESULT_READY", 0);
  DefineEnumMember(pendingStateEnum, "RESULT_NOT_READY", 1);
  DefineEnumMember(pendingStateEnum, "ERROR", 2);
  DefineEnumMember(pendingStateEnum, "NO_TASKS_AVAILABLE", 3);
  return pendingStateEnum;
}

inline Napi::Object CreateResultTypeEnum(Napi::Env env) {
  auto resultTypeEnum = Napi::Object::New(env);
  DefineEnumMember(resultTypeEnum, "INVALID", 0);
  DefineEnumMember(resultTypeEnum, "CHANGED_ROWS", 1);
  DefineEnumMember(resultTypeEnum, "NOTHING", 2);
  DefineEnumMember(resultTypeEnum, "QUERY_RESULT", 3);
  return resultTypeEnum;
}

inline Napi::Object CreateStatementTypeEnum(Napi::Env env) {
  auto statementTypeEnum = Napi::Object::New(env);
  DefineEnumMember(statementTypeEnum, "INVALID", 0);
	DefineEnumMember(statementTypeEnum, "SELECT", 1);
	DefineEnumMember(statementTypeEnum, "INSERT", 2);
	DefineEnumMember(statementTypeEnum, "UPDATE", 3);
	DefineEnumMember(statementTypeEnum, "EXPLAIN", 4);
	DefineEnumMember(statementTypeEnum, "DELETE", 5);
	DefineEnumMember(statementTypeEnum, "PREPARE", 6);
	DefineEnumMember(statementTypeEnum, "CREATE", 7);
	DefineEnumMember(statementTypeEnum, "EXECUTE", 8);
	DefineEnumMember(statementTypeEnum, "ALTER", 9);
	DefineEnumMember(statementTypeEnum, "TRANSACTION", 10);
	DefineEnumMember(statementTypeEnum, "COPY", 11);
	DefineEnumMember(statementTypeEnum, "ANALYZE", 12);
	DefineEnumMember(statementTypeEnum, "VARIABLE_SET", 13);
	DefineEnumMember(statementTypeEnum, "CREATE_FUNC", 14);
	DefineEnumMember(statementTypeEnum, "DROP", 15);
	DefineEnumMember(statementTypeEnum, "EXPORT", 16);
	DefineEnumMember(statementTypeEnum, "PRAGMA", 17);
	DefineEnumMember(statementTypeEnum, "VACUUM", 18);
	DefineEnumMember(statementTypeEnum, "CALL", 19);
	DefineEnumMember(statementTypeEnum, "SET", 20);
	DefineEnumMember(statementTypeEnum, "LOAD", 21);
	DefineEnumMember(statementTypeEnum, "RELATION", 22);
	DefineEnumMember(statementTypeEnum, "EXTENSION", 23);
	DefineEnumMember(statementTypeEnum, "LOGICAL_PLAN", 24);
	DefineEnumMember(statementTypeEnum, "ATTACH", 25);
	DefineEnumMember(statementTypeEnum, "DETACH", 26);
	DefineEnumMember(statementTypeEnum, "MULTI", 27);
  return statementTypeEnum;
}

inline Napi::Object CreateTypeEnum(Napi::Env env) {
  auto typeEnum = Napi::Object::New(env);
  DefineEnumMember(typeEnum, "INVALID", 0);
	DefineEnumMember(typeEnum, "BOOLEAN", 1);
	DefineEnumMember(typeEnum, "TINYINT", 2);
	DefineEnumMember(typeEnum, "SMALLINT", 3);
	DefineEnumMember(typeEnum, "INTEGER", 4);
	DefineEnumMember(typeEnum, "BIGINT", 5);
	DefineEnumMember(typeEnum, "UTINYINT", 6);
	DefineEnumMember(typeEnum, "USMALLINT", 7);
	DefineEnumMember(typeEnum, "UINTEGER", 8);
	DefineEnumMember(typeEnum, "UBIGINT", 9);
	DefineEnumMember(typeEnum, "FLOAT", 10);
	DefineEnumMember(typeEnum, "DOUBLE", 11);
	DefineEnumMember(typeEnum, "TIMESTAMP", 12);
	DefineEnumMember(typeEnum, "DATE", 13);
	DefineEnumMember(typeEnum, "TIME", 14);
	DefineEnumMember(typeEnum, "INTERVAL", 15);
	DefineEnumMember(typeEnum, "HUGEINT", 16);
	DefineEnumMember(typeEnum, "UHUGEINT", 32);
	DefineEnumMember(typeEnum, "VARCHAR", 17);
	DefineEnumMember(typeEnum, "BLOB", 18);
	DefineEnumMember(typeEnum, "DECIMAL", 19);
	DefineEnumMember(typeEnum, "TIMESTAMP_S", 20);
	DefineEnumMember(typeEnum, "TIMESTAMP_MS", 21);
	DefineEnumMember(typeEnum, "TIMESTAMP_NS", 22);
	DefineEnumMember(typeEnum, "ENUM", 23);
	DefineEnumMember(typeEnum, "LIST", 24);
	DefineEnumMember(typeEnum, "STRUCT", 25);
	DefineEnumMember(typeEnum, "MAP", 26);
	DefineEnumMember(typeEnum, "ARRAY", 33);
	DefineEnumMember(typeEnum, "UUID", 27);
	DefineEnumMember(typeEnum, "UNION", 28);
	DefineEnumMember(typeEnum, "BIT", 29);
	DefineEnumMember(typeEnum, "TIME_TZ", 30);
	DefineEnumMember(typeEnum, "TIMESTAMP_TZ", 31);
  DefineEnumMember(typeEnum, "ANY", 34);
  DefineEnumMember(typeEnum, "BIGNUM", 35);
  DefineEnumMember(typeEnum, "SQLNULL", 36);
  DefineEnumMember(typeEnum, "STRING_LITERAL", 37);
  DefineEnumMember(typeEnum, "INTEGER_LITERAL", 38);
  DefineEnumMember(typeEnum, "TIME_NS", 39);
  DefineEnumMember(typeEnum, "GEOMETRY", 40);
  DefineEnumMember(typeEnum, "VARIANT", 41);
  return typeEnum;
}
