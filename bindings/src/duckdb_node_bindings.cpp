#define NODE_ADDON_API_DISABLE_DEPRECATED
#define NODE_ADDON_API_REQUIRE_BASIC_FINALIZERS
#define NODE_API_NO_EXTERNAL_BUFFERS_ALLOWED
#include "napi.h"

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "duckdb.h"

#define DEFAULT_DUCKDB_API "node-neo-bindings"

// Conversion betweeen structs and objects

Napi::Object MakeDateObject(Napi::Env env, duckdb_date date) {
  auto date_obj = Napi::Object::New(env);
  date_obj.Set("days", Napi::Number::New(env, date.days));
  return date_obj;
}

duckdb_date GetDateFromObject(Napi::Object date_obj) {
  auto days = date_obj.Get("days").As<Napi::Number>().Int32Value();
  return { days };
}

Napi::Object MakeDatePartsObject(Napi::Env env, duckdb_date_struct date_parts) {
  auto date_parts_obj = Napi::Object::New(env);
  date_parts_obj.Set("year", Napi::Number::New(env, date_parts.year));
  date_parts_obj.Set("month", Napi::Number::New(env, date_parts.month));
  date_parts_obj.Set("day", Napi::Number::New(env, date_parts.day));
  return date_parts_obj;
}

duckdb_date_struct GetDatePartsFromObject(Napi::Object date_parts_obj) {
  int32_t year = date_parts_obj.Get("year").As<Napi::Number>().Int32Value();
  int8_t month = date_parts_obj.Get("month").As<Napi::Number>().Int32Value();
  int8_t day = date_parts_obj.Get("day").As<Napi::Number>().Int32Value();
  return { year, month, day };
}

Napi::Object MakeTimeObject(Napi::Env env, duckdb_time time) {
  auto time_obj = Napi::Object::New(env);
  time_obj.Set("micros", Napi::BigInt::New(env, time.micros));
  return time_obj;
}

duckdb_time GetTimeFromObject(Napi::Env env, Napi::Object time_obj) {
  bool lossless;
  auto micros = time_obj.Get("micros").As<Napi::BigInt>().Int64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "micros out of int64 range");
  }
  return { micros };
}

Napi::Object MakeTimePartsObject(Napi::Env env, duckdb_time_struct time_parts) {
  auto time_parts_obj = Napi::Object::New(env);
  time_parts_obj.Set("hour", Napi::Number::New(env, time_parts.hour));
  time_parts_obj.Set("min", Napi::Number::New(env, time_parts.min));
  time_parts_obj.Set("sec", Napi::Number::New(env, time_parts.sec));
  time_parts_obj.Set("micros", Napi::Number::New(env, time_parts.micros));
  return time_parts_obj;
}

duckdb_time_struct GetTimePartsFromObject(Napi::Object time_parts_obj) {
  int8_t hour = time_parts_obj.Get("hour").As<Napi::Number>().Int32Value();
  int8_t min = time_parts_obj.Get("min").As<Napi::Number>().Int32Value();
  int8_t sec = time_parts_obj.Get("sec").As<Napi::Number>().Int32Value();
  int32_t micros = time_parts_obj.Get("micros").As<Napi::Number>().Int32Value();
  return { hour, min, sec, micros };
}

Napi::Object MakeTimeNSObject(Napi::Env env, duckdb_time_ns time_ns) {
  auto time_ns_obj = Napi::Object::New(env);
  time_ns_obj.Set("nanos", Napi::BigInt::New(env, time_ns.nanos));
  return time_ns_obj;
}

duckdb_time_ns GetTimeNSFromObject(Napi::Env env, Napi::Object time_ns_obj) {
  bool lossless;
  auto nanos = time_ns_obj.Get("nanos").As<Napi::BigInt>().Int64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "nanos out of int64 range");
  }
  return { nanos };
}

Napi::Object MakeTimeTZObject(Napi::Env env, duckdb_time_tz time_tz) {
  auto time_tz_obj = Napi::Object::New(env);
  time_tz_obj.Set("bits", Napi::BigInt::New(env, time_tz.bits));
  return time_tz_obj;
}

duckdb_time_tz GetTimeTZFromObject(Napi::Env env, Napi::Object time_tz_obj) {
  bool lossless;
  auto bits = time_tz_obj.Get("bits").As<Napi::BigInt>().Uint64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "bits out of uint64 range");
  }
  return { bits };
}

Napi::Object MakeTimeTZPartsObject(Napi::Env env, duckdb_time_tz_struct time_tz_parts) {
  auto time_tz_parts_obj = Napi::Object::New(env);
  time_tz_parts_obj.Set("time", MakeTimePartsObject(env, time_tz_parts.time));
  time_tz_parts_obj.Set("offset", Napi::Number::New(env, time_tz_parts.offset));
  return time_tz_parts_obj;
}

// GetTimeTZPartsFromObject not used

Napi::Object MakeTimestampObject(Napi::Env env, duckdb_timestamp timestamp) {
  auto timestamp_obj = Napi::Object::New(env);
  timestamp_obj.Set("micros", Napi::BigInt::New(env, timestamp.micros));
  return timestamp_obj;
}

duckdb_timestamp GetTimestampFromObject(Napi::Env env, Napi::Object timestamp_obj) {
  bool lossless;
  auto micros = timestamp_obj.Get("micros").As<Napi::BigInt>().Int64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "micros out of int64 range");
  }
  return { micros };
}

Napi::Object MakeTimestampSecondsObject(Napi::Env env, duckdb_timestamp_s timestamp) {
  auto timestamp_s_obj = Napi::Object::New(env);
  timestamp_s_obj.Set("seconds", Napi::BigInt::New(env, timestamp.seconds));
  return timestamp_s_obj;
}

duckdb_timestamp_s GetTimestampSecondsFromObject(Napi::Env env, Napi::Object timestamp_s_obj) {
  bool lossless;
  auto seconds = timestamp_s_obj.Get("seconds").As<Napi::BigInt>().Int64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "seconds out of int64 range");
  }
  return { seconds };
}

Napi::Object MakeTimestampMillisecondsObject(Napi::Env env, duckdb_timestamp_ms timestamp) {
  auto timestamp_ms_obj = Napi::Object::New(env);
  timestamp_ms_obj.Set("millis", Napi::BigInt::New(env, timestamp.millis));
  return timestamp_ms_obj;
}

duckdb_timestamp_ms GetTimestampMillisecondsFromObject(Napi::Env env, Napi::Object timestamp_ms_obj) {
  bool lossless;
  auto millis = timestamp_ms_obj.Get("millis").As<Napi::BigInt>().Int64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "millis out of int64 range");
  }
  return { millis };
}

Napi::Object MakeTimestampNanosecondsObject(Napi::Env env, duckdb_timestamp_ns timestamp) {
  auto timestamp_ns_obj = Napi::Object::New(env);
  timestamp_ns_obj.Set("nanos", Napi::BigInt::New(env, timestamp.nanos));
  return timestamp_ns_obj;
}

duckdb_timestamp_ns GetTimestampNanosecondsFromObject(Napi::Env env, Napi::Object timestamp_ns_obj) {
  bool lossless;
  auto nanos = timestamp_ns_obj.Get("nanos").As<Napi::BigInt>().Int64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "nanos out of int64 range");
  }
  return { nanos };
}

Napi::Object MakeTimestampPartsObject(Napi::Env env, duckdb_timestamp_struct timestamp_parts) {
  auto timestamp_parts_obj = Napi::Object::New(env);
  timestamp_parts_obj.Set("date", MakeDatePartsObject(env, timestamp_parts.date));
  timestamp_parts_obj.Set("time", MakeTimePartsObject(env, timestamp_parts.time));
  return timestamp_parts_obj;
}

duckdb_timestamp_struct GetTimestampPartsFromObject(Napi::Object timestamp_parts_obj) {
  auto date = GetDatePartsFromObject(timestamp_parts_obj.Get("date").As<Napi::Object>());
  auto time = GetTimePartsFromObject(timestamp_parts_obj.Get("time").As<Napi::Object>());
  return { date, time };
}

Napi::Object MakeIntervalObject(Napi::Env env, duckdb_interval interval) {
  auto interval_obj = Napi::Object::New(env);
  interval_obj.Set("months", Napi::Number::New(env, interval.months));
  interval_obj.Set("days", Napi::Number::New(env, interval.days));
  interval_obj.Set("micros", Napi::BigInt::New(env, interval.micros));
  return interval_obj;
}

duckdb_interval GetIntervalFromObject(Napi::Env env, Napi::Object interval_obj) {
  int32_t months = interval_obj.Get("months").As<Napi::Number>().Int32Value();
  int32_t days = interval_obj.Get("days").As<Napi::Number>().Int32Value();
  bool lossless;
  int64_t micros = interval_obj.Get("micros").As<Napi::BigInt>().Int64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "micros out of int64 range");
  }
  return { months, days, micros };
}

duckdb_hugeint GetHugeIntFromBigInt(Napi::Env env, Napi::BigInt bigint) {
  int sign_bit;
  size_t word_count = 2;
  uint64_t words[2];
  bigint.ToWords(&sign_bit, &word_count, words);
  if (word_count > 2) {
    throw Napi::Error::New(env, "bigint out of hugeint range");
  }
  bool carry = false;
  uint64_t lower;
  if (word_count > 0) {
    lower = words[0];
    if (sign_bit) {
      lower = ~lower + 1;
      carry = lower == 0;
    }
  } else {
    lower = 0;
  }
  uint64_t upper;
  if (word_count > 1) {
    upper = words[1];
    if (sign_bit) {
      upper = ~upper;
      if (carry) {
        upper += 1;
      }
    }
  } else {
    if (word_count > 0 && sign_bit) {
      upper = -1;
    } else {
      upper = 0;
    }
  }
  return { lower, int64_t(upper) };
}

Napi::BigInt MakeBigIntFromHugeInt(Napi::Env env, duckdb_hugeint hugeint) {
  int sign_bit = hugeint.upper < 0 ? 1 : 0;
  size_t word_count = hugeint.upper == -1 ? 1 : 2;
  uint64_t words[2];
  bool carry = false;
  words[0] = hugeint.lower;
  if (sign_bit) {
    words[0] = ~(words[0] - 1);
    carry = words[0] == 0;
  }
  if (word_count > 1) {
    words[1] = hugeint.upper;
    if (sign_bit) {
      if (carry) {
        words[1] -= 1;
      }
      words[1] = ~words[1];
    }
  } else {
    words[1] = 0;
  }
  return Napi::BigInt::New(env, sign_bit, word_count, words);
}

duckdb_uhugeint GetUHugeIntFromBigInt(Napi::Env env, Napi::BigInt bigint) {
  int sign_bit;
  size_t word_count = 2;
  uint64_t words[2];
  bigint.ToWords(&sign_bit, &word_count, words);
  if (word_count > 2 || sign_bit) {
    throw Napi::Error::New(env, "bigint out of uhugeint range");
  }
  uint64_t lower = word_count > 0 ? words[0] : 0;
  uint64_t upper = word_count > 1 ? words[1] : 0;
  return { lower, upper };
}

Napi::BigInt MakeBigIntFromUHugeInt(Napi::Env env, duckdb_uhugeint uhugeint) {
  int sign_bit = 0;
  size_t word_count = 2;
  uint64_t words[2];
  words[0] = uhugeint.lower;
  words[1] = uhugeint.upper;
  return Napi::BigInt::New(env, sign_bit, word_count, words);
}

duckdb_bignum GetBigNumFromBigInt(Napi::Env env, Napi::BigInt bigint) {
  int sign_bit;
  size_t word_count = bigint.WordCount();
  size_t byte_count = word_count * 8;
  uint64_t *words = static_cast<uint64_t*>(duckdb_malloc(byte_count));
  bigint.ToWords(&sign_bit, &word_count, words);
  uint8_t *data = reinterpret_cast<uint8_t*>(words);
  idx_t size = byte_count;
  bool is_negative = bool(sign_bit);
  // convert little-endian to big-endian
  for (size_t i = 0; i < size/2; i++) {
    auto tmp = data[i];
    data[i] = data[size - 1 - i];
    data[size - 1 - i] = tmp;
  }
  return { data, size, is_negative };
}

Napi::BigInt MakeBigIntFromBigNum(Napi::Env env, duckdb_bignum bignum) {
  int sign_bit = bignum.is_negative ? 1 : 0;
  size_t word_count = bignum.size / 8;
  uint8_t *data = static_cast<uint8_t*>(duckdb_malloc(bignum.size));
  // convert big-endian to little-endian
  for (size_t i = 0; i < bignum.size; i++) {
    data[i] = bignum.data[bignum.size - 1 - i];
  }
  uint64_t *words = reinterpret_cast<uint64_t*>(data);
  auto bigint = Napi::BigInt::New(env, sign_bit, word_count, words);
  duckdb_free(data);
  return bigint;
}

Napi::Object MakeDecimalObject(Napi::Env env, duckdb_decimal decimal) {
  auto decimal_obj = Napi::Object::New(env);
  decimal_obj.Set("width", Napi::Number::New(env, decimal.width));
  decimal_obj.Set("scale", Napi::Number::New(env, decimal.scale));
  decimal_obj.Set("value", MakeBigIntFromHugeInt(env, decimal.value));
  return decimal_obj;
}

duckdb_decimal GetDecimalFromObject(Napi::Env env, Napi::Object decimal_obj) {
  uint8_t width = decimal_obj.Get("width").As<Napi::Number>().Uint32Value();
  uint8_t scale = decimal_obj.Get("scale").As<Napi::Number>().Uint32Value();
  auto value = GetHugeIntFromBigInt(env, decimal_obj.Get("value").As<Napi::BigInt>());
  return { width, scale, value };
}

// Externals

template<typename T, typename Finalizer>
Napi::External<T> CreateExternal(Napi::Env env, const napi_type_tag &type_tag, T *data, Finalizer finalizer) {
  auto external = Napi::External<T>::New(env, data, finalizer);
  external.TypeTag(&type_tag);
  return external;
}

template<typename T>
Napi::External<T> CreateExternalWithoutFinalizer(Napi::Env env, const napi_type_tag &type_tag, T *data) {
  auto external = Napi::External<T>::New(env, data);
  external.TypeTag(&type_tag);
  return external;
}

template<typename T>
T* GetDataFromExternal(Napi::Env env, const napi_type_tag &type_tag, Napi::Value value, const char *error_message) {
  auto external = value.As<Napi::External<T>>();
  if (!external.CheckTypeTag(&type_tag)) {
    throw Napi::Error::New(env, error_message);
  }
  return external.Data();
}

// The following type tags are generated using: uuidgen | sed -r -e 's/-//g' -e 's/(.{16})(.*)/0x\1, 0x\2/'

static const napi_type_tag AppenderTypeTag = {
  0x32E0AB3B83F74A89, 0xB785905D92D54996
};

void FinalizeAppender(Napi::BasicEnv, duckdb_appender appender) {
  if (appender) {
    duckdb_appender_destroy(&appender);
    appender = nullptr;
  }
}

Napi::External<_duckdb_appender> CreateExternalForAppender(Napi::Env env, duckdb_appender appender) {
  return CreateExternal<_duckdb_appender>(env, AppenderTypeTag, appender, FinalizeAppender);
}

duckdb_appender GetAppenderFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_appender>(env, AppenderTypeTag, value, "Invalid appender argument");
}

static const napi_type_tag ClientContextTypeTag = {
  0x1E1738782ED94232, 0x867B024D1858DF3A
};

void FinalizeClientContext(Napi::BasicEnv, duckdb_client_context client_context) {
  duckdb_destroy_client_context(&client_context);
}

Napi::External<_duckdb_client_context> CreateExternalForClientContext(Napi::Env env, duckdb_client_context client_context) {
  return CreateExternal<_duckdb_client_context>(env, ClientContextTypeTag, client_context, FinalizeClientContext);
}

duckdb_client_context GetClientContextFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_client_context>(env, ClientContextTypeTag, value, "Invalid client context argument");
}

static const napi_type_tag ConfigTypeTag = {
  0x5963FBB9648B4D2A, 0xB41ADE86056218D1
};

void FinalizeConfig(Napi::BasicEnv, duckdb_config config) {
  if (config) {
    duckdb_destroy_config(&config);
    config = nullptr;
  }
}

Napi::External<_duckdb_config> CreateExternalForConfig(Napi::Env env, duckdb_config config) {
  return CreateExternal<_duckdb_config>(env, ConfigTypeTag, config, FinalizeConfig);
}

duckdb_config GetConfigFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_config>(env, ConfigTypeTag, value, "Invalid config argument");
}

static const napi_type_tag ConnectionTypeTag = {
  0x922B9BF54AB04DFC, 0x8A258578D371DB71
};

typedef struct {
  duckdb_connection connection;
} duckdb_connection_holder;

duckdb_connection_holder *CreateConnectionHolder(duckdb_connection connection) {
  auto connection_holder_ptr = reinterpret_cast<duckdb_connection_holder*>(duckdb_malloc(sizeof(duckdb_connection_holder)));
  connection_holder_ptr->connection = connection;
  return connection_holder_ptr;
}

void FinalizeConnectionHolder(Napi::BasicEnv, duckdb_connection_holder *connection_holder_ptr) {
  // duckdb_disconnect is a no-op if already disconnected
  duckdb_disconnect(&connection_holder_ptr->connection);
  duckdb_free(connection_holder_ptr);
}

Napi::External<duckdb_connection_holder> CreateExternalForConnection(Napi::Env env, duckdb_connection connection) {
  return CreateExternal<duckdb_connection_holder>(env, ConnectionTypeTag, CreateConnectionHolder(connection), FinalizeConnectionHolder);
}

duckdb_connection_holder *GetConnectionHolderFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<duckdb_connection_holder>(env, ConnectionTypeTag, value, "Invalid connection argument");
}

duckdb_connection GetConnectionFromExternal(Napi::Env env, Napi::Value value) {
  return GetConnectionHolderFromExternal(env, value)->connection;
}

static const napi_type_tag DatabaseTypeTag = {
  0x835A8533653C40D1, 0x83B3BE2B233BA8F3
};

typedef struct {
  duckdb_database database;
} duckdb_database_holder;

duckdb_database_holder *CreateDatabaseHolder(duckdb_database database) {
  auto database_holder_ptr = reinterpret_cast<duckdb_database_holder*>(duckdb_malloc(sizeof(duckdb_database_holder)));
  database_holder_ptr->database = database;
  return database_holder_ptr;
}

void FinalizeDatabaseHolder(Napi::BasicEnv, duckdb_database_holder *database_holder_ptr) {
  // duckdb_close is a no-op if already closed
  duckdb_close(&database_holder_ptr->database);
  duckdb_free(database_holder_ptr);
}

Napi::External<duckdb_database_holder> CreateExternalForDatabase(Napi::Env env, duckdb_database database) {
  return CreateExternal<duckdb_database_holder>(env, DatabaseTypeTag, CreateDatabaseHolder(database), FinalizeDatabaseHolder);
}

duckdb_database_holder *GetDatabaseHolderFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<duckdb_database_holder>(env, DatabaseTypeTag, value, "Invalid database argument");
}

duckdb_database GetDatabaseFromExternal(Napi::Env env, Napi::Value value) {
  return GetDatabaseHolderFromExternal(env, value)->database;
}

static const napi_type_tag DataChunkTypeTag = {
  0x2C7537AB063A4296, 0xB1E70F08B0BBD1A3
};

void FinalizeDataChunk(Napi::BasicEnv, duckdb_data_chunk chunk) {
  if (chunk) {
    duckdb_destroy_data_chunk(&chunk);
    chunk = nullptr;
  }
}

Napi::External<_duckdb_data_chunk> CreateExternalForDataChunk(Napi::Env env, duckdb_data_chunk chunk) {
  return CreateExternal<_duckdb_data_chunk>(env, DataChunkTypeTag, chunk, FinalizeDataChunk);
}

Napi::External<_duckdb_data_chunk> CreateExternalForDataChunkWithoutFinalizer(Napi::Env env, duckdb_data_chunk chunk) {
  return CreateExternalWithoutFinalizer<_duckdb_data_chunk>(env, DataChunkTypeTag, chunk);
}

duckdb_data_chunk GetDataChunkFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_data_chunk>(env, DataChunkTypeTag, value, "Invalid data chunk argument");
}

static const napi_type_tag ExtractedStatementsTypeTag = {
  0x59288E1C60C44EEB, 0xBFA35376EE0F04DD
};

void FinalizeExtractedStatements(Napi::BasicEnv, duckdb_extracted_statements extracted_statements) {
  if (extracted_statements) {
    duckdb_destroy_extracted(&extracted_statements);
    extracted_statements = nullptr;
  }
}

Napi::External<_duckdb_extracted_statements> CreateExternalForExtractedStatements(Napi::Env env, duckdb_extracted_statements extracted_statements) {
  return CreateExternal<_duckdb_extracted_statements>(env, ExtractedStatementsTypeTag, extracted_statements, FinalizeExtractedStatements);
}

duckdb_extracted_statements GetExtractedStatementsFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_extracted_statements>(env, ExtractedStatementsTypeTag, value, "Invalid extracted statements argument");
}

static const napi_type_tag FunctionInfoTypeTag = {
  0xB0E6739D698048EA, 0x9E79734E3E137AC3
};

Napi::External<_duckdb_function_info> CreateExternalForFunctionInfoWithoutFinalizer(Napi::Env env, duckdb_function_info function_info) {
  // FunctionInfo objects are never explicitly created; they are passed in to function callbacks.
  return CreateExternalWithoutFinalizer<_duckdb_function_info>(env, FunctionInfoTypeTag, function_info);
}

duckdb_function_info GetFunctionInfoFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_function_info>(env, FunctionInfoTypeTag, value, "Invalid function info argument");
}



static const napi_type_tag InstanceCacheTypeTag = {
  0x2F3346E30FB5457C, 0xB9201EE5112EEF9F
};

void FinalizeInstanceCache(Napi::BasicEnv, duckdb_instance_cache instance_cache) {
  duckdb_destroy_instance_cache(&instance_cache);
}

Napi::External<_duckdb_instance_cache> CreateExternalForInstanceCache(Napi::Env env, duckdb_instance_cache instance_cache) {
  return CreateExternal<_duckdb_instance_cache>(env, InstanceCacheTypeTag, instance_cache, FinalizeInstanceCache);
}

duckdb_instance_cache GetInstanceCacheFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_instance_cache>(env, InstanceCacheTypeTag, value, "Invalid instance cache argument");
}

static const napi_type_tag LogicalTypeTypeTag = {
  0x78AF202191ED4A23, 0x8093715369592A2B
};

void FinalizeLogicalType(Napi::BasicEnv, duckdb_logical_type logical_type) {
  duckdb_destroy_logical_type(&logical_type);
}

Napi::External<_duckdb_logical_type> CreateExternalForLogicalType(Napi::Env env, duckdb_logical_type logical_type) {
  return CreateExternal<_duckdb_logical_type>(env, LogicalTypeTypeTag, logical_type, FinalizeLogicalType);
}

Napi::External<_duckdb_logical_type> CreateExternalForLogicalTypeWithoutFinalizer(Napi::Env env, duckdb_logical_type logical_type) {
  return CreateExternalWithoutFinalizer<_duckdb_logical_type>(env, LogicalTypeTypeTag, logical_type);
}

duckdb_logical_type GetLogicalTypeFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_logical_type>(env, LogicalTypeTypeTag, value, "Invalid logical type argument");
}

static const napi_type_tag PendingResultTypeTag = {
  0x257E88ECE8294FEC, 0xB64963BBBD1DBB41
};

void FinalizePendingResult(Napi::BasicEnv, duckdb_pending_result pending_result) {
  if (pending_result) {
    duckdb_destroy_pending(&pending_result);
    pending_result = nullptr;
  }
}

Napi::External<_duckdb_pending_result> CreateExternalForPendingResult(Napi::Env env, duckdb_pending_result pending_result) {
  return CreateExternal<_duckdb_pending_result>(env, PendingResultTypeTag, pending_result, FinalizePendingResult);
}

duckdb_pending_result GetPendingResultFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_pending_result>(env, PendingResultTypeTag, value, "Invalid pending result argument");
}

static const napi_type_tag PreparedStatementTypeTag = {
  0xA8B03DAD16D34416, 0x9735A7E1F2A1240C
};

typedef struct {
  duckdb_prepared_statement prepared;
} duckdb_prepared_statement_holder;

duckdb_prepared_statement_holder *CreatePreparedStatementHolder(duckdb_prepared_statement prepared) {
  auto prepared_statement_holder_ptr = reinterpret_cast<duckdb_prepared_statement_holder*>(duckdb_malloc(sizeof(duckdb_prepared_statement_holder)));
  prepared_statement_holder_ptr->prepared = prepared;
  return prepared_statement_holder_ptr;
}

void FinalizePreparedStatementHolder(Napi::BasicEnv, duckdb_prepared_statement_holder *prepared_statement_holder_ptr) {
  // duckdb_destroy_prepare is a no-op if already destroyed
  duckdb_destroy_prepare(&prepared_statement_holder_ptr->prepared);
  duckdb_free(prepared_statement_holder_ptr);
}

Napi::External<duckdb_prepared_statement_holder> CreateExternalForPreparedStatement(Napi::Env env, duckdb_prepared_statement prepared_statement) {
  return CreateExternal<duckdb_prepared_statement_holder>(env, PreparedStatementTypeTag, CreatePreparedStatementHolder(prepared_statement), FinalizePreparedStatementHolder);
}

duckdb_prepared_statement_holder *GetPreparedStatementHolderFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<duckdb_prepared_statement_holder>(env, PreparedStatementTypeTag, value, "Invalid prepared statement argument");
}

duckdb_prepared_statement GetPreparedStatementFromExternal(Napi::Env env, Napi::Value value) {
  return GetPreparedStatementHolderFromExternal(env, value)->prepared;
}

static const napi_type_tag ResultTypeTag = {
  0x08F7FE3AE12345E5, 0x8733310DC29372D9
};

void FinalizeResult(Napi::BasicEnv, duckdb_result *result_ptr) {
  if (result_ptr) {
    duckdb_destroy_result(result_ptr);
    duckdb_free(result_ptr); // memory for duckdb_result struct is malloc'd in QueryWorker, ExecutePreparedWorker, or ExecutePendingWorker.
    result_ptr = nullptr;
  }
}

Napi::External<duckdb_result> CreateExternalForResult(Napi::Env env, duckdb_result *result_ptr) {
  return CreateExternal<duckdb_result>(env, ResultTypeTag, result_ptr, FinalizeResult);
}

duckdb_result *GetResultFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<duckdb_result>(env, ResultTypeTag, value, "Invalid result argument");
}

static const napi_type_tag ScalarFunctionTypeTag = {
  0x95D48B7051D14994, 0x9F883D7DF5DEA86D
};

using ScalarFunctionMainTSFNContext = std::nullptr_t;
struct ScalarFunctionMainTSFNData;
void ScalarFunctionMainTSFNCallback(Napi::Env env, Napi::Function callback, ScalarFunctionMainTSFNContext *context, ScalarFunctionMainTSFNData *data);
using ScalarFunctionMainTSFN = Napi::TypedThreadSafeFunction<ScalarFunctionMainTSFNContext, ScalarFunctionMainTSFNData, ScalarFunctionMainTSFNCallback>;

struct ScalarFunctionInternalExtraInfo {
  std::unique_ptr<ScalarFunctionMainTSFN> main_tsfn;
  std::unique_ptr<Napi::ObjectReference> user_extra_info_ref;

  ScalarFunctionInternalExtraInfo() {}

  ~ScalarFunctionInternalExtraInfo() {
    if (bool(main_tsfn)) {
      main_tsfn->Release();
    }
  }

  void SetMainFunction(Napi::Env env, Napi::Function func) {
    if (bool(main_tsfn)) {
      main_tsfn->Release();
    }
    main_tsfn = std::make_unique<ScalarFunctionMainTSFN>(ScalarFunctionMainTSFN::New(env, func, "ScalarFunctionMain", 0, 1));
  }

  void SetUserExtraInfo(Napi::Object user_extra_info) {
    user_extra_info_ref = std::make_unique<Napi::ObjectReference>(user_extra_info.IsUndefined() ? Napi::ObjectReference() : Napi::Persistent(user_extra_info));
  }
};

void DeleteScalarFunctionInternalExtraInfo(ScalarFunctionInternalExtraInfo *internal_extra_info) {
  delete internal_extra_info;
}

struct ScalarFunctionHolder {
  duckdb_scalar_function scalar_function;
  ScalarFunctionInternalExtraInfo *internal_extra_info;

  ScalarFunctionHolder(duckdb_scalar_function scalar_function_in): scalar_function(scalar_function_in), internal_extra_info(nullptr) {}

  ~ScalarFunctionHolder() {
    // duckdb_destroy_scalar_function is a no-op if already destroyed
    duckdb_destroy_scalar_function(&scalar_function);
  }

  ScalarFunctionInternalExtraInfo *EnsureInternalExtraInfo() {
    if (!internal_extra_info) {
      internal_extra_info = new ScalarFunctionInternalExtraInfo();
      duckdb_scalar_function_set_extra_info(scalar_function, internal_extra_info, reinterpret_cast<duckdb_delete_callback_t>(DeleteScalarFunctionInternalExtraInfo));
    }
    return internal_extra_info;
  }
};

ScalarFunctionHolder *CreateScalarFunctionHolder(duckdb_scalar_function scalar_function) {
  return new ScalarFunctionHolder(scalar_function);
}

void FinalizeScalarFunctionHolder(Napi::BasicEnv, ScalarFunctionHolder *holder) {
  delete holder;
}

Napi::External<ScalarFunctionHolder> CreateExternalForScalarFunction(Napi::Env env, duckdb_scalar_function scalar_function) {
  return CreateExternal<ScalarFunctionHolder>(env, ScalarFunctionTypeTag, CreateScalarFunctionHolder(scalar_function), FinalizeScalarFunctionHolder);
}

ScalarFunctionHolder *GetScalarFunctionHolderFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<ScalarFunctionHolder>(env, ScalarFunctionTypeTag, value, "Invalid scalar function argument");
}

duckdb_scalar_function GetScalarFunctionFromExternal(Napi::Env env, Napi::Value value) {
  return GetScalarFunctionHolderFromExternal(env, value)->scalar_function;
}

static const napi_type_tag ValueTypeTag = {
  0xC60F36613BF14E93, 0xBAA92848936FAA25
};

void FinalizeValue(Napi::BasicEnv, duckdb_value value) {
  if (value) {
    duckdb_destroy_value(&value);
    value = nullptr;
  }
}

Napi::External<_duckdb_value> CreateExternalForValue(Napi::Env env, duckdb_value value) {
  return CreateExternal<_duckdb_value>(env, ValueTypeTag, value, FinalizeValue);
}

duckdb_value GetValueFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_value>(env, ValueTypeTag, value, "Invalid value argument");
}

static const napi_type_tag VectorTypeTag = {
  0x9FE56DE8E3124D07, 0x9ABF31145EDE1C9E
};

Napi::External<_duckdb_vector> CreateExternalForVectorWithoutFinalizer(Napi::Env env, duckdb_vector vector) {
  // Vectors live as long as their containing data chunk; they cannot be explicitly destroyed.
  return CreateExternalWithoutFinalizer<_duckdb_vector>(env, VectorTypeTag, vector);
}

duckdb_vector GetVectorFromExternal(Napi::Env env, Napi::Value value) {
  return GetDataFromExternal<_duckdb_vector>(env, VectorTypeTag, value, "Invalid vector argument");
}

// Scalar functions

struct ScalarFunctionMainTSFNData {
  duckdb_function_info info;
  duckdb_data_chunk input;
  duckdb_vector output;
  std::condition_variable *cv;
  std::mutex *cv_mutex;
  bool done;
};

void ScalarFunctionMainTSFNCallback(Napi::Env env, Napi::Function callback, ScalarFunctionMainTSFNContext *context, ScalarFunctionMainTSFNData *data) {
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
  }
  data->cv->notify_one();
}

ScalarFunctionInternalExtraInfo *GetScalarFunctionInternalExtraInfo(duckdb_function_info function_info) {
  return reinterpret_cast<ScalarFunctionInternalExtraInfo*>(duckdb_scalar_function_get_extra_info(function_info));
}

void ScalarFunctionMainFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
  auto internal_extra_info = GetScalarFunctionInternalExtraInfo(info);
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

  GetOrCreateFromCacheWorker(Napi::Env env, duckdb_instance_cache instance_cache, std::optional<std::string> path, duckdb_config config)
      : PromiseWorker(env), instance_cache_(instance_cache), path_(path), config_(config) {
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
  std::optional<std::string> path_;
  duckdb_config config_;
  duckdb_database database_ = nullptr;

};

class OpenWorker : public PromiseWorker {

public:

  OpenWorker(Napi::Env env, std::optional<std::string> path, duckdb_config config)
    : PromiseWorker(env), path_(path), config_(config) {
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
  duckdb_database database_ = nullptr;

};

class ConnectWorker : public PromiseWorker {

public:

  ConnectWorker(Napi::Env env, duckdb_database database)
    : PromiseWorker(env), database_(database) {
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
  duckdb_connection connection_ = nullptr;

};

class QueryWorker : public PromiseWorker {

public:

  QueryWorker(Napi::Env env, duckdb_connection connection, std::string query)
    : PromiseWorker(env), connection_(connection), query_(query) {
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
  std::string query_;
  duckdb_result *result_ptr_ = nullptr;

};

class PrepareWorker : public PromiseWorker {

public:

  PrepareWorker(Napi::Env env, duckdb_connection connection, std::string query)
    : PromiseWorker(env), connection_(connection), query_(query) {
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
  std::string query_;
  duckdb_prepared_statement prepared_statement_ = nullptr;

};

class ExecutePreparedWorker : public PromiseWorker {

public:

  ExecutePreparedWorker(Napi::Env env, duckdb_prepared_statement prepared_statement)
    : PromiseWorker(env), prepared_statement_(prepared_statement) {
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
  duckdb_result *result_ptr_ = nullptr;

};

class ExecutePreparedStreamingWorker : public PromiseWorker {

public:

  ExecutePreparedStreamingWorker(Napi::Env env, duckdb_prepared_statement prepared_statement)
    : PromiseWorker(env), prepared_statement_(prepared_statement) {
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
  duckdb_result *result_ptr_;

};

class ExtractStatementsWorker : public PromiseWorker {

public:

  ExtractStatementsWorker(Napi::Env env, duckdb_connection connection, std::string query)
    : PromiseWorker(env), connection_(connection), query_(query) {
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
  std::string query_;
  duckdb_extracted_statements extracted_statements_ = nullptr;
  idx_t statement_count_ = 0;

};

class PrepareExtractedStatementWorker : public PromiseWorker {

public:

  PrepareExtractedStatementWorker(Napi::Env env, duckdb_connection connection, duckdb_extracted_statements extracted_statements, idx_t index)
    : PromiseWorker(env), connection_(connection), extracted_statements_(extracted_statements), index_(index) {
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
  duckdb_extracted_statements extracted_statements_;
  idx_t index_;
  duckdb_prepared_statement prepared_statement_ = nullptr;

};

class ExecutePendingWorker : public PromiseWorker {

public:

  ExecutePendingWorker(Napi::Env env, duckdb_pending_result pending_result)
    : PromiseWorker(env), pending_result_(pending_result) {
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
  duckdb_result *result_ptr_ = nullptr;

};

class FetchWorker : public PromiseWorker {

public:

  FetchWorker(Napi::Env env, duckdb_result *result_ptr)
    : PromiseWorker(env), result_ptr_(result_ptr) {
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
  duckdb_data_chunk data_chunk_ = nullptr;

};

// Enums

void DefineEnumMember(Napi::Object enumObj, const char *key, uint32_t value) {
  enumObj.Set(key, value);
  enumObj.Set(value, key);
}

Napi::Object CreatePendingStateEnum(Napi::Env env) {
  auto pendingStateEnum = Napi::Object::New(env);
  DefineEnumMember(pendingStateEnum, "RESULT_READY", 0);
  DefineEnumMember(pendingStateEnum, "RESULT_NOT_READY", 1);
  DefineEnumMember(pendingStateEnum, "ERROR", 2);
  DefineEnumMember(pendingStateEnum, "NO_TASKS_AVAILABLE", 3);
  return pendingStateEnum;
}

Napi::Object CreateResultTypeEnum(Napi::Env env) {
  auto resultTypeEnum = Napi::Object::New(env);
  DefineEnumMember(resultTypeEnum, "INVALID", 0);
  DefineEnumMember(resultTypeEnum, "CHANGED_ROWS", 1);
  DefineEnumMember(resultTypeEnum, "NOTHING", 2);
  DefineEnumMember(resultTypeEnum, "QUERY_RESULT", 3);
  return resultTypeEnum;
}

Napi::Object CreateStatementTypeEnum(Napi::Env env) {
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

Napi::Object CreateTypeEnum(Napi::Env env) {
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
  return typeEnum;
}

// Addon

class DuckDBNodeAddon : public Napi::Addon<DuckDBNodeAddon> {

public:

  DuckDBNodeAddon(Napi::Env env, Napi::Object exports) {
    DefineAddon(exports, {
      InstanceValue("sizeof_bool", Napi::Number::New(env, sizeof(bool))),

      InstanceValue("PendingState", CreatePendingStateEnum(env)),
      InstanceValue("ResultType", CreateResultTypeEnum(env)),
      InstanceValue("StatementType", CreateStatementTypeEnum(env)),
      InstanceValue("Type", CreateTypeEnum(env)),

      InstanceMethod("create_instance_cache", &DuckDBNodeAddon::create_instance_cache),
      InstanceMethod("get_or_create_from_cache", &DuckDBNodeAddon::get_or_create_from_cache),

      InstanceMethod("open", &DuckDBNodeAddon::open),
      InstanceMethod("close_sync", &DuckDBNodeAddon::close_sync),
      InstanceMethod("connect", &DuckDBNodeAddon::connect),
      InstanceMethod("interrupt", &DuckDBNodeAddon::interrupt),
      InstanceMethod("query_progress", &DuckDBNodeAddon::query_progress),
      InstanceMethod("disconnect_sync", &DuckDBNodeAddon::disconnect_sync),

      InstanceMethod("connection_get_client_context", &DuckDBNodeAddon::connection_get_client_context),
      InstanceMethod("client_context_get_connection_id", &DuckDBNodeAddon::client_context_get_connection_id),

      InstanceMethod("library_version", &DuckDBNodeAddon::library_version),

      InstanceMethod("get_table_names", &DuckDBNodeAddon::get_table_names),

      InstanceMethod("create_config", &DuckDBNodeAddon::create_config),
      InstanceMethod("config_count", &DuckDBNodeAddon::config_count),
      InstanceMethod("get_config_flag", &DuckDBNodeAddon::get_config_flag),
      InstanceMethod("set_config", &DuckDBNodeAddon::set_config),

      InstanceMethod("query", &DuckDBNodeAddon::query),
      InstanceMethod("column_name", &DuckDBNodeAddon::column_name),
      InstanceMethod("column_type", &DuckDBNodeAddon::column_type),
      InstanceMethod("result_statement_type", &DuckDBNodeAddon::result_statement_type),
      InstanceMethod("column_logical_type", &DuckDBNodeAddon::column_logical_type),
      InstanceMethod("column_count", &DuckDBNodeAddon::column_count),
      InstanceMethod("row_count", &DuckDBNodeAddon::row_count),
      InstanceMethod("rows_changed", &DuckDBNodeAddon::rows_changed),
      InstanceMethod("result_get_chunk", &DuckDBNodeAddon::result_get_chunk),
      InstanceMethod("result_is_streaming", &DuckDBNodeAddon::result_is_streaming),
      InstanceMethod("result_chunk_count", &DuckDBNodeAddon::result_chunk_count),
      InstanceMethod("result_return_type", &DuckDBNodeAddon::result_return_type),

      InstanceMethod("vector_size", &DuckDBNodeAddon::vector_size),

      InstanceMethod("from_date", &DuckDBNodeAddon::from_date),
      InstanceMethod("to_date", &DuckDBNodeAddon::to_date),
      InstanceMethod("is_finite_date", &DuckDBNodeAddon::is_finite_date),
      InstanceMethod("from_time", &DuckDBNodeAddon::from_time),
      InstanceMethod("create_time_tz", &DuckDBNodeAddon::create_time_tz),
      InstanceMethod("from_time_tz", &DuckDBNodeAddon::from_time_tz),
      InstanceMethod("to_time", &DuckDBNodeAddon::to_time),
      InstanceMethod("from_timestamp", &DuckDBNodeAddon::from_timestamp),
      InstanceMethod("to_timestamp", &DuckDBNodeAddon::to_timestamp),
      InstanceMethod("is_finite_timestamp", &DuckDBNodeAddon::is_finite_timestamp),
      InstanceMethod("is_finite_timestamp_s", &DuckDBNodeAddon::is_finite_timestamp_s),
      InstanceMethod("is_finite_timestamp_ms", &DuckDBNodeAddon::is_finite_timestamp_ms),
      InstanceMethod("is_finite_timestamp_ns", &DuckDBNodeAddon::is_finite_timestamp_ns),

      InstanceMethod("hugeint_to_double", &DuckDBNodeAddon::hugeint_to_double),
      InstanceMethod("double_to_hugeint", &DuckDBNodeAddon::double_to_hugeint),
      InstanceMethod("uhugeint_to_double", &DuckDBNodeAddon::uhugeint_to_double),
      InstanceMethod("double_to_uhugeint", &DuckDBNodeAddon::double_to_uhugeint),
      InstanceMethod("double_to_decimal", &DuckDBNodeAddon::double_to_decimal),
      InstanceMethod("decimal_to_double", &DuckDBNodeAddon::decimal_to_double),

      InstanceMethod("prepare", &DuckDBNodeAddon::prepare),
      InstanceMethod("destroy_prepare_sync", &DuckDBNodeAddon::destroy_prepare_sync),
      InstanceMethod("nparams", &DuckDBNodeAddon::nparams),
      InstanceMethod("parameter_name", &DuckDBNodeAddon::parameter_name),
      InstanceMethod("param_type", &DuckDBNodeAddon::param_type),
      InstanceMethod("param_logical_type", &DuckDBNodeAddon::param_logical_type),
      InstanceMethod("clear_bindings", &DuckDBNodeAddon::clear_bindings),
      InstanceMethod("prepared_statement_type", &DuckDBNodeAddon::prepared_statement_type),
      InstanceMethod("prepared_statement_column_count", &DuckDBNodeAddon::prepared_statement_column_count),
      InstanceMethod("prepared_statement_column_name", &DuckDBNodeAddon::prepared_statement_column_name),
      InstanceMethod("prepared_statement_column_logical_type", &DuckDBNodeAddon::prepared_statement_column_logical_type),
      InstanceMethod("prepared_statement_column_type", &DuckDBNodeAddon::prepared_statement_column_type),
      InstanceMethod("bind_value", &DuckDBNodeAddon::bind_value),
      InstanceMethod("bind_parameter_index", &DuckDBNodeAddon::bind_parameter_index),
      InstanceMethod("bind_boolean", &DuckDBNodeAddon::bind_boolean),
      InstanceMethod("bind_int8", &DuckDBNodeAddon::bind_int8),
      InstanceMethod("bind_int16", &DuckDBNodeAddon::bind_int16),
      InstanceMethod("bind_int32", &DuckDBNodeAddon::bind_int32),
      InstanceMethod("bind_int64", &DuckDBNodeAddon::bind_int64),
      InstanceMethod("bind_hugeint", &DuckDBNodeAddon::bind_hugeint),
      InstanceMethod("bind_uhugeint", &DuckDBNodeAddon::bind_uhugeint),
      InstanceMethod("bind_decimal", &DuckDBNodeAddon::bind_decimal),
      InstanceMethod("bind_uint8", &DuckDBNodeAddon::bind_uint8),
      InstanceMethod("bind_uint16", &DuckDBNodeAddon::bind_uint16),
      InstanceMethod("bind_uint32", &DuckDBNodeAddon::bind_uint32),
      InstanceMethod("bind_uint64", &DuckDBNodeAddon::bind_uint64),
      InstanceMethod("bind_float", &DuckDBNodeAddon::bind_float),
      InstanceMethod("bind_double", &DuckDBNodeAddon::bind_double),
      InstanceMethod("bind_date", &DuckDBNodeAddon::bind_date),
      InstanceMethod("bind_time", &DuckDBNodeAddon::bind_time),
      InstanceMethod("bind_timestamp", &DuckDBNodeAddon::bind_timestamp),
      InstanceMethod("bind_timestamp_tz", &DuckDBNodeAddon::bind_timestamp_tz),
      InstanceMethod("bind_interval", &DuckDBNodeAddon::bind_interval),
      InstanceMethod("bind_varchar", &DuckDBNodeAddon::bind_varchar),
      InstanceMethod("bind_blob", &DuckDBNodeAddon::bind_blob),
      InstanceMethod("bind_null", &DuckDBNodeAddon::bind_null),
      InstanceMethod("execute_prepared", &DuckDBNodeAddon::execute_prepared),
      InstanceMethod("execute_prepared_streaming", &DuckDBNodeAddon::execute_prepared_streaming),

      InstanceMethod("extract_statements", &DuckDBNodeAddon::extract_statements),
      InstanceMethod("prepare_extracted_statement", &DuckDBNodeAddon::prepare_extracted_statement),
      InstanceMethod("extract_statements_error", &DuckDBNodeAddon::extract_statements_error),

      InstanceMethod("pending_prepared", &DuckDBNodeAddon::pending_prepared),
      InstanceMethod("pending_prepared_streaming", &DuckDBNodeAddon::pending_prepared_streaming),
      InstanceMethod("pending_error", &DuckDBNodeAddon::pending_error),
      InstanceMethod("pending_execute_task", &DuckDBNodeAddon::pending_execute_task),
      InstanceMethod("pending_execute_check_state", &DuckDBNodeAddon::pending_execute_check_state),
      InstanceMethod("execute_pending", &DuckDBNodeAddon::execute_pending),
      InstanceMethod("pending_execution_is_finished", &DuckDBNodeAddon::pending_execution_is_finished),

      InstanceMethod("create_varchar", &DuckDBNodeAddon::create_varchar),
      InstanceMethod("create_bool", &DuckDBNodeAddon::create_bool),
      InstanceMethod("create_int8", &DuckDBNodeAddon::create_int8),
      InstanceMethod("create_uint8", &DuckDBNodeAddon::create_uint8),
      InstanceMethod("create_int16", &DuckDBNodeAddon::create_int16),
      InstanceMethod("create_uint16", &DuckDBNodeAddon::create_uint16),
      InstanceMethod("create_int32", &DuckDBNodeAddon::create_int32),
      InstanceMethod("create_uint32", &DuckDBNodeAddon::create_uint32),
      InstanceMethod("create_uint64", &DuckDBNodeAddon::create_uint64),
      InstanceMethod("create_int64", &DuckDBNodeAddon::create_int64),
      InstanceMethod("create_hugeint", &DuckDBNodeAddon::create_hugeint),
      InstanceMethod("create_uhugeint", &DuckDBNodeAddon::create_uhugeint),
      InstanceMethod("create_bignum", &DuckDBNodeAddon::create_bignum),
      InstanceMethod("create_decimal", &DuckDBNodeAddon::create_decimal),
      InstanceMethod("create_float", &DuckDBNodeAddon::create_float),
      InstanceMethod("create_double", &DuckDBNodeAddon::create_double),
      InstanceMethod("create_date", &DuckDBNodeAddon::create_date),
      InstanceMethod("create_time", &DuckDBNodeAddon::create_time),
      InstanceMethod("create_time_ns", &DuckDBNodeAddon::create_time_ns),
      InstanceMethod("create_time_tz_value", &DuckDBNodeAddon::create_time_tz_value),
      InstanceMethod("create_timestamp", &DuckDBNodeAddon::create_timestamp),
      InstanceMethod("create_timestamp_tz", &DuckDBNodeAddon::create_timestamp_tz),
      InstanceMethod("create_timestamp_s", &DuckDBNodeAddon::create_timestamp_s),
      InstanceMethod("create_timestamp_ms", &DuckDBNodeAddon::create_timestamp_ms),
      InstanceMethod("create_timestamp_ns", &DuckDBNodeAddon::create_timestamp_ns),
      InstanceMethod("create_interval", &DuckDBNodeAddon::create_interval),
      InstanceMethod("create_blob", &DuckDBNodeAddon::create_blob),
      InstanceMethod("create_bit", &DuckDBNodeAddon::create_bit),
      InstanceMethod("create_uuid", &DuckDBNodeAddon::create_uuid),
      InstanceMethod("get_bool", &DuckDBNodeAddon::get_bool),
      InstanceMethod("get_int8", &DuckDBNodeAddon::get_int8),
      InstanceMethod("get_uint8", &DuckDBNodeAddon::get_uint8),
      InstanceMethod("get_int16", &DuckDBNodeAddon::get_int16),
      InstanceMethod("get_uint16", &DuckDBNodeAddon::get_uint16),
      InstanceMethod("get_int32", &DuckDBNodeAddon::get_int32),
      InstanceMethod("get_uint32", &DuckDBNodeAddon::get_uint32),
      InstanceMethod("get_int64", &DuckDBNodeAddon::get_int64),
      InstanceMethod("get_uint64", &DuckDBNodeAddon::get_uint64),
      InstanceMethod("get_hugeint", &DuckDBNodeAddon::get_hugeint),
      InstanceMethod("get_uhugeint", &DuckDBNodeAddon::get_uhugeint),
      InstanceMethod("get_bignum", &DuckDBNodeAddon::get_bignum),
      InstanceMethod("get_decimal", &DuckDBNodeAddon::get_decimal),
      InstanceMethod("get_float", &DuckDBNodeAddon::get_float),
      InstanceMethod("get_double", &DuckDBNodeAddon::get_double),
      InstanceMethod("get_date", &DuckDBNodeAddon::get_date),
      InstanceMethod("get_time", &DuckDBNodeAddon::get_time),
      InstanceMethod("get_time_ns", &DuckDBNodeAddon::get_time_ns),
      InstanceMethod("get_time_tz", &DuckDBNodeAddon::get_time_tz),
      InstanceMethod("get_timestamp", &DuckDBNodeAddon::get_timestamp),
      InstanceMethod("get_timestamp_tz", &DuckDBNodeAddon::get_timestamp_tz),
      InstanceMethod("get_timestamp_s", &DuckDBNodeAddon::get_timestamp_s),
      InstanceMethod("get_timestamp_ms", &DuckDBNodeAddon::get_timestamp_ms),
      InstanceMethod("get_timestamp_ns", &DuckDBNodeAddon::get_timestamp_ns),
      InstanceMethod("get_interval", &DuckDBNodeAddon::get_interval),
      InstanceMethod("get_value_type", &DuckDBNodeAddon::get_value_type),
      InstanceMethod("get_blob", &DuckDBNodeAddon::get_blob),
      InstanceMethod("get_bit", &DuckDBNodeAddon::get_bit),
      InstanceMethod("get_uuid", &DuckDBNodeAddon::get_uuid),
      InstanceMethod("get_varchar", &DuckDBNodeAddon::get_varchar),
      InstanceMethod("create_struct_value", &DuckDBNodeAddon::create_struct_value),
      InstanceMethod("create_list_value", &DuckDBNodeAddon::create_list_value),
      InstanceMethod("create_array_value", &DuckDBNodeAddon::create_array_value),
      InstanceMethod("create_map_value", &DuckDBNodeAddon::create_map_value),
      InstanceMethod("create_union_value", &DuckDBNodeAddon::create_union_value),
      InstanceMethod("get_map_size", &DuckDBNodeAddon::get_map_size),
      InstanceMethod("get_map_key", &DuckDBNodeAddon::get_map_key),
      InstanceMethod("get_map_value", &DuckDBNodeAddon::get_map_value),
      InstanceMethod("is_null_value", &DuckDBNodeAddon::is_null_value),
      InstanceMethod("create_null_value", &DuckDBNodeAddon::create_null_value),
      InstanceMethod("get_list_size", &DuckDBNodeAddon::get_list_size),
      InstanceMethod("get_list_child", &DuckDBNodeAddon::get_list_child),
      InstanceMethod("create_enum_value", &DuckDBNodeAddon::create_enum_value),
      InstanceMethod("get_enum_value", &DuckDBNodeAddon::get_enum_value),
      InstanceMethod("get_struct_child", &DuckDBNodeAddon::get_struct_child),

      InstanceMethod("create_logical_type", &DuckDBNodeAddon::create_logical_type),
      InstanceMethod("logical_type_get_alias", &DuckDBNodeAddon::logical_type_get_alias),
      InstanceMethod("logical_type_set_alias", &DuckDBNodeAddon::logical_type_set_alias),
      InstanceMethod("create_list_type", &DuckDBNodeAddon::create_list_type),
      InstanceMethod("create_array_type", &DuckDBNodeAddon::create_array_type),
      InstanceMethod("create_map_type", &DuckDBNodeAddon::create_map_type),
      InstanceMethod("create_union_type", &DuckDBNodeAddon::create_union_type),
      InstanceMethod("create_struct_type", &DuckDBNodeAddon::create_struct_type),
      InstanceMethod("create_enum_type", &DuckDBNodeAddon::create_enum_type),
      InstanceMethod("create_decimal_type", &DuckDBNodeAddon::create_decimal_type),
      InstanceMethod("get_type_id", &DuckDBNodeAddon::get_type_id),
      InstanceMethod("decimal_width", &DuckDBNodeAddon::decimal_width),
      InstanceMethod("decimal_scale", &DuckDBNodeAddon::decimal_scale),
      InstanceMethod("decimal_internal_type", &DuckDBNodeAddon::decimal_internal_type),
      InstanceMethod("enum_internal_type", &DuckDBNodeAddon::enum_internal_type),
      InstanceMethod("enum_dictionary_size", &DuckDBNodeAddon::enum_dictionary_size),
      InstanceMethod("enum_dictionary_value", &DuckDBNodeAddon::enum_dictionary_value),
      InstanceMethod("list_type_child_type", &DuckDBNodeAddon::list_type_child_type),
      InstanceMethod("array_type_child_type", &DuckDBNodeAddon::array_type_child_type),
      InstanceMethod("array_type_array_size", &DuckDBNodeAddon::array_type_array_size),
      InstanceMethod("map_type_key_type", &DuckDBNodeAddon::map_type_key_type),
      InstanceMethod("map_type_value_type", &DuckDBNodeAddon::map_type_value_type),
      InstanceMethod("struct_type_child_count", &DuckDBNodeAddon::struct_type_child_count),
      InstanceMethod("struct_type_child_name", &DuckDBNodeAddon::struct_type_child_name),
      InstanceMethod("struct_type_child_type", &DuckDBNodeAddon::struct_type_child_type),
      InstanceMethod("union_type_member_count", &DuckDBNodeAddon::union_type_member_count),
      InstanceMethod("union_type_member_name", &DuckDBNodeAddon::union_type_member_name),
      InstanceMethod("union_type_member_type", &DuckDBNodeAddon::union_type_member_type),

      InstanceMethod("create_data_chunk", &DuckDBNodeAddon::create_data_chunk),
      InstanceMethod("data_chunk_reset", &DuckDBNodeAddon::data_chunk_reset),
      InstanceMethod("data_chunk_get_column_count", &DuckDBNodeAddon::data_chunk_get_column_count),
      InstanceMethod("data_chunk_get_vector", &DuckDBNodeAddon::data_chunk_get_vector),
      InstanceMethod("data_chunk_get_size", &DuckDBNodeAddon::data_chunk_get_size),
      InstanceMethod("data_chunk_set_size", &DuckDBNodeAddon::data_chunk_set_size),

      InstanceMethod("vector_get_column_type", &DuckDBNodeAddon::vector_get_column_type),
      InstanceMethod("vector_get_data", &DuckDBNodeAddon::vector_get_data),
      InstanceMethod("vector_get_validity", &DuckDBNodeAddon::vector_get_validity),
      InstanceMethod("vector_ensure_validity_writable", &DuckDBNodeAddon::vector_ensure_validity_writable),
      InstanceMethod("vector_assign_string_element", &DuckDBNodeAddon::vector_assign_string_element),
      InstanceMethod("vector_assign_string_element_len", &DuckDBNodeAddon::vector_assign_string_element_len),
      InstanceMethod("list_vector_get_child", &DuckDBNodeAddon::list_vector_get_child),
      InstanceMethod("list_vector_get_size", &DuckDBNodeAddon::list_vector_get_size),
      InstanceMethod("list_vector_set_size", &DuckDBNodeAddon::list_vector_set_size),
      InstanceMethod("list_vector_reserve", &DuckDBNodeAddon::list_vector_reserve),
      InstanceMethod("struct_vector_get_child", &DuckDBNodeAddon::struct_vector_get_child),
      InstanceMethod("array_vector_get_child", &DuckDBNodeAddon::array_vector_get_child),
      InstanceMethod("validity_row_is_valid", &DuckDBNodeAddon::validity_row_is_valid),
      InstanceMethod("validity_set_row_validity", &DuckDBNodeAddon::validity_set_row_validity),
      InstanceMethod("validity_set_row_invalid", &DuckDBNodeAddon::validity_set_row_invalid),
      InstanceMethod("validity_set_row_valid", &DuckDBNodeAddon::validity_set_row_valid),

      InstanceMethod("create_scalar_function", &DuckDBNodeAddon::create_scalar_function),
      InstanceMethod("destroy_scalar_function_sync", &DuckDBNodeAddon::destroy_scalar_function_sync),
      InstanceMethod("scalar_function_set_name", &DuckDBNodeAddon::scalar_function_set_name),
      InstanceMethod("scalar_function_set_varargs", &DuckDBNodeAddon::scalar_function_set_varargs),
      InstanceMethod("scalar_function_set_special_handling", &DuckDBNodeAddon::scalar_function_set_special_handling),
      InstanceMethod("scalar_function_set_volatile", &DuckDBNodeAddon::scalar_function_set_volatile),
      InstanceMethod("scalar_function_add_parameter", &DuckDBNodeAddon::scalar_function_add_parameter),
      InstanceMethod("scalar_function_set_return_type", &DuckDBNodeAddon::scalar_function_set_return_type),
      InstanceMethod("scalar_function_set_extra_info", &DuckDBNodeAddon::scalar_function_set_extra_info),
      InstanceMethod("scalar_function_set_function", &DuckDBNodeAddon::scalar_function_set_function),
      InstanceMethod("register_scalar_function", &DuckDBNodeAddon::register_scalar_function),
      InstanceMethod("scalar_function_get_extra_info", &DuckDBNodeAddon::scalar_function_get_extra_info),
      InstanceMethod("scalar_function_set_error", &DuckDBNodeAddon::scalar_function_set_error),

      InstanceMethod("appender_create", &DuckDBNodeAddon::appender_create),
      InstanceMethod("appender_create_ext", &DuckDBNodeAddon::appender_create_ext),
      InstanceMethod("appender_column_count", &DuckDBNodeAddon::appender_column_count),
      InstanceMethod("appender_column_type", &DuckDBNodeAddon::appender_column_type),
      InstanceMethod("appender_flush_sync", &DuckDBNodeAddon::appender_flush_sync),
      InstanceMethod("appender_close_sync", &DuckDBNodeAddon::appender_close_sync),
      InstanceMethod("appender_end_row", &DuckDBNodeAddon::appender_end_row),
      InstanceMethod("append_default", &DuckDBNodeAddon::append_default),
      InstanceMethod("append_bool", &DuckDBNodeAddon::append_bool),
      InstanceMethod("append_int8", &DuckDBNodeAddon::append_int8),
      InstanceMethod("append_int16", &DuckDBNodeAddon::append_int16),
      InstanceMethod("append_int32", &DuckDBNodeAddon::append_int32),
      InstanceMethod("append_int64", &DuckDBNodeAddon::append_int64),
      InstanceMethod("append_hugeint", &DuckDBNodeAddon::append_hugeint),
      InstanceMethod("append_uint8", &DuckDBNodeAddon::append_uint8),
      InstanceMethod("append_uint16", &DuckDBNodeAddon::append_uint16),
      InstanceMethod("append_uint32", &DuckDBNodeAddon::append_uint32),
      InstanceMethod("append_uint64", &DuckDBNodeAddon::append_uint64),
      InstanceMethod("append_uhugeint", &DuckDBNodeAddon::append_uhugeint),
      InstanceMethod("append_float", &DuckDBNodeAddon::append_float),
      InstanceMethod("append_double", &DuckDBNodeAddon::append_double),
      InstanceMethod("append_date", &DuckDBNodeAddon::append_date),
      InstanceMethod("append_time", &DuckDBNodeAddon::append_time),
      InstanceMethod("append_timestamp", &DuckDBNodeAddon::append_timestamp),
      InstanceMethod("append_interval", &DuckDBNodeAddon::append_interval),
      InstanceMethod("append_varchar", &DuckDBNodeAddon::append_varchar),
      InstanceMethod("append_blob", &DuckDBNodeAddon::append_blob),
      InstanceMethod("append_null", &DuckDBNodeAddon::append_null),
      InstanceMethod("append_value", &DuckDBNodeAddon::append_value),
      InstanceMethod("append_data_chunk", &DuckDBNodeAddon::append_data_chunk),

      InstanceMethod("fetch_chunk", &DuckDBNodeAddon::fetch_chunk),

      InstanceMethod("get_data_from_pointer", &DuckDBNodeAddon::get_data_from_pointer),
      InstanceMethod("copy_data_to_vector", &DuckDBNodeAddon::copy_data_to_vector),
      InstanceMethod("copy_data_to_vector_validity", &DuckDBNodeAddon::copy_data_to_vector_validity),
    });
  }

private:

  // DUCKDB_C_API duckdb_instance_cache duckdb_create_instance_cache();
  // function create_instance_cache(): InstanceCache
  Napi::Value create_instance_cache(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto instance_cache = duckdb_create_instance_cache();
    return CreateExternalForInstanceCache(env, instance_cache);
  }

  // DUCKDB_C_API duckdb_state duckdb_get_or_create_from_cache(duckdb_instance_cache instance_cache, const char *path, duckdb_database *out_database, duckdb_config config, char **out_error);
  // function get_or_create_from_cache(cache: InstanceCache, path?: string, config?: Config): Promise<Database>
  Napi::Value get_or_create_from_cache(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto instance_cache = GetInstanceCacheFromExternal(env, info[0]);
    auto pathValue = info[1];
    auto configValue = info[2];
    std::optional<std::string> path = std::nullopt;
    if (!pathValue.IsUndefined()) {
      path = pathValue.As<Napi::String>();
    }
    duckdb_config config = nullptr;
    if (!configValue.IsUndefined()) {
      config = GetConfigFromExternal(env, configValue);
    }
    auto worker = new GetOrCreateFromCacheWorker(env, instance_cache, path, config);
    worker->Queue();
    return worker->Promise();
  }

  // DUCKDB_C_API void duckdb_destroy_instance_cache(duckdb_instance_cache *instance_cache);
  // not exposed: destroyed in finalizer

  // DUCKDB_C_API duckdb_state duckdb_open(const char *path, duckdb_database *out_database);
  // function open(path?: string, config?: Config): Promise<Database>
  Napi::Value open(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto pathValue = info[0];
    auto configValue = info[1];
    std::optional<std::string> path = std::nullopt;
    if (!pathValue.IsUndefined()) {
      path = pathValue.As<Napi::String>();
    }
    duckdb_config config = nullptr;
    if (!configValue.IsUndefined()) {
      config = GetConfigFromExternal(env, configValue);
    }
    auto worker = new OpenWorker(env, path, config);
    worker->Queue();
    return worker->Promise();
  }

  // DUCKDB_C_API duckdb_state duckdb_open_ext(const char *path, duckdb_database *out_database, duckdb_config config, char **out_error);
  // not exposed: consolidated into open

  // DUCKDB_C_API void duckdb_close(duckdb_database *database);
  // function close(database: Database): void
  Napi::Value close_sync(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto database_holder_ptr = GetDatabaseHolderFromExternal(env, info[0]);
    // duckdb_close is a no-op if already closed
    duckdb_close(&database_holder_ptr->database);
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection);
  // function connect(database: Database): Promise<Connection>
  Napi::Value connect(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto database = GetDatabaseFromExternal(env, info[0]);
    auto worker = new ConnectWorker(env, database);
    worker->Queue();
    return worker->Promise();
  }

  // DUCKDB_C_API void duckdb_interrupt(duckdb_connection connection);
  // function interrupt(connection: Connection): void
  Napi::Value interrupt(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto connection = GetConnectionFromExternal(env, info[0]);
    duckdb_interrupt(connection);
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_query_progress_type duckdb_query_progress(duckdb_connection connection);
  // function query_progress(connection: Connection): QueryProgress
  Napi::Value query_progress(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto connection = GetConnectionFromExternal(env, info[0]);
    auto progress = duckdb_query_progress(connection);
    auto result = Napi::Object::New(env);
    result.Set("percentage", Napi::Number::New(env, progress.percentage));
    result.Set("rows_processed", Napi::BigInt::New(env, progress.rows_processed));
    result.Set("total_rows_to_process", Napi::BigInt::New(env, progress.total_rows_to_process));
    return result;
  }

  // DUCKDB_C_API void duckdb_disconnect(duckdb_connection *connection);
  // function disconnect(connection: Connection): void
  Napi::Value disconnect_sync(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto connection_holder_ptr = GetConnectionHolderFromExternal(env, info[0]);
    // duckdb_disconnect is a no-op if already disconnected
    duckdb_disconnect(&connection_holder_ptr->connection);
    return env.Undefined();
  }

  // DUCKDB_C_API void duckdb_connection_get_client_context(duckdb_connection connection, duckdb_client_context *out_context);
  // function connection_get_client_context(connection: Connection): ClientContext
  Napi::Value connection_get_client_context(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto connection = GetConnectionFromExternal(env, info[0]);
    duckdb_client_context client_context;
    duckdb_connection_get_client_context(connection, &client_context);
    if (!client_context) {
      throw Napi::Error::New(env, "Failed to get client context");
    }
    return CreateExternalForClientContext(env, client_context);
  }

  // DUCKDB_C_API void duckdb_connection_get_arrow_options(duckdb_connection connection, duckdb_arrow_options *out_arrow_options);
  // TODO arrow

  // DUCKDB_C_API idx_t duckdb_client_context_get_connection_id(duckdb_client_context context);
  // function client_context_get_connection_id(client_context: ClientContext): number
  Napi::Value client_context_get_connection_id(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto client_context = GetClientContextFromExternal(env, info[0]);
    auto id = duckdb_client_context_get_connection_id(client_context);
    return Napi::Number::New(env, id);
  }

  // DUCKDB_C_API void duckdb_destroy_client_context(duckdb_client_context *context);
  // not exposed: destroyed in finalizer

  // DUCKDB_C_API void duckdb_destroy_arrow_options(duckdb_arrow_options *arrow_options);
  // TODO arrow

  // DUCKDB_C_API const char *duckdb_library_version();
  // function library_version(): string
  Napi::Value library_version(const Napi::CallbackInfo& info) {
    return Napi::String::New(info.Env(), duckdb_library_version());
  }

  // DUCKDB_C_API duckdb_value duckdb_get_table_names(duckdb_connection connection, const char *query, bool qualified);
  // function get_table_names(connection: Connection, query: string, qualified: boolean): Value
  Napi::Value get_table_names(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto connection = GetConnectionFromExternal(env, info[0]);
    std::string query = info[1].As<Napi::String>();
    auto qualified = info[2].As<Napi::Boolean>();
    auto value = duckdb_get_table_names(connection, query.c_str(), qualified);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_state duckdb_create_config(duckdb_config *out_config);
  // function create_config(): Config
  Napi::Value create_config(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    duckdb_config config;
    if (duckdb_create_config(&config)) {
      duckdb_destroy_config(&config);
      throw Napi::Error::New(env, "Failed to create config");
    }
    // Set the default duckdb_api value for the bindings. Can be overridden.
    if (duckdb_set_config(config, "duckdb_api", DEFAULT_DUCKDB_API)) {
      throw Napi::Error::New(env, "Failed to set duckdb_api");
    }
    return CreateExternalForConfig(env, config);
  }

  // DUCKDB_C_API size_t duckdb_config_count();
  // function config_count(): number
  Napi::Value config_count(const Napi::CallbackInfo& info) {
    return Napi::Number::New(info.Env(), duckdb_config_count());
  }

  // DUCKDB_C_API duckdb_state duckdb_get_config_flag(size_t index, const char **out_name, const char **out_description);
  // function get_config_flag(index: number): ConfigFlag
  Napi::Value get_config_flag(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto index = info[0].As<Napi::Number>().Uint32Value();
    const char *name;
    const char *description;
    if (duckdb_get_config_flag(index, &name, &description)) {
      throw Napi::Error::New(env, "Config option not found");
    }
    auto config_flag = Napi::Object::New(env);
    config_flag.Set("name", name);
    config_flag.Set("description", description);
    return config_flag;
  }

  // DUCKDB_C_API duckdb_state duckdb_set_config(duckdb_config config, const char *name, const char *option);
  // function set_config(config: Config, name: string, option: string): void
  Napi::Value set_config(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto config = GetConfigFromExternal(env, info[0]);
    std::string name = info[1].As<Napi::String>();
    std::string option = info[2].As<Napi::String>();
    if (duckdb_set_config(config, name.c_str(), option.c_str())) {
      throw Napi::Error::New(env, "Failed to set config");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API void duckdb_destroy_config(duckdb_config *config);
  // not exposed: destroyed in finalizer

  // DUCKDB_C_API duckdb_error_data duckdb_create_error_data(duckdb_error_type type, const char *message);
  // TODO error data

  // DUCKDB_C_API void duckdb_destroy_error_data(duckdb_error_data *error_data);
  // TODO error data

  // DUCKDB_C_API duckdb_error_type duckdb_error_data_error_type(duckdb_error_data error_data);
  // TODO error data

  // DUCKDB_C_API const char *duckdb_error_data_message(duckdb_error_data error_data);
  // TODO error data

  // DUCKDB_C_API bool duckdb_error_data_has_error(duckdb_error_data error_data);
  // TODO error data

  // DUCKDB_C_API duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out_result);
  // function query(connection: Connection, query: string): Promise<Result>
  Napi::Value query(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto connection = GetConnectionFromExternal(env, info[0]);
    std::string query = info[1].As<Napi::String>();
    auto worker = new QueryWorker(env, connection, query);
    worker->Queue();
    return worker->Promise();
  }


  // DUCKDB_C_API void duckdb_destroy_result(duckdb_result *result);
  // not exposed: destroyed in finalizer

  // DUCKDB_C_API const char *duckdb_column_name(duckdb_result *result, idx_t col);
  // function column_name(result: Result, column_index: number): string
  Napi::Value column_name(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto column_index = info[1].As<Napi::Number>().Uint32Value();
    auto column_name = duckdb_column_name(result_ptr, column_index);
    return Napi::String::New(env, column_name);
  }

  // DUCKDB_C_API duckdb_type duckdb_column_type(duckdb_result *result, idx_t col);
  // function column_type(result: Result, column_index: number): Type
  Napi::Value column_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto column_index = info[1].As<Napi::Number>().Uint32Value();
    auto column_type = duckdb_column_type(result_ptr, column_index);
    return Napi::Number::New(env, column_type);
  }

  // DUCKDB_C_API duckdb_statement_type duckdb_result_statement_type(duckdb_result result);
  // function result_statement_type(result: Result): StatementType
  Napi::Value result_statement_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto statement_type = duckdb_result_statement_type(*result_ptr);
    return Napi::Number::New(env, statement_type);
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_column_logical_type(duckdb_result *result, idx_t col);
  // function column_logical_type(result: Result, column_index: number): LogicalType
  Napi::Value column_logical_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto column_index = info[1].As<Napi::Number>().Uint32Value();
    auto column_logical_type = duckdb_column_logical_type(result_ptr, column_index);
    return CreateExternalForLogicalType(env, column_logical_type);
  }

  // DUCKDB_C_API duckdb_arrow_options duckdb_result_get_arrow_options(duckdb_result *result);
  // TODO arrow

  // DUCKDB_C_API idx_t duckdb_column_count(duckdb_result *result);
  // function column_count(result: Result): number
  Napi::Value column_count(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto column_count = duckdb_column_count(result_ptr);
    return Napi::Number::New(env, column_count);
  }

  // #ifndef DUCKDB_API_NO_DEPRECATED

  // DUCKDB_C_API idx_t duckdb_row_count(duckdb_result *result);
  // function row_count(result: Result): number
  Napi::Value row_count(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto row_count = duckdb_row_count(result_ptr);
    return Napi::Number::New(env, row_count);
  }

  // #endif

  // DUCKDB_C_API idx_t duckdb_rows_changed(duckdb_result *result);
  // function rows_changed(result: Result): number
  Napi::Value rows_changed(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto rows_changed = duckdb_rows_changed(result_ptr);
    return Napi::Number::New(env, rows_changed);
  }

  // #ifndef DUCKDB_API_NO_DEPRECATED

  // DUCKDB_C_API void *duckdb_column_data(duckdb_result *result, idx_t col);
  // deprecated

  // DUCKDB_C_API bool *duckdb_nullmask_data(duckdb_result *result, idx_t col);
  // deprecated

  // #endif

  // DUCKDB_C_API const char *duckdb_result_error(duckdb_result *result);
  // not exposed: query, execute_prepared, and execute_pending reject promise with error

  // DUCKDB_C_API duckdb_error_type duckdb_result_error_type(duckdb_result *result);
  // not exposed: query, execute_prepared, and execute_pending reject promise with error

  // #ifndef DUCKDB_API_NO_DEPRECATED

  // DUCKDB_C_API duckdb_data_chunk duckdb_result_get_chunk(duckdb_result result, idx_t chunk_index);
  // function result_get_chunk(result: Result, chunkIndex: number): DataChunk
  Napi::Value result_get_chunk(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto chunk_index = info[1].As<Napi::Number>().Uint32Value();
    auto chunk = duckdb_result_get_chunk(*result_ptr, chunk_index);
    if (!chunk) {
      throw Napi::Error::New(env, "Failed to get data chunk. Only supported for materialized results.");
    }
    return CreateExternalForDataChunk(env, chunk);
  }

  // DUCKDB_C_API bool duckdb_result_is_streaming(duckdb_result result);
  // function result_is_streaming(result: Result): boolean
  Napi::Value result_is_streaming(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto is_streaming = duckdb_result_is_streaming(*result_ptr);
    return Napi::Boolean::New(env, is_streaming);
  }

  // DUCKDB_C_API idx_t duckdb_result_chunk_count(duckdb_result result);
  // function result_chunk_count(result: Result): number
  Napi::Value result_chunk_count(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto chunk_count = duckdb_result_chunk_count(*result_ptr);
    return Napi::Number::New(env, chunk_count);
  }

  // #endif

  // DUCKDB_C_API duckdb_result_type duckdb_result_return_type(duckdb_result result);
  // function result_return_type(result: Result): ResultType
  Napi::Value result_return_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto result_type = duckdb_result_return_type(*result_ptr);
    return Napi::Number::New(env, result_type);
  }

  // #ifndef DUCKDB_API_NO_DEPRECATED

  // DUCKDB_C_API bool duckdb_value_boolean(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API int8_t duckdb_value_int8(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API int16_t duckdb_value_int16(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API int32_t duckdb_value_int32(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API int64_t duckdb_value_int64(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API duckdb_hugeint duckdb_value_hugeint(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API duckdb_uhugeint duckdb_value_uhugeint(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API duckdb_decimal duckdb_value_decimal(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API uint8_t duckdb_value_uint8(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API uint16_t duckdb_value_uint16(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API uint32_t duckdb_value_uint32(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API uint64_t duckdb_value_uint64(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API float duckdb_value_float(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API double duckdb_value_double(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API duckdb_date duckdb_value_date(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API duckdb_time duckdb_value_time(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API duckdb_timestamp duckdb_value_timestamp(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API duckdb_interval duckdb_value_interval(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API char *duckdb_value_varchar(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API duckdb_string duckdb_value_string(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API char *duckdb_value_varchar_internal(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API duckdb_string duckdb_value_string_internal(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API duckdb_blob duckdb_value_blob(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // DUCKDB_C_API bool duckdb_value_is_null(duckdb_result *result, idx_t col, idx_t row);
  // deprecated

  // #endif

  // DUCKDB_C_API void *duckdb_malloc(size_t size);
  // not exposed: only used internally

  // DUCKDB_C_API void duckdb_free(void *ptr);
  // not exposed: only used internally

  // DUCKDB_C_API idx_t duckdb_vector_size();
  // function vector_size(): number
  Napi::Value vector_size(const Napi::CallbackInfo& info) {
    return Napi::Number::New(info.Env(), duckdb_vector_size());
  }

  // DUCKDB_C_API bool duckdb_string_is_inlined(duckdb_string_t string);
  // not exposed: handled internally

  // DUCKDB_C_API uint32_t duckdb_string_t_length(duckdb_string_t string);
  // not exposed: handled internally

  // DUCKDB_C_API const char *duckdb_string_t_data(duckdb_string_t *string);
  // not exposed: handled internally

  // DUCKDB_C_API duckdb_date_struct duckdb_from_date(duckdb_date date);
  // function from_date(date: Date_): DateParts
  Napi::Value from_date(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto date_obj = info[0].As<Napi::Object>();
    auto date = GetDateFromObject(date_obj);
    auto date_parts = duckdb_from_date(date);
    return MakeDatePartsObject(env, date_parts);
  }

  // DUCKDB_C_API duckdb_date duckdb_to_date(duckdb_date_struct date);
  // function to_date(parts: DateParts): Date_
  Napi::Value to_date(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto date_parts_obj = info[0].As<Napi::Object>();
    auto date_parts = GetDatePartsFromObject(date_parts_obj);
    auto date = duckdb_to_date(date_parts);
    return MakeDateObject(env, date);
  }

  // DUCKDB_C_API bool duckdb_is_finite_date(duckdb_date date);
  // function is_finite_date(date: Date_): boolean
  Napi::Value is_finite_date(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto date_obj = info[0].As<Napi::Object>();
    auto date = GetDateFromObject(date_obj);
    auto is_finite = duckdb_is_finite_date(date);
    return Napi::Boolean::New(env, is_finite);
  }

  // DUCKDB_C_API duckdb_time_struct duckdb_from_time(duckdb_time time);
  // function from_time(time: Time): TimeParts
  Napi::Value from_time(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto time_obj = info[0].As<Napi::Object>();
    auto time = GetTimeFromObject(env, time_obj);
    auto time_parts = duckdb_from_time(time);
    return MakeTimePartsObject(env, time_parts);
  }

  // DUCKDB_C_API duckdb_time_tz duckdb_create_time_tz(int64_t micros, int32_t offset);
  // function create_time_tz(micros: number, offset: number): TimeTZ
  Napi::Value create_time_tz(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto micros = info[0].As<Napi::Number>().Int64Value();
    auto offset = info[1].As<Napi::Number>().Int32Value();
    auto time_tz = duckdb_create_time_tz(micros, offset);
    return MakeTimeTZObject(env, time_tz);
  }

  // DUCKDB_C_API duckdb_time_tz_struct duckdb_from_time_tz(duckdb_time_tz micros);
  // function from_time_tz(time_tz: TimeTZ): TimeTZParts
  Napi::Value from_time_tz(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto time_tz_obj = info[0].As<Napi::Object>();
    auto time_tz = GetTimeTZFromObject(env, time_tz_obj);
    auto time_tz_parts = duckdb_from_time_tz(time_tz);
    return MakeTimeTZPartsObject(env, time_tz_parts);
  }

  // DUCKDB_C_API duckdb_time duckdb_to_time(duckdb_time_struct time);
  // function to_time(parts: TimeParts): Time
  Napi::Value to_time(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto time_parts_obj = info[0].As<Napi::Object>();
    auto time_parts = GetTimePartsFromObject(time_parts_obj);
    auto time = duckdb_to_time(time_parts);
    return MakeTimeObject(env, time);
  }

  // DUCKDB_C_API duckdb_timestamp_struct duckdb_from_timestamp(duckdb_timestamp ts);
  // function from_timestamp(timestamp: Timestamp): TimestampParts
  Napi::Value from_timestamp(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto timestamp_obj = info[0].As<Napi::Object>();
    auto timestamp = GetTimestampFromObject(env, timestamp_obj);
    auto timestamp_parts = duckdb_from_timestamp(timestamp);
    return MakeTimestampPartsObject(env, timestamp_parts);
  }

  // DUCKDB_C_API duckdb_timestamp duckdb_to_timestamp(duckdb_timestamp_struct ts);
  // function to_timestamp(parts: TimestampParts): Timestamp
  Napi::Value to_timestamp(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto timestamp_parts_obj = info[0].As<Napi::Object>();
    auto timestamp_parts = GetTimestampPartsFromObject(timestamp_parts_obj);
    auto timestamp = duckdb_to_timestamp(timestamp_parts);
    return MakeTimestampObject(env, timestamp);
  }

  // DUCKDB_C_API bool duckdb_is_finite_timestamp(duckdb_timestamp ts);
  // function is_finite_timestamp(timestamp: Timestamp): boolean
  Napi::Value is_finite_timestamp(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto timestamp_obj = info[0].As<Napi::Object>();
    auto timestamp = GetTimestampFromObject(env, timestamp_obj);
    auto is_finite = duckdb_is_finite_timestamp(timestamp);
    return Napi::Boolean::New(env, is_finite);
  }

  // DUCKDB_C_API bool duckdb_is_finite_timestamp_s(duckdb_timestamp_s ts);
  // function is_finite_timestamp_s(timestampSeconds: TimestampSeconds): boolean
  Napi::Value is_finite_timestamp_s(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto timestamp_s_obj = info[0].As<Napi::Object>();
    auto timestamp_s = GetTimestampSecondsFromObject(env, timestamp_s_obj);
    auto is_finite = duckdb_is_finite_timestamp_s(timestamp_s);
    return Napi::Boolean::New(env, is_finite);
  }

  // DUCKDB_C_API bool duckdb_is_finite_timestamp_ms(duckdb_timestamp_ms ts);
  // function is_finite_timestamp_ms(timestampMilliseconds: TimestampMilliseconds): boolean
  Napi::Value is_finite_timestamp_ms(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto timestamp_ms_obj = info[0].As<Napi::Object>();
    auto timestamp_ms = GetTimestampMillisecondsFromObject(env, timestamp_ms_obj);
    auto is_finite = duckdb_is_finite_timestamp_ms(timestamp_ms);
    return Napi::Boolean::New(env, is_finite);
  }

  // DUCKDB_C_API bool duckdb_is_finite_timestamp_ns(duckdb_timestamp_ns ts);
  // function is_finite_timestamp_ns(timestampNanoseconds: TimestampNanoseconds): boolean
  Napi::Value is_finite_timestamp_ns(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto timestamp_ns_obj = info[0].As<Napi::Object>();
    auto timestamp_ns = GetTimestampNanosecondsFromObject(env, timestamp_ns_obj);
    auto is_finite = duckdb_is_finite_timestamp_ns(timestamp_ns);
    return Napi::Boolean::New(env, is_finite);
  }

  // DUCKDB_C_API double duckdb_hugeint_to_double(duckdb_hugeint val);
  // function hugeint_to_double(hugeint: bigint): number
  Napi::Value hugeint_to_double(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto bigint = info[0].As<Napi::BigInt>();
    auto hugeint = GetHugeIntFromBigInt(env, bigint);
    auto output_double = duckdb_hugeint_to_double(hugeint);
    return Napi::Number::New(env, output_double);
  }

  // DUCKDB_C_API duckdb_hugeint duckdb_double_to_hugeint(double val);
  // function double_to_hugeint(double: number): bigint
  Napi::Value double_to_hugeint(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input_double = info[0].As<Napi::Number>().DoubleValue();
    auto hugeint = duckdb_double_to_hugeint(input_double);
    return MakeBigIntFromHugeInt(env, hugeint);
  }

  // DUCKDB_C_API double duckdb_uhugeint_to_double(duckdb_uhugeint val);
  // function uhugeint_to_double(uhugeint: bigint): number
  Napi::Value uhugeint_to_double(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto bigint = info[0].As<Napi::BigInt>();
    auto uhugeint = GetUHugeIntFromBigInt(env, bigint);
    auto output_double = duckdb_uhugeint_to_double(uhugeint);
    return Napi::Number::New(env, output_double);
  }

  // DUCKDB_C_API duckdb_uhugeint duckdb_double_to_uhugeint(double val);
  // function double_to_uhugeint(double: number): bigint
  Napi::Value double_to_uhugeint(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input_double = info[0].As<Napi::Number>().DoubleValue();
    auto uhugeint = duckdb_double_to_uhugeint(input_double);
    return MakeBigIntFromUHugeInt(env, uhugeint);
  }

  // DUCKDB_C_API duckdb_decimal duckdb_double_to_decimal(double val, uint8_t width, uint8_t scale);
  // function double_to_decimal(double: number, width: number, scale: number): Decimal
  Napi::Value double_to_decimal(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input_double = info[0].As<Napi::Number>().DoubleValue();
    auto width = info[1].As<Napi::Number>().Uint32Value();
    auto scale = info[2].As<Napi::Number>().Uint32Value();
    auto decimal = duckdb_double_to_decimal(input_double, width, scale);
    return MakeDecimalObject(env, decimal);
  }

  // DUCKDB_C_API double duckdb_decimal_to_double(duckdb_decimal val);
  // function decimal_to_double(decimal: Decimal): number
  Napi::Value decimal_to_double(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto decimal_obj = info[0].As<Napi::Object>();
    auto decimal = GetDecimalFromObject(env, decimal_obj);
    auto output_double = duckdb_decimal_to_double(decimal);
    return Napi::Number::New(env, output_double);
  }

  // DUCKDB_C_API duckdb_state duckdb_prepare(duckdb_connection connection, const char *query, duckdb_prepared_statement *out_prepared_statement);
  // function prepare(connection: Connection, query: string): Promise<PreparedStatement>
  Napi::Value prepare(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto connection = GetConnectionFromExternal(env, info[0]);
    std::string query = info[1].As<Napi::String>();
    auto worker = new PrepareWorker(env, connection, query);
    worker->Queue();
    return worker->Promise();
  }

  // DUCKDB_C_API void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement);
  // function destroy_prepare_sync(prepared_statement: PreparedStatement): void
  Napi::Value destroy_prepare_sync(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement_holder_ptr = GetPreparedStatementHolderFromExternal(env, info[0]);
    // duckdb_destroy_prepare is a no-op if already destroyed
    duckdb_destroy_prepare(&prepared_statement_holder_ptr->prepared);
    return env.Undefined();
  }

  // DUCKDB_C_API const char *duckdb_prepare_error(duckdb_prepared_statement prepared_statement);
  // not exposed: prepare rejects promise with error

  // DUCKDB_C_API idx_t duckdb_nparams(duckdb_prepared_statement prepared_statement);
  // function nparams(prepared_statement: PreparedStatement): number
  Napi::Value nparams(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto nparams = duckdb_nparams(prepared_statement);
    return Napi::Number::New(env, nparams);
  }

  // DUCKDB_C_API const char *duckdb_parameter_name(duckdb_prepared_statement prepared_statement, idx_t index);
  // function parameter_name(prepared_statement: PreparedStatement, index: number): string
  Napi::Value parameter_name(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto parameter_name = duckdb_parameter_name(prepared_statement, index);
    auto str = Napi::String::New(env, parameter_name);
    duckdb_free((void *)parameter_name);
    return str;
  }

  // DUCKDB_C_API duckdb_type duckdb_param_type(duckdb_prepared_statement prepared_statement, idx_t param_idx);
  // function param_type(prepared_statement: PreparedStatement, index: number): Type
  Napi::Value param_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto type = duckdb_param_type(prepared_statement, index);
    return Napi::Number::New(env, type);
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_param_logical_type(duckdb_prepared_statement prepared_statement, idx_t param_idx);
  // function param_logical_type(prepared_statement: PreparedStatement, index: number): LogicalType
  Napi::Value param_logical_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto logical_type = duckdb_param_logical_type(prepared_statement, index);
    if (!logical_type) {
      throw Napi::Error::New(env, "Failed to get param logical type");
    }
    return CreateExternalForLogicalType(env, logical_type);
  }

  // DUCKDB_C_API duckdb_state duckdb_clear_bindings(duckdb_prepared_statement prepared_statement);
  // function clear_bindings(prepared_statement: PreparedStatement): void
  Napi::Value clear_bindings(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    if (duckdb_clear_bindings(prepared_statement)) {
      throw Napi::Error::New(env, "Failed to clear bindings");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_statement_type duckdb_prepared_statement_type(duckdb_prepared_statement statement);
  // function prepared_statement_type(prepared_statement: PreparedStatement): StatementType
  Napi::Value prepared_statement_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto statement_type = duckdb_prepared_statement_type(prepared_statement);
    return Napi::Number::New(env, statement_type);
  }

  // DUCKDB_C_API idx_t duckdb_prepared_statement_column_count(duckdb_prepared_statement prepared_statement);
  // function prepared_statement_column_count(prepared_statement: PreparedStatement): number
  Napi::Value prepared_statement_column_count(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto column_count = duckdb_prepared_statement_column_count(prepared_statement);
    return Napi::Number::New(env, column_count);
  }

  // DUCKDB_C_API const char *duckdb_prepared_statement_column_name(duckdb_prepared_statement prepared_statement, idx_t col_idx);
  // function prepared_statement_column_name(prepared_statement: PreparedStatement, index: number): string
  Napi::Value prepared_statement_column_name(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto column_name = duckdb_prepared_statement_column_name(prepared_statement, index);
    if (!column_name) {
      throw Napi::Error::New(env, "Failed to get prepared statement column name");
    }
    auto column_name_str = Napi::String::New(env, column_name);
    duckdb_free((void *)column_name);
    return column_name_str;
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_prepared_statement_column_logical_type(duckdb_prepared_statement prepared_statement, idx_t col_idx);
  // function prepared_statement_column_logical_type(prepared_statement: PreparedStatement, index: number): LogicalType
  Napi::Value prepared_statement_column_logical_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto logical_type = duckdb_prepared_statement_column_logical_type(prepared_statement, index);
    if (!logical_type) {
      throw Napi::Error::New(env, "Failed to get prepared statement column logical type");
    }
    return CreateExternalForLogicalType(env, logical_type);
  }

  // DUCKDB_C_API duckdb_type duckdb_prepared_statement_column_type(duckdb_prepared_statement prepared_statement, idx_t col_idx);
  // function prepared_statement_column_type(prepared_statement: PreparedStatement, index: number): Type
  Napi::Value prepared_statement_column_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto type = duckdb_prepared_statement_column_type(prepared_statement, index);
    return Napi::Number::New(env, type);
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_value(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_value val);
  // function bind_value(prepared_statement: PreparedStatement, index: number, value: Value): void
  Napi::Value bind_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = GetValueFromExternal(env, info[2]);
    if (duckdb_bind_value(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind value");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_parameter_index(duckdb_prepared_statement prepared_statement, idx_t *param_idx_out, const char *name);
  // function bind_parameter_index(prepared_statement: PreparedStatement, name: string): number
  Napi::Value bind_parameter_index(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    std::string name = info[1].As<Napi::String>();
    idx_t param_index;
    if (duckdb_bind_parameter_index(prepared_statement, &param_index, name.c_str())) {
      throw Napi::Error::New(env, "Failed to retrieve bind parameter index");
    }
    return Napi::Number::New(env, param_index);
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val);
  // function bind_boolean(prepared_statement: PreparedStatement, index: number, bool: boolean): void
  Napi::Value bind_boolean(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = info[2].As<Napi::Boolean>();
    if (duckdb_bind_boolean(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind boolean");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val);
  // function bind_int8(prepared_statement: PreparedStatement, index: number, int8: number): void
  Napi::Value bind_int8(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = info[2].As<Napi::Number>().Int32Value();
    if (duckdb_bind_int8(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind int8");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val);
  // function bind_int16(prepared_statement: PreparedStatement, index: number, int16: number): void
  Napi::Value bind_int16(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = info[2].As<Napi::Number>().Int32Value();
    if (duckdb_bind_int16(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind int16");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t val);
  // function bind_int32(prepared_statement: PreparedStatement, index: number, int32: number): void
  Napi::Value bind_int32(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = info[2].As<Napi::Number>().Int32Value();
    if (duckdb_bind_int32(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind int32");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t val);
  // function bind_int64(prepared_statement: PreparedStatement, index: number, int64: bigint): void
  Napi::Value bind_int64(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    bool lossless;
    auto value = info[2].As<Napi::BigInt>().Int64Value(&lossless);
    if (!lossless) {
      throw Napi::Error::New(env, "bigint out of int64 range");
    }
    if (duckdb_bind_int64(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind int64");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_hugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_hugeint val);
  // function bind_hugeint(prepared_statement: PreparedStatement, index: number, hugeint: bigint): void
  Napi::Value bind_hugeint(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = GetHugeIntFromBigInt(env, info[2].As<Napi::BigInt>());
    if (duckdb_bind_hugeint(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind hugeint");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_uhugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_uhugeint val);
  // function bind_uhugeint(prepared_statement: PreparedStatement, index: number, uhugeint: bigint): void
  Napi::Value bind_uhugeint(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = GetUHugeIntFromBigInt(env, info[2].As<Napi::BigInt>());
    if (duckdb_bind_uhugeint(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind uhugeint");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_decimal(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_decimal val);
  // function bind_decimal(prepared_statement: PreparedStatement, index: number, decimal: Decimal): void
  Napi::Value bind_decimal(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = GetDecimalFromObject(env, info[2].As<Napi::Object>());
    if (duckdb_bind_decimal(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind decimal");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_uint8(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint8_t val);
  // function bind_uint8(prepared_statement: PreparedStatement, index: number, uint8: number): void
  Napi::Value bind_uint8(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = info[2].As<Napi::Number>().Uint32Value();
    if (duckdb_bind_uint8(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind uint8");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_uint16(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint16_t val);
  // function bind_uint16(prepared_statement: PreparedStatement, index: number, uint16: number): void
  Napi::Value bind_uint16(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = info[2].As<Napi::Number>().Uint32Value();
    if (duckdb_bind_uint16(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind uint16");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_uint32(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint32_t val);
  // function bind_uint32(prepared_statement: PreparedStatement, index: number, uint32: number): void
  Napi::Value bind_uint32(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = info[2].As<Napi::Number>().Uint32Value();
    if (duckdb_bind_uint32(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind uint32");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_uint64(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint64_t val);
  // function bind_uint64(prepared_statement: PreparedStatement, index: number, uint64: bigint): void
  Napi::Value bind_uint64(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    bool lossless;
    auto value = info[2].As<Napi::BigInt>().Uint64Value(&lossless);
    if (!lossless) {
      throw Napi::Error::New(env, "bigint out of uint64 range");
    }
    if (duckdb_bind_uint64(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind uint64");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val);
  // function bind_float(prepared_statement: PreparedStatement, index: number, float: number): void
  Napi::Value bind_float(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = info[2].As<Napi::Number>().FloatValue();
    if (duckdb_bind_float(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind float");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val);
  // function bind_double(prepared_statement: PreparedStatement, index: number, double: number): void
  Napi::Value bind_double(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = info[2].As<Napi::Number>().DoubleValue();
    if (duckdb_bind_double(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind double");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_date(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_date val);
  // function bind_date(prepared_statement: PreparedStatement, index: number, date: Date_): void
  Napi::Value bind_date(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = GetDateFromObject(info[2].As<Napi::Object>());
    if (duckdb_bind_date(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind date");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_time(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_time val);
  // function bind_time(prepared_statement: PreparedStatement, index: number, time: Time): void
  Napi::Value bind_time(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = GetTimeFromObject(env, info[2].As<Napi::Object>());
    if (duckdb_bind_time(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind time");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_timestamp(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_timestamp val);
  // function bind_timestamp(prepared_statement: PreparedStatement, index: number, timestamp: Timestamp): void
  Napi::Value bind_timestamp(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = GetTimestampFromObject(env, info[2].As<Napi::Object>());
    if (duckdb_bind_timestamp(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind timestamp");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_timestamp_tz(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_timestamp val);
  // function bind_timestamp_tz(prepared_statement: PreparedStatement, index: number, timestamp: Timestamp): void
  Napi::Value bind_timestamp_tz(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = GetTimestampFromObject(env, info[2].As<Napi::Object>());
    if (duckdb_bind_timestamp_tz(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind timestamp_tz");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_interval(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_interval val);
  // function bind_interval(prepared_statement: PreparedStatement, index: number, interval: Interval): void
  Napi::Value bind_interval(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = GetIntervalFromObject(env, info[2].As<Napi::Object>());
    if (duckdb_bind_interval(prepared_statement, index, value)) {
      throw Napi::Error::New(env, "Failed to bind interval");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val);
  // function bind_varchar(prepared_statement: PreparedStatement, index: number, varchar: string): void
  Napi::Value bind_varchar(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    std::string value = info[2].As<Napi::String>();
    if (duckdb_bind_varchar_length(prepared_statement, index, value.c_str(), value.size())) {
      throw Napi::Error::New(env, "Failed to bind varchar");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_varchar_length(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val, idx_t length);
  // not exposed: JS string includes length

  // DUCKDB_C_API duckdb_state duckdb_bind_blob(duckdb_prepared_statement prepared_statement, idx_t param_idx, const void *data, idx_t length);
  // function bind_blob(prepared_statement: PreparedStatement, index: number, data: Uint8Array): void
  Napi::Value bind_blob(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto array = info[2].As<Napi::Uint8Array>();
    auto data = reinterpret_cast<void*>(array.Data());
    auto length = array.ByteLength();
    if (duckdb_bind_blob(prepared_statement, index, data, length)) {
      throw Napi::Error::New(env, "Failed to bind blob");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx);
  // function bind_null(prepared_statement: PreparedStatement, index: number): void
  Napi::Value bind_null(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    if (duckdb_bind_null(prepared_statement, index)) {
      throw Napi::Error::New(env, "Failed to bind null");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement, duckdb_result *out_result);
  // function execute_prepared(prepared_statement: PreparedStatement): Promise<Result>
  Napi::Value execute_prepared(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto worker = new ExecutePreparedWorker(env, prepared_statement);
    worker->Queue();
    return worker->Promise();
  }

  // #ifndef DUCKDB_API_NO_DEPRECATED

  // DUCKDB_C_API duckdb_state duckdb_execute_prepared_streaming(duckdb_prepared_statement prepared_statement, duckdb_result *out_result);
  // function execute_prepared_streaming(prepared_statement: PreparedStatement): Promise<Result>
  Napi::Value execute_prepared_streaming(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    auto worker = new ExecutePreparedStreamingWorker(env, prepared_statement);
    worker->Queue();
    return worker->Promise();
  }

  // #endif

  // DUCKDB_C_API idx_t duckdb_extract_statements(duckdb_connection connection, const char *query, duckdb_extracted_statements *out_extracted_statements);
  // function extract_statements(connection: Connection, query: string): Promise<ExtractedStatementsAndCount>
  Napi::Value extract_statements(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto connection = GetConnectionFromExternal(env, info[0]);
    std::string query = info[1].As<Napi::String>();
    auto worker = new ExtractStatementsWorker(env, connection, query);
    worker->Queue();
    return worker->Promise();
  }

  // DUCKDB_C_API duckdb_state duckdb_prepare_extracted_statement(duckdb_connection connection, duckdb_extracted_statements extracted_statements, idx_t index, duckdb_prepared_statement *out_prepared_statement);
  // function prepare_extracted_statement(connection: Connection, extracted_statements: ExtractedStatements, index: number): Promise<PreparedStatement>
  Napi::Value prepare_extracted_statement(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto connection = GetConnectionFromExternal(env, info[0]);
    auto extracted_statements = GetExtractedStatementsFromExternal(env, info[1]);
    auto index = info[2].As<Napi::Number>().Uint32Value();
    auto worker = new PrepareExtractedStatementWorker(env, connection, extracted_statements, index);
    worker->Queue();
    return worker->Promise();
  }

  // DUCKDB_C_API const char *duckdb_extract_statements_error(duckdb_extracted_statements extracted_statements);
  // function extract_statements_error(extracted_statements: ExtractedStatements): string
  Napi::Value extract_statements_error(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto extracted_statements = GetExtractedStatementsFromExternal(env, info[0]);
    auto str = duckdb_extract_statements_error(extracted_statements);
    return Napi::String::New(env, str);
  }

  // DUCKDB_C_API void duckdb_destroy_extracted(duckdb_extracted_statements *extracted_statements);
  // not exposed: destroyed in finalizer

  // DUCKDB_C_API duckdb_state duckdb_pending_prepared(duckdb_prepared_statement prepared_statement, duckdb_pending_result *out_result);
  // function pending_prepared(prepared_statement: PreparedStatement): PendingResult
  Napi::Value pending_prepared(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    duckdb_pending_result pending_result;
    if (duckdb_pending_prepared(prepared_statement, &pending_result)) {
      std::string error = duckdb_pending_error(pending_result);
      duckdb_destroy_pending(&pending_result);
      throw Napi::Error::New(env, error);
    }
    return CreateExternalForPendingResult(env, pending_result);
  }

  // #ifndef DUCKDB_API_NO_DEPRECATED

  // DUCKDB_C_API duckdb_state duckdb_pending_prepared_streaming(duckdb_prepared_statement prepared_statement, duckdb_pending_result *out_result);
  // function pending_prepared_streaming(prepared_statement: PreparedStatement): PendingResult
  Napi::Value pending_prepared_streaming(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto prepared_statement = GetPreparedStatementFromExternal(env, info[0]);
    duckdb_pending_result pending_result;
    if (duckdb_pending_prepared_streaming(prepared_statement, &pending_result)) {
      std::string error = duckdb_pending_error(pending_result);
      duckdb_destroy_pending(&pending_result);
      throw Napi::Error::New(env, error);
    }
    return CreateExternalForPendingResult(env, pending_result);
  }

  // #endif

  // DUCKDB_C_API void duckdb_destroy_pending(duckdb_pending_result *pending_result);
  // not exposed: destroyed in finalizer

  // DUCKDB_C_API const char *duckdb_pending_error(duckdb_pending_result pending_result);
  // function pending_error(pending_result: PendingResult): string
  Napi::Value pending_error(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto pending_result = GetPendingResultFromExternal(env, info[0]);
    auto error = duckdb_pending_error(pending_result);
    return Napi::String::New(env, error);
  }

  // DUCKDB_C_API duckdb_pending_state duckdb_pending_execute_task(duckdb_pending_result pending_result);
  // function pending_execute_task(pending_result: PendingResult): PendingState
  Napi::Value pending_execute_task(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto pending_result = GetPendingResultFromExternal(env, info[0]);
    auto pending_state = duckdb_pending_execute_task(pending_result);
    return Napi::Number::New(env, pending_state);
  }

  // DUCKDB_C_API duckdb_pending_state duckdb_pending_execute_check_state(duckdb_pending_result pending_result);
  // function pending_execute_check_state(pending_resulit: PendingResult): PendingState
  Napi::Value pending_execute_check_state(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto pending_result = GetPendingResultFromExternal(env, info[0]);
    auto pending_state = duckdb_pending_execute_check_state(pending_result);
    return Napi::Number::New(env, pending_state);
  }

  // DUCKDB_C_API duckdb_state duckdb_execute_pending(duckdb_pending_result pending_result, duckdb_result *out_result);
  // function execute_pending(pending_result: PendingResult): Promise<Result>
  Napi::Value execute_pending(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto pending_result = GetPendingResultFromExternal(env, info[0]);
    auto worker = new ExecutePendingWorker(env, pending_result);
    worker->Queue();
    return worker->Promise();
  }

  // DUCKDB_C_API bool duckdb_pending_execution_is_finished(duckdb_pending_state pending_state);
  // function pending_execution_is_finished(pending_state: PendingState): boolean
  Napi::Value pending_execution_is_finished(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto pending_state = static_cast<duckdb_pending_state>(info[0].As<Napi::Number>().Uint32Value());
    auto is_finished = duckdb_pending_execution_is_finished(pending_state);
    return Napi::Boolean::New(env, is_finished);
  }

  // DUCKDB_C_API void duckdb_destroy_value(duckdb_value *value);
  // not exposed: destroyed in finalizer

  // DUCKDB_C_API duckdb_value duckdb_create_varchar(const char *text);
  // function create_varchar(text: string): Value
  Napi::Value create_varchar(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    std::string text = info[0].As<Napi::String>();
    auto value = duckdb_create_varchar_length(text.c_str(), text.size());
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_varchar_length(const char *text, idx_t length);
  // not exposed: JS string includes length

  // DUCKDB_C_API duckdb_value duckdb_create_bool(bool input);
  // function create_bool(input: boolean): Value
  Napi::Value create_bool(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = info[0].As<Napi::Boolean>();
    auto value = duckdb_create_bool(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_int8(int8_t input);
  // function create_int8(input: number): Value
  Napi::Value create_int8(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = info[0].As<Napi::Number>().Int32Value();
    auto value = duckdb_create_int8(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_uint8(uint8_t input);
  // function create_uint8(input: number): Value
  Napi::Value create_uint8(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = info[0].As<Napi::Number>().Uint32Value();
    auto value = duckdb_create_uint8(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_int16(int16_t input);
  // function create_int16(input: number): Value
  Napi::Value create_int16(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = info[0].As<Napi::Number>().Int32Value();
    auto value = duckdb_create_int16(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_uint16(uint16_t input);
  // function create_uint16(input: number): Value
  Napi::Value create_uint16(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = info[0].As<Napi::Number>().Uint32Value();
    auto value = duckdb_create_uint16(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_int32(int32_t input);
  // function create_int32(input: number): Value
  Napi::Value create_int32(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = info[0].As<Napi::Number>().Int32Value();
    auto value = duckdb_create_int32(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_uint32(uint32_t input);
  // function create_uint32(input: number): Value
  Napi::Value create_uint32(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = info[0].As<Napi::Number>().Uint32Value();
    auto value = duckdb_create_uint32(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_uint64(uint64_t input);
  // function create_uint64(input: bigint): Value
  Napi::Value create_uint64(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    bool lossless;
    auto uint64 = info[0].As<Napi::BigInt>().Uint64Value(&lossless);
    if (!lossless) {
      throw Napi::Error::New(env, "bigint out of uint64 range");
    }
    auto value = duckdb_create_uint64(uint64);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_int64(int64_t val);
  // function create_int64(int64: bigint): Value
  Napi::Value create_int64(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    bool lossless;
    auto int64 = info[0].As<Napi::BigInt>().Int64Value(&lossless);
    if (!lossless) {
      throw Napi::Error::New(env, "bigint out of int64 range");
    }
    auto value = duckdb_create_int64(int64);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_hugeint(duckdb_hugeint input);
  // function create_hugeint(input: bigint): Value
  Napi::Value create_hugeint(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input_bigint = info[0].As<Napi::BigInt>();
    auto hugeint = GetHugeIntFromBigInt(env, input_bigint);
    auto value = duckdb_create_hugeint(hugeint);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_uhugeint(duckdb_uhugeint input);
  // function create_uhugeint(input: bigint): Value
  Napi::Value create_uhugeint(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input_bigint = info[0].As<Napi::BigInt>();
    auto uhugeint = GetUHugeIntFromBigInt(env, input_bigint);
    auto value = duckdb_create_uhugeint(uhugeint);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_bignum(duckdb_bignum input);
  // function create_bignum(input: bigint): Value
  Napi::Value create_bignum(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input_bigint = info[0].As<Napi::BigInt>();
    auto bignum = GetBigNumFromBigInt(env, input_bigint);
    auto value = duckdb_create_bignum(bignum);
    duckdb_free(bignum.data);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_decimal(duckdb_decimal input);
  // function create_decimal(input: Decimal): Value
  Napi::Value create_decimal(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto decimal_obj = info[0].As<Napi::Object>();
    auto decimal = GetDecimalFromObject(env, decimal_obj);
    auto value = duckdb_create_decimal(decimal);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_float(float input);
  // function create_float(input: number): Value
  Napi::Value create_float(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = info[0].As<Napi::Number>().FloatValue();
    auto value = duckdb_create_float(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_double(double input);
  // function create_double(input: number): Value
  Napi::Value create_double(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = info[0].As<Napi::Number>().DoubleValue();
    auto value = duckdb_create_double(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_date(duckdb_date input);
  // function create_date(input: Date_): Value
  Napi::Value create_date(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = GetDateFromObject(info[0].As<Napi::Object>());
    auto value = duckdb_create_date(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_time(duckdb_time input);
  // function create_time(input: Time): Value
  Napi::Value create_time(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = GetTimeFromObject(env, info[0].As<Napi::Object>());
    auto value = duckdb_create_time(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_time_ns(duckdb_time_ns input);
  // function create_time_ns(input: TimeNS): Value
  Napi::Value create_time_ns(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = GetTimeNSFromObject(env, info[0].As<Napi::Object>());
    auto value = duckdb_create_time_ns(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_time_tz_value(duckdb_time_tz value);
  // function create_time_tz_value(input: TimeTZ): Value
  Napi::Value create_time_tz_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = GetTimeTZFromObject(env, info[0].As<Napi::Object>());
    auto value = duckdb_create_time_tz_value(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_timestamp(duckdb_timestamp input);
  // function create_timestamp(input: Timestamp): Value
  Napi::Value create_timestamp(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = GetTimestampFromObject(env, info[0].As<Napi::Object>());
    auto value = duckdb_create_timestamp(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_timestamp_tz(duckdb_timestamp input);
  // function create_timestamp_tz(input: Timestamp): Value
  Napi::Value create_timestamp_tz(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = GetTimestampFromObject(env, info[0].As<Napi::Object>());
    auto value = duckdb_create_timestamp_tz(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_timestamp_s(duckdb_timestamp_s input);
  // function create_timestamp_s(input: TimestampSeconds): Value
  Napi::Value create_timestamp_s(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = GetTimestampSecondsFromObject(env, info[0].As<Napi::Object>());
    auto value = duckdb_create_timestamp_s(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_timestamp_ms(duckdb_timestamp_ms input);
  // function create_timestamp_ms(input: TimestampMilliseconds): Value
  Napi::Value create_timestamp_ms(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = GetTimestampMillisecondsFromObject(env, info[0].As<Napi::Object>());
    auto value = duckdb_create_timestamp_ms(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_timestamp_ns(duckdb_timestamp_ns input);
  // function create_timestamp_ns(input: TimestampNanoseconds): Value
  Napi::Value create_timestamp_ns(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = GetTimestampNanosecondsFromObject(env, info[0].As<Napi::Object>());
    auto value = duckdb_create_timestamp_ns(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_interval(duckdb_interval input);
  // function create_interval(input: Interval): Value
  Napi::Value create_interval(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input = GetIntervalFromObject(env, info[0].As<Napi::Object>());
    auto value = duckdb_create_interval(input);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_blob(const uint8_t *data, idx_t length);
  // function create_blob(data: Uint8Array): Value
  Napi::Value create_blob(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto array = info[0].As<Napi::Uint8Array>();
    auto data = array.Data();
    auto length = array.ByteLength();
    auto value = duckdb_create_blob(data, length);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_bit(duckdb_bit input);
  // function create_bit(data: Uint8Array): Value
  Napi::Value create_bit(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto array = info[0].As<Napi::Uint8Array>();
    auto data = array.Data();
    auto size = array.ByteLength();
    auto value = duckdb_create_bit({ data, size });
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_uuid(duckdb_uhugeint input);
  // function create_uuid(input: bigint): Value
  Napi::Value create_uuid(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto input_bigint = info[0].As<Napi::BigInt>();
    auto uhugeint = GetUHugeIntFromBigInt(env, input_bigint);
    auto value = duckdb_create_uuid(uhugeint);
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API bool duckdb_get_bool(duckdb_value val);
  // function get_bool(value: Value): boolean
  Napi::Value get_bool(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto output = duckdb_get_bool(value);
    return Napi::Boolean::New(env, output);
  }

  // DUCKDB_C_API int8_t duckdb_get_int8(duckdb_value val);
  // function get_int8(value: Value): number
  Napi::Value get_int8(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto output = duckdb_get_int8(value);
    return Napi::Number::New(env, output);
  }

  // DUCKDB_C_API uint8_t duckdb_get_uint8(duckdb_value val);
  // function get_uint8(value: Value): number
  Napi::Value get_uint8(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto output = duckdb_get_uint8(value);
    return Napi::Number::New(env, output);
  }

  // DUCKDB_C_API int16_t duckdb_get_int16(duckdb_value val);
  // function get_int16(value: Value): number
  Napi::Value get_int16(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto output = duckdb_get_int16(value);
    return Napi::Number::New(env, output);
  }

  // DUCKDB_C_API uint16_t duckdb_get_uint16(duckdb_value val);
  // function get_uint16(value: Value): number
  Napi::Value get_uint16(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto output = duckdb_get_uint16(value);
    return Napi::Number::New(env, output);
  }

  // DUCKDB_C_API int32_t duckdb_get_int32(duckdb_value val);
  // function get_int32(value: Value): number
  Napi::Value get_int32(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto output = duckdb_get_int32(value);
    return Napi::Number::New(env, output);
  }

  // DUCKDB_C_API uint32_t duckdb_get_uint32(duckdb_value val);
  // function get_uint32(value: Value): number
  Napi::Value get_uint32(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto output = duckdb_get_uint32(value);
    return Napi::Number::New(env, output);
  }

  // DUCKDB_C_API int64_t duckdb_get_int64(duckdb_value val);
  // function get_int64(value: Value): bigint
  Napi::Value get_int64(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto int64 = duckdb_get_int64(value);
    return Napi::BigInt::New(env, int64);
  }

  // DUCKDB_C_API uint64_t duckdb_get_uint64(duckdb_value val);
  // function get_uint64(value: Value): bigint
  Napi::Value get_uint64(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto uint64 = duckdb_get_uint64(value);
    return Napi::BigInt::New(env, uint64);
  }

  // DUCKDB_C_API duckdb_hugeint duckdb_get_hugeint(duckdb_value val);
  // function get_hugeint(value: Value): bigint
  Napi::Value get_hugeint(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto hugeint = duckdb_get_hugeint(value);
    return MakeBigIntFromHugeInt(env, hugeint);
  }

  // DUCKDB_C_API duckdb_uhugeint duckdb_get_uhugeint(duckdb_value val);
  // function get_uhugeint(value: Value): bigint
  Napi::Value get_uhugeint(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto uhugeint = duckdb_get_uhugeint(value);
    return MakeBigIntFromUHugeInt(env, uhugeint);
  }

  // DUCKDB_C_API duckdb_bignum duckdb_get_bignum(duckdb_value val);
  // function get_bignum(value: Value): bigint
  Napi::Value get_bignum(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto bignum = duckdb_get_bignum(value);
    auto bigint = MakeBigIntFromBigNum(env, bignum);
    duckdb_free(bignum.data);
    return bigint;
  }

  // DUCKDB_C_API duckdb_decimal duckdb_get_decimal(duckdb_value val);
  // function get_decimal(value: Value): Decimal
  Napi::Value get_decimal(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto decimal = duckdb_get_decimal(value);
    return MakeDecimalObject(env, decimal);
  }

  // DUCKDB_C_API float duckdb_get_float(duckdb_value val);
  // function get_float(value: Value): number
  Napi::Value get_float(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto output = duckdb_get_float(value);
    return Napi::Number::New(env, output);
  }

  // DUCKDB_C_API double duckdb_get_double(duckdb_value val);
  // function get_double(value: Value): number
  Napi::Value get_double(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto output = duckdb_get_double(value);
    return Napi::Number::New(env, output);
  }

  // DUCKDB_C_API duckdb_date duckdb_get_date(duckdb_value val);
  // function get_date(value: Value): Date_
  Napi::Value get_date(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto date = duckdb_get_date(value);
    return MakeDateObject(env, date);
  }

  // DUCKDB_C_API duckdb_time duckdb_get_time(duckdb_value val);
  // function get_time(value: Value): Time
  Napi::Value get_time(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto time = duckdb_get_time(value);
    return MakeTimeObject(env, time);
  }

  // DUCKDB_C_API duckdb_time_ns duckdb_get_time_ns(duckdb_value val);
  // function get_time_ns(value: Value): TimeNS
  Napi::Value get_time_ns(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto time = duckdb_get_time_ns(value);
    return MakeTimeNSObject(env, time);
  }

  // DUCKDB_C_API duckdb_time_tz duckdb_get_time_tz(duckdb_value val);
  // function get_time_tz(value: Value): TimeTZ
  Napi::Value get_time_tz(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto time_tz = duckdb_get_time_tz(value);
    return MakeTimeTZObject(env, time_tz);
  }

  // DUCKDB_C_API duckdb_timestamp duckdb_get_timestamp(duckdb_value val);
  // function get_timestamp(value: Value): Timestamp
  Napi::Value get_timestamp(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto timestamp = duckdb_get_timestamp(value);
    return MakeTimestampObject(env, timestamp);
  }

  // DUCKDB_C_API duckdb_timestamp duckdb_get_timestamp_tz(duckdb_value val);
  // function get_timestamp_tz(value: Value): Timestamp
  Napi::Value get_timestamp_tz(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto timestamp = duckdb_get_timestamp_tz(value);
    return MakeTimestampObject(env, timestamp);
  }

  // DUCKDB_C_API duckdb_timestamp_s duckdb_get_timestamp_s(duckdb_value val);
  // function get_timestamp_s(value: Value): TimestampSeconds
  Napi::Value get_timestamp_s(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto timestamp_s = duckdb_get_timestamp_s(value);
    return MakeTimestampSecondsObject(env, timestamp_s);
  }

  // DUCKDB_C_API duckdb_timestamp_ms duckdb_get_timestamp_ms(duckdb_value val);
  // function get_timestamp_ms(value: Value): TimestampMilliseconds
  Napi::Value get_timestamp_ms(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto timestamp_ms = duckdb_get_timestamp_ms(value);
    return MakeTimestampMillisecondsObject(env, timestamp_ms);
  }

  // DUCKDB_C_API duckdb_timestamp_ns duckdb_get_timestamp_ns(duckdb_value val);
  // function get_timestamp_ns(value: Value): TimestampNanoseconds
  Napi::Value get_timestamp_ns(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto timestamp_ns = duckdb_get_timestamp_ns(value);
    return MakeTimestampNanosecondsObject(env, timestamp_ns);
  }

  // DUCKDB_C_API duckdb_interval duckdb_get_interval(duckdb_value val);
  // function get_interval(value: Value): Interval
  Napi::Value get_interval(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto interval = duckdb_get_interval(value);
    return MakeIntervalObject(env, interval);
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_get_value_type(duckdb_value val);
  // function get_value_type(value: Value): LogicalType
  Napi::Value get_value_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto logical_type = duckdb_get_value_type(value);
    return CreateExternalForLogicalTypeWithoutFinalizer(env, logical_type);
  }

  // DUCKDB_C_API duckdb_blob duckdb_get_blob(duckdb_value val);
  // function get_blob(value: Value): Uint8Array
  Napi::Value get_blob(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto blob = duckdb_get_blob(value);
    return Napi::Buffer<uint8_t>::NewOrCopy(env, reinterpret_cast<uint8_t*>(blob.data), blob.size);
  }

  // DUCKDB_C_API duckdb_bit duckdb_get_bit(duckdb_value val);
  // function get_bit(value: Value): Uint8Array
  Napi::Value get_bit(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto bit = duckdb_get_bit(value);
    return Napi::Buffer<uint8_t>::NewOrCopy(env, bit.data, bit.size);
  }

  // DUCKDB_C_API duckdb_uhugeint duckdb_get_uuid(duckdb_value val);
  // function get_uuid(value: Value): bigint
  Napi::Value get_uuid(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto uhugeint = duckdb_get_uuid(value);
    return MakeBigIntFromUHugeInt(env, uhugeint);
  }

  // DUCKDB_C_API char *duckdb_get_varchar(duckdb_value value);
  // function get_varchar(value: Value): string
  Napi::Value get_varchar(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto varchar = duckdb_get_varchar(value);
    auto str = Napi::String::New(env, varchar);
    duckdb_free(varchar);
    return str;
  }

  // DUCKDB_C_API duckdb_value duckdb_create_struct_value(duckdb_logical_type type, duckdb_value *values);
  // function create_struct_value(logical_type: LogicalType, values: readonly Value[]): Value
  Napi::Value create_struct_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto values_array = info[1].As<Napi::Array>();
    auto values_count = values_array.Length();
    // If there are no values, we still need a valid data pointer, so create a single element vector containing a null.
    std::vector<duckdb_value> values_vector(values_count > 0 ? values_count : 1);
    values_vector[0] = nullptr;
    for (uint32_t i = 0; i < values_count; i++) {
      values_vector[i] = GetValueFromExternal(env, values_array.Get(i));
    }
    auto value = duckdb_create_struct_value(logical_type, values_vector.data());
    if (!value) {
      throw Napi::Error::New(env, "Failed to create struct value");
    }
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_list_value(duckdb_logical_type type, duckdb_value *values, idx_t value_count);
  // function create_list_value(logical_type: LogicalType, values: readonly Value[]): Value
  Napi::Value create_list_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto values_array = info[1].As<Napi::Array>();
    auto values_count = values_array.Length();
    // If there are no values, we still need a valid data pointer, so create a single element vector containing a null.
    std::vector<duckdb_value> values_vector(values_count > 0 ? values_count : 1);
    values_vector[0] = nullptr;
    for (uint32_t i = 0; i < values_count; i++) {
      values_vector[i] = GetValueFromExternal(env, values_array.Get(i));
    }
    auto value = duckdb_create_list_value(logical_type, values_vector.data(), values_count);
    if (!value) {
      throw Napi::Error::New(env, "Failed to create list value");
    }
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_array_value(duckdb_logical_type type, duckdb_value *values, idx_t value_count);
  // function create_array_value(logical_type: LogicalType, values: readonly Value[]): Value
  Napi::Value create_array_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto values_array = info[1].As<Napi::Array>();
    auto values_count = values_array.Length();
    // If there are no values, we still need a valid data pointer, so create a single element vector containing a null.
    std::vector<duckdb_value> values_vector(values_count > 0 ? values_count : 1);
    values_vector[0] = nullptr;
    for (uint32_t i = 0; i < values_count; i++) {
      values_vector[i] = GetValueFromExternal(env, values_array.Get(i));
    }
    auto value = duckdb_create_array_value(logical_type, values_vector.data(), values_count);
    if (!value) {
      throw Napi::Error::New(env, "Failed to create array value");
    }
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_map_value(duckdb_logical_type map_type, duckdb_value *keys, duckdb_value *values, idx_t entry_count);
  // function create_map_value(map_type: LogicalType, keys: readonly Value[], values: readonly Value[]): Value
  Napi::Value create_map_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto map_type = GetLogicalTypeFromExternal(env, info[0]);
    auto keys_array = info[1].As<Napi::Array>();
    auto keys_count = keys_array.Length();
    auto values_array = info[2].As<Napi::Array>();
    auto values_count = values_array.Length();
    if (keys_count != values_count) {
      throw Napi::Error::New(env, "Failed to create map value: must have same number of keys and values");
    }
    auto entry_count = keys_count;
    // If there are no entries, we still need valid data pointers, so create single element vectors containing a null.
    std::vector<duckdb_value> keys_vector(entry_count > 0 ? entry_count : 1);
    keys_vector[0] = nullptr;
    std::vector<duckdb_value> values_vector(entry_count > 0 ? entry_count : 1);
    values_vector[0] = nullptr;
    for (uint32_t i = 0; i < entry_count; i++) {
      keys_vector[i] = GetValueFromExternal(env, keys_array.Get(i));
      values_vector[i] = GetValueFromExternal(env, values_array.Get(i));
    }
    auto value = duckdb_create_map_value(map_type, keys_vector.data(), values_vector.data(), entry_count);
    if (!value) {
      throw Napi::Error::New(env, "Failed to create map value");
    }
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_union_value(duckdb_logical_type union_type, idx_t tag_index, duckdb_value value);
  // function create_union_value(union_type: LogicalType, tag_index: number, value: Value): Value
  Napi::Value create_union_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto union_type = GetLogicalTypeFromExternal(env, info[0]);
    auto tag_index = info[1].As<Napi::Number>().Uint32Value();
    auto value = GetValueFromExternal(env, info[2]);
    auto union_value = duckdb_create_union_value(union_type, tag_index, value);
    if (!union_value) {
      throw Napi::Error::New(env, "Failed to create union value");
    }
    return CreateExternalForValue(env, union_value);
  }

  // DUCKDB_C_API idx_t duckdb_get_map_size(duckdb_value value);
  // function get_map_size(value: Value): number
  Napi::Value get_map_size(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto size = duckdb_get_map_size(value);
    return Napi::Number::New(env, size);
  }

  // DUCKDB_C_API duckdb_value duckdb_get_map_key(duckdb_value value, idx_t index);
  // function get_map_key(value: Value, index: number): Value
  Napi::Value get_map_key(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto output_key = duckdb_get_map_key(value, index);
    if (!output_key) {
      throw Napi::Error::New(env, "Failed to get map key");
    }
    return CreateExternalForValue(env, output_key);
  }

  // DUCKDB_C_API duckdb_value duckdb_get_map_value(duckdb_value value, idx_t index);
  // function get_map_value(value: Value, index: number): Value
  Napi::Value get_map_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto output_value = duckdb_get_map_value(value, index);
    if (!output_value) {
      throw Napi::Error::New(env, "Failed to get map value");
    }
    return CreateExternalForValue(env, output_value);
  }

  // DUCKDB_C_API bool duckdb_is_null_value(duckdb_value value);
  // function is_null_value(value: Value): boolean
  Napi::Value is_null_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto is_null = duckdb_is_null_value(value);
    return Napi::Boolean::New(env, is_null);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_null_value();
  // function create_null_value(): Value
  Napi::Value create_null_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = duckdb_create_null_value();
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API idx_t duckdb_get_list_size(duckdb_value value);
  // function get_list_size(value: Value): number
  Napi::Value get_list_size(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto size = duckdb_get_list_size(value);
    return Napi::Number::New(env, size);
  }

  // DUCKDB_C_API duckdb_value duckdb_get_list_child(duckdb_value value, idx_t index);
  // function get_list_child(value: Value, index: number): Value
  Napi::Value get_list_child(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto output_value = duckdb_get_list_child(value, index);
    if (!output_value) {
      throw Napi::Error::New(env, "Failed to get list child");
    }
    return CreateExternalForValue(env, output_value);
  }

  // DUCKDB_C_API duckdb_value duckdb_create_enum_value(duckdb_logical_type type, uint64_t value);
  // function create_enum_value(logical_type: LogicalType, value: number): Value
  Napi::Value create_enum_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto input_value = info[1].As<Napi::Number>().Uint32Value();
    auto value = duckdb_create_enum_value(logical_type, input_value);
    if (!value) {
      throw Napi::Error::New(env, "Failed to create enum value");
    }
    return CreateExternalForValue(env, value);
  }

  // DUCKDB_C_API uint64_t duckdb_get_enum_value(duckdb_value value);
  // function get_enum_value(value: Value): number
  Napi::Value get_enum_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto output_value = duckdb_get_enum_value(value);
    return Napi::Number::New(env, output_value);
  }

  // DUCKDB_C_API duckdb_value duckdb_get_struct_child(duckdb_value value, idx_t index);
  // function get_struct_child(value: Value, index: number): Value
  Napi::Value get_struct_child(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto value = GetValueFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto output_value = duckdb_get_struct_child(value, index);
    if (!output_value) {
      throw Napi::Error::New(env, "Failed to get struct child");
    }
    return CreateExternalForValue(env, output_value);
  }

  // DUCKDB_C_API char *duckdb_value_to_string(duckdb_value value);
  // TODO value to string

  // DUCKDB_C_API duckdb_logical_type duckdb_create_logical_type(duckdb_type type);
  // function create_logical_type(type: Type): LogicalType
  Napi::Value create_logical_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto type = static_cast<duckdb_type>(info[0].As<Napi::Number>().Uint32Value());
    auto logical_type = duckdb_create_logical_type(type);
    return CreateExternalForLogicalType(env, logical_type);
  }

  // DUCKDB_C_API char *duckdb_logical_type_get_alias(duckdb_logical_type type);
  // function logical_type_get_alias(logical_type: LogicalType): string | null
  Napi::Value logical_type_get_alias(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto alias = duckdb_logical_type_get_alias(logical_type);
    if (!alias) {
      return env.Null();
    }
    auto str = Napi::String::New(env, alias);
    duckdb_free(alias);
    return str;
  }

  // DUCKDB_C_API void duckdb_logical_type_set_alias(duckdb_logical_type type, const char *alias);
  // function logical_type_set_alias(logical_type: LogicalType, alias: string): void
  Napi::Value logical_type_set_alias(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto logical_type = GetLogicalTypeFromExternal(env, info[0]);
    std::string alias = info[1].As<Napi::String>();
    duckdb_logical_type_set_alias(logical_type, alias.c_str());
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_create_list_type(duckdb_logical_type type);
  // function create_list_type(logical_type: LogicalType): LogicalType
  Napi::Value create_list_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto child_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto list_logical_type = duckdb_create_list_type(child_logical_type);
    return CreateExternalForLogicalType(env, list_logical_type);
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_create_array_type(duckdb_logical_type type, idx_t array_size);
  // function create_array_type(logical_type: LogicalType, array_size: number): LogicalType
  Napi::Value create_array_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto child_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto array_size = info[1].As<Napi::Number>().Uint32Value();
    auto array_logical_type = duckdb_create_array_type(child_logical_type, array_size);
    return CreateExternalForLogicalType(env, array_logical_type);
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_create_map_type(duckdb_logical_type key_type, duckdb_logical_type value_type);
  // function create_map_type(key_type: LogicalType, value_type: LogicalType): LogicalType
  Napi::Value create_map_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto key_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto value_logical_type = GetLogicalTypeFromExternal(env, info[1]);
    auto map_logical_type = duckdb_create_map_type(key_logical_type, value_logical_type);
    return CreateExternalForLogicalType(env, map_logical_type);
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_create_union_type(duckdb_logical_type *member_types, const char **member_names, idx_t member_count);
  // function create_union_type(member_types: readonly LogicalType[], member_names: readonly string[]): LogicalType
  Napi::Value create_union_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto member_types_array = info[0].As<Napi::Array>();
    auto member_names_array = info[1].As<Napi::Array>();
    auto member_types_count = member_types_array.Length();
    auto member_names_count = member_names_array.Length();
    auto member_count = member_types_count < member_names_count ? member_types_count : member_names_count;
    // If there are no members, we still need valid data pointers, so create single element vectors containing nulls.
    std::vector<duckdb_logical_type> member_types(member_count > 0 ? member_count : 1);
    std::vector<std::string> member_names_strings(member_count);
    std::vector<const char *> member_names(member_count > 0 ? member_count : 1);
    member_types[0] = nullptr;
    member_names[0] = nullptr;
    for (uint32_t i = 0; i < member_count; i++) {
      member_types[i] = GetLogicalTypeFromExternal(env, member_types_array.Get(i));
      member_names_strings[i] = member_names_array.Get(i).As<Napi::String>();
      member_names[i] = member_names_strings[i].c_str();
    }
    auto union_logical_type = duckdb_create_union_type(member_types.data(), member_names.data(), member_count);
    return CreateExternalForLogicalType(env, union_logical_type);
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_create_struct_type(duckdb_logical_type *member_types, const char **member_names, idx_t member_count);
  // function create_struct_type(member_types: readonly LogicalType[], member_names: readonly string[]): LogicalType
  Napi::Value create_struct_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto member_types_array = info[0].As<Napi::Array>();
    auto member_names_array = info[1].As<Napi::Array>();
    auto member_types_count = member_types_array.Length();
    auto member_names_count = member_names_array.Length();
    auto member_count = member_types_count < member_names_count ? member_types_count : member_names_count;
    // If there are no members, we still need valid data pointers, so create single element vectors containing nulls.
    std::vector<duckdb_logical_type> member_types(member_count > 0 ? member_count : 1);
    std::vector<std::string> member_names_strings(member_count);
    std::vector<const char *> member_names(member_count > 0 ? member_count : 1);
    member_types[0] = nullptr;
    member_names[0] = nullptr;
    for (uint32_t i = 0; i < member_count; i++) {
      member_types[i] = GetLogicalTypeFromExternal(env, member_types_array.Get(i));
      member_names_strings[i] = member_names_array.Get(i).As<Napi::String>();
      member_names[i] = member_names_strings[i].c_str();
    }
    auto struct_logical_type = duckdb_create_struct_type(member_types.data(), member_names.data(), member_count);
    return CreateExternalForLogicalType(env, struct_logical_type);
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_create_enum_type(const char **member_names, idx_t member_count);
  // function create_enum_type(member_names: readonly string[]): LogicalType
  Napi::Value create_enum_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto member_names_array = info[0].As<Napi::Array>();
    auto member_count = member_names_array.Length();
    std::vector<std::string> member_names_strings(member_count);
    // If there are no members, we still need a valid data pointer, so create a single element vector containing a null.
    std::vector<const char *> member_names(member_count > 0 ? member_count : 1);
    member_names[0] = nullptr;
    for (uint32_t i = 0; i < member_count; i++) {
      member_names_strings[i] = member_names_array.Get(i).As<Napi::String>();
      member_names[i] = member_names_strings[i].c_str();
    }
    auto enum_logical_type = duckdb_create_enum_type(member_names.data(), member_count);
    return CreateExternalForLogicalType(env, enum_logical_type);
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_create_decimal_type(uint8_t width, uint8_t scale);
  // function create_decimal_type(width: number, scale: number): LogicalType
  Napi::Value create_decimal_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto width = info[0].As<Napi::Number>().Uint32Value();
    auto scale = info[1].As<Napi::Number>().Uint32Value();
    auto decimal_logical_type = duckdb_create_decimal_type(width, scale);
    return CreateExternalForLogicalType(env, decimal_logical_type);
  }

  // DUCKDB_C_API duckdb_type duckdb_get_type_id(duckdb_logical_type type);
  // function get_type_id(logical_type: LogicalType): Type
  Napi::Value get_type_id(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto type = duckdb_get_type_id(logical_type);
    return Napi::Number::New(env, type);
  }

  // DUCKDB_C_API uint8_t duckdb_decimal_width(duckdb_logical_type type);
  // function decimal_width(logical_type: LogicalType): number
  Napi::Value decimal_width(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto decimal_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto width = duckdb_decimal_width(decimal_logical_type);
    return Napi::Number::New(env, width);
  }

  // DUCKDB_C_API uint8_t duckdb_decimal_scale(duckdb_logical_type type);
  // function decimal_scale(logical_type: LogicalType): number
  Napi::Value decimal_scale(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto decimal_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto width = duckdb_decimal_scale(decimal_logical_type);
    return Napi::Number::New(env, width);
  }

  // DUCKDB_C_API duckdb_type duckdb_decimal_internal_type(duckdb_logical_type type);
  // function decimal_internal_type(logical_type: LogicalType): Type
  Napi::Value decimal_internal_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto decimal_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto type = duckdb_decimal_internal_type(decimal_logical_type);
    return Napi::Number::New(env, type);
  }

  // DUCKDB_C_API duckdb_type duckdb_enum_internal_type(duckdb_logical_type type);
  // function enum_internal_type(logical_type: LogicalType): Type
  Napi::Value enum_internal_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto enum_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto type = duckdb_enum_internal_type(enum_logical_type);
    return Napi::Number::New(env, type);
  }

  // DUCKDB_C_API uint32_t duckdb_enum_dictionary_size(duckdb_logical_type type);
  // function enum_dictionary_size(logical_type: LogicalType): number
  Napi::Value enum_dictionary_size(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto enum_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto size = duckdb_enum_dictionary_size(enum_logical_type);
    return Napi::Number::New(env, size);
  }

  // DUCKDB_C_API char *duckdb_enum_dictionary_value(duckdb_logical_type type, idx_t index);
  // function enum_dictionary_value(logical_type: LogicalType, index: number): string
  Napi::Value enum_dictionary_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto enum_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto value = duckdb_enum_dictionary_value(enum_logical_type, index);
    auto str = Napi::String::New(env, value);
    duckdb_free(value);
    return str;
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_list_type_child_type(duckdb_logical_type type);
  // function list_type_child_type(logical_type: LogicalType): LogicalType
  Napi::Value list_type_child_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto list_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto child_logical_type = duckdb_list_type_child_type(list_logical_type);
    return CreateExternalForLogicalType(env, child_logical_type);
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_array_type_child_type(duckdb_logical_type type);
  // function array_type_child_type(logical_type: LogicalType): LogicalType
  Napi::Value array_type_child_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto array_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto child_logical_type = duckdb_array_type_child_type(array_logical_type);
    return CreateExternalForLogicalType(env, child_logical_type);
  }

  // DUCKDB_C_API idx_t duckdb_array_type_array_size(duckdb_logical_type type);
  // function array_type_array_size(logical_type: LogicalType): number
  Napi::Value array_type_array_size(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto array_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto array_size = duckdb_array_type_array_size(array_logical_type);
    return Napi::Number::New(env, array_size);
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_map_type_key_type(duckdb_logical_type type);
  // function map_type_key_type(logical_type: LogicalType): LogicalType
  Napi::Value map_type_key_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto map_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto key_logical_type = duckdb_map_type_key_type(map_logical_type);
    return CreateExternalForLogicalType(env, key_logical_type);
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_map_type_value_type(duckdb_logical_type type);
  // function map_type_value_type(logical_type: LogicalType): LogicalType
  Napi::Value map_type_value_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto map_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto value_logical_type = duckdb_map_type_value_type(map_logical_type);
    return CreateExternalForLogicalType(env, value_logical_type);
  }

  // DUCKDB_C_API idx_t duckdb_struct_type_child_count(duckdb_logical_type type);
  // function struct_type_child_count(logical_type: LogicalType): number
  Napi::Value struct_type_child_count(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto struct_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto child_count = duckdb_struct_type_child_count(struct_logical_type);
    return Napi::Number::New(env, child_count);
  }

  // DUCKDB_C_API char *duckdb_struct_type_child_name(duckdb_logical_type type, idx_t index);
  // function struct_type_child_name(logical_type: LogicalType, index: number): string
  Napi::Value struct_type_child_name(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto struct_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto child_name = duckdb_struct_type_child_name(struct_logical_type, index);
    auto str = Napi::String::New(env, child_name);
    duckdb_free(child_name);
    return str;
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_struct_type_child_type(duckdb_logical_type type, idx_t index);
  // function struct_type_child_type(logical_type: LogicalType, index: number): LogicalType
  Napi::Value struct_type_child_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto struct_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto child_logical_type = duckdb_struct_type_child_type(struct_logical_type, index);
    return CreateExternalForLogicalType(env, child_logical_type);
  }

  // DUCKDB_C_API idx_t duckdb_union_type_member_count(duckdb_logical_type type);
  // function union_type_member_count(logical_type: LogicalType): number
  Napi::Value union_type_member_count(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto union_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto member_count = duckdb_union_type_member_count(union_logical_type);
    return Napi::Number::New(env, member_count);
  }

  // DUCKDB_C_API char *duckdb_union_type_member_name(duckdb_logical_type type, idx_t index);
  // function union_type_member_name(logical_type: LogicalType, index: number): string
  Napi::Value union_type_member_name(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto union_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto member_name = duckdb_union_type_member_name(union_logical_type, index);
    auto str = Napi::String::New(env, member_name);
    duckdb_free(member_name);
    return str;
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_union_type_member_type(duckdb_logical_type type, idx_t index);
  // function union_type_member_type(logical_type: LogicalType, index: number): LogicalType
  Napi::Value union_type_member_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto union_logical_type = GetLogicalTypeFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto member_logical_type = duckdb_union_type_member_type(union_logical_type, index);
    return CreateExternalForLogicalType(env, member_logical_type);
  }

  // DUCKDB_C_API void duckdb_destroy_logical_type(duckdb_logical_type *type);
  // not exposed: destroyed in finalizer

  // DUCKDB_C_API duckdb_state duckdb_register_logical_type(duckdb_connection con, duckdb_logical_type type, duckdb_create_type_info info);
  // TODO register logical type
  // function register_logical_type(connection: Connection, logical_type: LogicalType, info: CreateTypeInfo): void

  // DUCKDB_C_API duckdb_data_chunk duckdb_create_data_chunk(duckdb_logical_type *types, idx_t column_count);
  // function create_data_chunk(logical_types: readonly LogicalType[]): DataChunk
  Napi::Value create_data_chunk(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto types_array = info[0].As<Napi::Array>();
    auto types_count = types_array.Length();
    // If there are no types, we still need a valid data pointer, so create a single element vector containing a null.
    std::vector<duckdb_logical_type> types(types_count > 0 ? types_count : 1);
    types[0] = nullptr;
    for (uint32_t i = 0; i < types_count; i++) {
      types[i] = GetLogicalTypeFromExternal(env, types_array.Get(i));
    }
    auto data_chunk = duckdb_create_data_chunk(types.data(), types_count);
    return CreateExternalForDataChunk(env, data_chunk);
  }

  // DUCKDB_C_API void duckdb_destroy_data_chunk(duckdb_data_chunk *chunk);
  // not exposed: destroyed in finalizer

  // DUCKDB_C_API void duckdb_data_chunk_reset(duckdb_data_chunk chunk);
  // function data_chunk_reset(chunk: DataChunk): void
  Napi::Value data_chunk_reset(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto chunk = GetDataChunkFromExternal(env, info[0]);
    duckdb_data_chunk_reset(chunk);
    return env.Undefined();
  }

  // DUCKDB_C_API idx_t duckdb_data_chunk_get_column_count(duckdb_data_chunk chunk);
  // function data_chunk_get_column_count(chunk: DataChunk): number
  Napi::Value data_chunk_get_column_count(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto chunk = GetDataChunkFromExternal(env, info[0]);
    auto column_count = duckdb_data_chunk_get_column_count(chunk);
    return Napi::Number::New(env, column_count);
  }

  // DUCKDB_C_API duckdb_vector duckdb_data_chunk_get_vector(duckdb_data_chunk chunk, idx_t col_idx);
  // function data_chunk_get_vector(chunk: DataChunk, column_index: number): Vector
  Napi::Value data_chunk_get_vector(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto chunk = GetDataChunkFromExternal(env, info[0]);
    auto column_index = info[1].As<Napi::Number>().Uint32Value();
    auto vector = duckdb_data_chunk_get_vector(chunk, column_index);
    return CreateExternalForVectorWithoutFinalizer(env, vector);
  }

  // DUCKDB_C_API idx_t duckdb_data_chunk_get_size(duckdb_data_chunk chunk);
  // function data_chunk_get_size(chunk: DataChunk): number
  Napi::Value data_chunk_get_size(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto chunk = GetDataChunkFromExternal(env, info[0]);
    auto size = duckdb_data_chunk_get_size(chunk);
    return Napi::Number::New(env, size);
  }

  // DUCKDB_C_API void duckdb_data_chunk_set_size(duckdb_data_chunk chunk, idx_t size);
  // function data_chunk_set_size(chunk: DataChunk, size: number): void
  Napi::Value data_chunk_set_size(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto chunk = GetDataChunkFromExternal(env, info[0]);
    auto size = info[1].As<Napi::Number>().Uint32Value();
    duckdb_data_chunk_set_size(chunk, size);
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_vector duckdb_create_vector(duckdb_logical_type type, idx_t capacity);
  // TODO vector creation

  // DUCKDB_C_API void duckdb_destroy_vector(duckdb_vector *vector);
  // TODO vector creation

  // DUCKDB_C_API duckdb_logical_type duckdb_vector_get_column_type(duckdb_vector vector);
  // function vector_get_column_type(vector: Vector): LogicalType
  Napi::Value vector_get_column_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto logical_type = duckdb_vector_get_column_type(vector);
    return CreateExternalForLogicalType(env, logical_type);
  }

  // DUCKDB_C_API void *duckdb_vector_get_data(duckdb_vector vector);
  // function vector_get_data(vector: Vector, byte_count: number): Uint8Array
  Napi::Value vector_get_data(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto byte_count = info[1].As<Napi::Number>().Uint32Value();
    void *data = duckdb_vector_get_data(vector);
    return Napi::Buffer<uint8_t>::NewOrCopy(env, reinterpret_cast<uint8_t*>(data), byte_count);
  }

  // DUCKDB_C_API uint64_t *duckdb_vector_get_validity(duckdb_vector vector);
  // function vector_get_validity(vector: Vector, byte_count: number): Uint8Array
  Napi::Value vector_get_validity(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto byte_count = info[1].As<Napi::Number>().Uint32Value();
    uint64_t *data = duckdb_vector_get_validity(vector);
    if (!data) {
      return env.Null();
    }
    return Napi::Buffer<uint8_t>::NewOrCopy(env, reinterpret_cast<uint8_t*>(data), byte_count);
  }

  // DUCKDB_C_API void duckdb_vector_ensure_validity_writable(duckdb_vector vector);
  // function vector_ensure_validity_writable(vector: Vector): void
  Napi::Value vector_ensure_validity_writable(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    duckdb_vector_ensure_validity_writable(vector);
    return env.Undefined();
  }

  // DUCKDB_C_API void duckdb_vector_assign_string_element(duckdb_vector vector, idx_t index, const char *str);
  // function vector_assign_string_element(vector: Vector, index: number, str: string): void
  Napi::Value vector_assign_string_element(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    std::string str = info[2].As<Napi::String>();
    auto size = str.size();
    // Use the _len variant to handle embedded null characters.
    duckdb_vector_assign_string_element_len(vector, index, str.c_str(), size);
    return env.Undefined();
  }

  // DUCKDB_C_API void duckdb_vector_assign_string_element_len(duckdb_vector vector, idx_t index, const char *str, idx_t str_len);
  // function vector_assign_string_element_len(vector: Vector, index: number, data: Uint8Array): void
  Napi::Value vector_assign_string_element_len(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto array = info[2].As<Napi::Uint8Array>();
    auto data = reinterpret_cast<const char *>(array.Data());
    auto length = array.ByteLength();
    duckdb_vector_assign_string_element_len(vector, index, data, length);
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_vector duckdb_list_vector_get_child(duckdb_vector vector);
  // function list_vector_get_child(vector: Vector): Vector
  Napi::Value list_vector_get_child(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto child = duckdb_list_vector_get_child(vector);
    return CreateExternalForVectorWithoutFinalizer(env, child);
  }

  // DUCKDB_C_API idx_t duckdb_list_vector_get_size(duckdb_vector vector);
  // function list_vector_get_size(vector: Vector): number
  Napi::Value list_vector_get_size(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto size = duckdb_list_vector_get_size(vector);
    return Napi::Number::New(env, size);
  }

  // DUCKDB_C_API duckdb_state duckdb_list_vector_set_size(duckdb_vector vector, idx_t size);
  // function list_vector_set_size(vector: Vector, size: number): void
  Napi::Value list_vector_set_size(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto size = info[1].As<Napi::Number>().Uint32Value();
    duckdb_list_vector_set_size(vector, size);
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_list_vector_reserve(duckdb_vector vector, idx_t required_capacity);
  // function list_vector_reserve(vector: Vector, required_capacity: number): void
  Napi::Value list_vector_reserve(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto required_capacity = info[1].As<Napi::Number>().Uint32Value();
    duckdb_list_vector_reserve(vector, required_capacity);
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_vector duckdb_struct_vector_get_child(duckdb_vector vector, idx_t index);
  // function struct_vector_get_child(vector: Vector, index: number): Vector
  Napi::Value struct_vector_get_child(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto index = info[1].As<Napi::Number>().Uint32Value();
    auto child = duckdb_struct_vector_get_child(vector, index);
    return CreateExternalForVectorWithoutFinalizer(env, child);
  }

  // DUCKDB_C_API duckdb_vector duckdb_array_vector_get_child(duckdb_vector vector);
  // function array_vector_get_child(vector: Vector): Vector
  Napi::Value array_vector_get_child(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto vector = GetVectorFromExternal(env, info[0]);
    auto child = duckdb_array_vector_get_child(vector);
    return CreateExternalForVectorWithoutFinalizer(env, child);
  }

  // DUCKDB_C_API void duckdb_slice_vector(duckdb_vector vector, duckdb_selection_vector sel, idx_t len);
  // TODO vector manipulation

  // DUCKDB_C_API void duckdb_vector_copy_sel(duckdb_vector src, duckdb_vector dst, duckdb_selection_vector sel, idx_t src_count, idx_t src_offset, idx_t dst_offset);
  // TODO vector manipulation

  // DUCKDB_C_API void duckdb_vector_reference_value(duckdb_vector vector, duckdb_value value);
  // TODO vector manipulation

  // DUCKDB_C_API void duckdb_vector_reference_vector(duckdb_vector to_vector, duckdb_vector from_vector);
  // TODO vector manipulation

  // DUCKDB_C_API bool duckdb_validity_row_is_valid(uint64_t *validity, idx_t row);
  // function validity_row_is_valid(validity: Uint8Array | null, row_index: number): boolean
  Napi::Value validity_row_is_valid(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto validity = info[0].IsNull() ? nullptr : reinterpret_cast<uint64_t*>(info[0].As<Napi::Uint8Array>().Data());
    auto row_index = info[1].As<Napi::Number>().Uint32Value();
    auto valid = duckdb_validity_row_is_valid(validity, row_index);
    return Napi::Boolean::New(env, valid);
  }

  // DUCKDB_C_API void duckdb_validity_set_row_validity(uint64_t *validity, idx_t row, bool valid);
  // function validity_set_row_validity(validity: Uint8Array, row_index: number, valid: boolean): void
  Napi::Value validity_set_row_validity(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto validity = reinterpret_cast<uint64_t*>(info[0].As<Napi::Uint8Array>().Data());
    auto row_index = info[1].As<Napi::Number>().Uint32Value();
    auto valid = info[2].As<Napi::Boolean>();
    duckdb_validity_set_row_validity(validity, row_index, valid);
    return env.Undefined();
  }

  // DUCKDB_C_API void duckdb_validity_set_row_invalid(uint64_t *validity, idx_t row);
  // function validity_set_row_invalid(validity: Uint8Array, row_index: number): void
  Napi::Value validity_set_row_invalid(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto validity = reinterpret_cast<uint64_t*>(info[0].As<Napi::Uint8Array>().Data());
    auto row_index = info[1].As<Napi::Number>().Uint32Value();
    duckdb_validity_set_row_invalid(validity, row_index);
    return env.Undefined();
  }

  // DUCKDB_C_API void duckdb_validity_set_row_valid(uint64_t *validity, idx_t row);
  // function validity_set_row_valid(validity: Uint8Array, row_index: number): void
  Napi::Value validity_set_row_valid(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto validity = reinterpret_cast<uint64_t*>(info[0].As<Napi::Uint8Array>().Data());
    auto row_index = info[1].As<Napi::Number>().Uint32Value();
    duckdb_validity_set_row_valid(validity, row_index);
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_scalar_function duckdb_create_scalar_function();
  // function create_scalar_function(): ScalarFunction
  Napi::Value create_scalar_function(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto scalar_function = duckdb_create_scalar_function();
    return CreateExternalForScalarFunction(env, scalar_function);
  }

  // DUCKDB_C_API void duckdb_destroy_scalar_function(duckdb_scalar_function *scalar_function);
  // function destroy_scalar_function_sync(scalar_function: ScalarFunction): void
  Napi::Value destroy_scalar_function_sync(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto holder = GetScalarFunctionHolderFromExternal(env, info[0]);
    // duckdb_destroy_scalar_function is a no-op if already destroyed
    duckdb_destroy_scalar_function(&holder->scalar_function);
    return env.Undefined();
  }

  // DUCKDB_C_API void duckdb_scalar_function_set_name(duckdb_scalar_function scalar_function, const char *name);
  // function scalar_function_set_name(scalar_function: ScalarFunction, name: string): void
  Napi::Value scalar_function_set_name(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto scalar_function = GetScalarFunctionFromExternal(env, info[0]);
    std::string name = info[1].As<Napi::String>();
    duckdb_scalar_function_set_name(scalar_function, name.c_str());
    return env.Undefined();
  }

  // DUCKDB_C_API void duckdb_scalar_function_set_varargs(duckdb_scalar_function scalar_function, duckdb_logical_type type);
  // function scalar_function_set_varargs(scalar_function: ScalarFunction, logical_type: LogicalType): void
  Napi::Value scalar_function_set_varargs(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto scalar_function = GetScalarFunctionFromExternal(env, info[0]);
    auto logical_type = GetLogicalTypeFromExternal(env, info[1]);
    duckdb_scalar_function_set_varargs(scalar_function, logical_type);
    return env.Undefined();
  }

  // DUCKDB_C_API void duckdb_scalar_function_set_special_handling(duckdb_scalar_function scalar_function);
  // function scalar_function_set_special_handling(scalar_function: ScalarFunction): void
  Napi::Value scalar_function_set_special_handling(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto scalar_function = GetScalarFunctionFromExternal(env, info[0]);
    duckdb_scalar_function_set_special_handling(scalar_function);
    return env.Undefined();
  }

  // DUCKDB_C_API void duckdb_scalar_function_set_volatile(duckdb_scalar_function scalar_function);
  // function scalar_function_set_volatile(scalar_function: ScalarFunction): void
  Napi::Value scalar_function_set_volatile(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto scalar_function = GetScalarFunctionFromExternal(env, info[0]);
    duckdb_scalar_function_set_volatile(scalar_function);
    return env.Undefined();
  }

  // DUCKDB_C_API void duckdb_scalar_function_add_parameter(duckdb_scalar_function scalar_function, duckdb_logical_type type);
  // function scalar_function_add_parameter(scalar_function: ScalarFunction, logical_type: LogicalType): void
  Napi::Value scalar_function_add_parameter(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto scalar_function = GetScalarFunctionFromExternal(env, info[0]);
    auto logical_type = GetLogicalTypeFromExternal(env, info[1]);
    duckdb_scalar_function_add_parameter(scalar_function, logical_type);
    return env.Undefined();
  }

  // DUCKDB_C_API void duckdb_scalar_function_set_return_type(duckdb_scalar_function scalar_function, duckdb_logical_type type);
  // function scalar_function_set_return_type(scalar_function: ScalarFunction, logical_type: LogicalType): void
  Napi::Value scalar_function_set_return_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto scalar_function = GetScalarFunctionFromExternal(env, info[0]);
    auto logical_type = GetLogicalTypeFromExternal(env, info[1]);
    duckdb_scalar_function_set_return_type(scalar_function, logical_type);
    return env.Undefined();
  }

  // DUCKDB_C_API void duckdb_scalar_function_set_extra_info(duckdb_scalar_function scalar_function, void *extra_info, duckdb_delete_callback_t destroy);
  // function scalar_function_set_extra_info(scalar_function: ScalarFunction, extra_info: object): void
  Napi::Value scalar_function_set_extra_info(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto holder = GetScalarFunctionHolderFromExternal(env, info[0]);
    auto user_extra_info = info[1].As<Napi::Object>();
    holder->EnsureInternalExtraInfo();
    holder->internal_extra_info->SetUserExtraInfo(user_extra_info);
    return env.Undefined();
  }

  // DUCKDB_C_API void duckdb_scalar_function_set_bind(duckdb_scalar_function scalar_function, duckdb_scalar_function_bind_t bind);
  // TODO scalar function bind

  // DUCKDB_C_API void duckdb_scalar_function_set_bind_data(duckdb_bind_info info, void *bind_data, duckdb_delete_callback_t destroy);
  // TODO scalar function bind

  // DUCKDB_C_API void duckdb_scalar_function_set_bind_data_copy(duckdb_bind_info info, duckdb_copy_callback_t copy);
  // TODO scalar function bind

  // DUCKDB_C_API void duckdb_scalar_function_bind_set_error(duckdb_bind_info info, const char *error);
  // TODO scalar function bind

  // DUCKDB_C_API void duckdb_scalar_function_set_function(duckdb_scalar_function scalar_function, duckdb_scalar_function_t function);
  // function scalar_function_set_function(scalar_function: ScalarFunction, func: ScalarFunctionMainFunction): void
  Napi::Value scalar_function_set_function(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto holder = GetScalarFunctionHolderFromExternal(env, info[0]);
    auto func = info[1].As<Napi::Function>();
    holder->EnsureInternalExtraInfo();
    holder->internal_extra_info->SetMainFunction(env, func);
    duckdb_scalar_function_set_function(holder->scalar_function, &ScalarFunctionMainFunction);
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_register_scalar_function(duckdb_connection con, duckdb_scalar_function scalar_function);
  // function register_scalar_function(connection: Connection, scalar_function: ScalarFunction): void
  Napi::Value register_scalar_function(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto connection = GetConnectionFromExternal(env, info[0]);
    auto scalar_function = GetScalarFunctionFromExternal(env, info[1]);
    if (duckdb_register_scalar_function(connection, scalar_function)) {
      throw Napi::Error::New(env, "Failed to register scalar function");
    }
    return env.Undefined();
  }

  // DUCKDB_C_API void *duckdb_scalar_function_get_extra_info(duckdb_function_info info);
  // function scalar_function_get_extra_info(function_info: FunctionInfo): object | undefined
  Napi::Value scalar_function_get_extra_info(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto function_info = GetFunctionInfoFromExternal(env, info[0]);
    auto internal_extra_info = GetScalarFunctionInternalExtraInfo(function_info);
    if (!internal_extra_info || !internal_extra_info->user_extra_info_ref || internal_extra_info->user_extra_info_ref->IsEmpty()) {
      return env.Undefined();
    }
    return internal_extra_info->user_extra_info_ref->Value();
  }

  // DUCKDB_C_API void *duckdb_scalar_function_bind_get_extra_info(duckdb_bind_info info);
  // TODO scalar function bind

  // DUCKDB_C_API void *duckdb_scalar_function_get_bind_data(duckdb_function_info info);
  // TODO scalar function bind

  // DUCKDB_C_API void duckdb_scalar_function_get_client_context(duckdb_bind_info info, duckdb_client_context *out_context);
  // TODO scalar function bind

  // DUCKDB_C_API void duckdb_scalar_function_set_error(duckdb_function_info info, const char *error);
  // function scalar_function_set_error(function_info: FunctionInfo, error: string): void
  Napi::Value scalar_function_set_error(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto function_info = GetFunctionInfoFromExternal(env, info[0]);
    std::string error = info[1].As<Napi::String>();
    duckdb_scalar_function_set_error(function_info, error.c_str());
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_scalar_function_set duckdb_create_scalar_function_set(const char *name);
  // TODO scalar function set

  // DUCKDB_C_API void duckdb_destroy_scalar_function_set(duckdb_scalar_function_set *scalar_function_set);
  // TODO scalar function set

  // DUCKDB_C_API duckdb_state duckdb_add_scalar_function_to_set(duckdb_scalar_function_set set, duckdb_scalar_function function);
  // TODO scalar function set

  // DUCKDB_C_API duckdb_state duckdb_register_scalar_function_set(duckdb_connection con, duckdb_scalar_function_set set);
  // TODO scalar function set

  // DUCKDB_C_API idx_t duckdb_scalar_function_bind_get_argument_count(duckdb_bind_info info);
  // TODO scalar function expression

  // DUCKDB_C_API duckdb_expression duckdb_scalar_function_bind_get_argument(duckdb_bind_info info, idx_t index);
  // TODO scalar function expression

  // DUCKDB_C_API duckdb_selection_vector duckdb_create_selection_vector(idx_t size);
  // TODO selection vector

  // DUCKDB_C_API void duckdb_destroy_selection_vector(duckdb_selection_vector sel);
  // TODO selection vector

  // DUCKDB_C_API sel_t *duckdb_selection_vector_get_data_ptr(duckdb_selection_vector sel);
  // TODO selection vector

  // DUCKDB_C_API duckdb_aggregate_function duckdb_create_aggregate_function();
  // TODO aggregate function

  // DUCKDB_C_API void duckdb_destroy_aggregate_function(duckdb_aggregate_function *aggregate_function);
  // TODO aggregate function

  // DUCKDB_C_API void duckdb_aggregate_function_set_name(duckdb_aggregate_function aggregate_function, const char *name);
  // TODO aggregate function

  // DUCKDB_C_API void duckdb_aggregate_function_add_parameter(duckdb_aggregate_function aggregate_function, duckdb_logical_type type);
  // TODO aggregate function

  // DUCKDB_C_API void duckdb_aggregate_function_set_return_type(duckdb_aggregate_function aggregate_function, duckdb_logical_type type);
  // TODO aggregate function

  // DUCKDB_C_API void duckdb_aggregate_function_set_functions(duckdb_aggregate_function aggregate_function, duckdb_aggregate_state_size state_size, duckdb_aggregate_init_t state_init, duckdb_aggregate_update_t update, duckdb_aggregate_combine_t combine, duckdb_aggregate_finalize_t finalize);
  // TODO aggregate function

  // DUCKDB_C_API void duckdb_aggregate_function_set_destructor(duckdb_aggregate_function aggregate_function, duckdb_aggregate_destroy_t destroy);
  // TODO aggregate function

  // DUCKDB_C_API duckdb_state duckdb_register_aggregate_function(duckdb_connection con, duckdb_aggregate_function aggregate_function);
  // TODO aggregate function

  // DUCKDB_C_API void duckdb_aggregate_function_set_special_handling(duckdb_aggregate_function aggregate_function);
  // TODO aggregate function

  // DUCKDB_C_API void duckdb_aggregate_function_set_extra_info(duckdb_aggregate_function aggregate_function, void *extra_info, duckdb_delete_callback_t destroy);
  // TODO aggregate function

  // DUCKDB_C_API void *duckdb_aggregate_function_get_extra_info(duckdb_function_info info);
  // TODO aggregate function

  // DUCKDB_C_API void duckdb_aggregate_function_set_error(duckdb_function_info info, const char *error);
  // TODO aggregate function

  // DUCKDB_C_API duckdb_aggregate_function_set duckdb_create_aggregate_function_set(const char *name);
  // TODO aggregate function set

  // DUCKDB_C_API void duckdb_destroy_aggregate_function_set(duckdb_aggregate_function_set *aggregate_function_set);
  // TODO aggregate function set

  // DUCKDB_C_API duckdb_state duckdb_add_aggregate_function_to_set(duckdb_aggregate_function_set set, duckdb_aggregate_function function);
  // TODO aggregate function set

  // DUCKDB_C_API duckdb_state duckdb_register_aggregate_function_set(duckdb_connection con, duckdb_aggregate_function_set set);
  // TODO aggregate function set

  // DUCKDB_C_API duckdb_table_function duckdb_create_table_function();
  // TODO table function

  // DUCKDB_C_API void duckdb_destroy_table_function(duckdb_table_function *table_function);
  // TODO table function

  // DUCKDB_C_API void duckdb_table_function_set_name(duckdb_table_function table_function, const char *name);
  // TODO table function

  // DUCKDB_C_API void duckdb_table_function_add_parameter(duckdb_table_function table_function, duckdb_logical_type type);
  // TODO table function

  // DUCKDB_C_API void duckdb_table_function_add_named_parameter(duckdb_table_function table_function, const char *name, duckdb_logical_type type);
  // TODO table function

  // DUCKDB_C_API void duckdb_table_function_set_extra_info(duckdb_table_function table_function, void *extra_info, duckdb_delete_callback_t destroy);
  // TODO table function

  // DUCKDB_C_API void duckdb_table_function_set_bind(duckdb_table_function table_function, duckdb_table_function_bind_t bind);
  // TODO table function

  // DUCKDB_C_API void duckdb_table_function_set_init(duckdb_table_function table_function, duckdb_table_function_init_t init);
  // TODO table function

  // DUCKDB_C_API void duckdb_table_function_set_local_init(duckdb_table_function table_function, duckdb_table_function_init_t init);
  // TODO table function

  // DUCKDB_C_API void duckdb_table_function_set_function(duckdb_table_function table_function, duckdb_table_function_t function);
  // TODO table function

  // DUCKDB_C_API void duckdb_table_function_supports_projection_pushdown(duckdb_table_function table_function, bool pushdown);
  // TODO table function

  // DUCKDB_C_API duckdb_state duckdb_register_table_function(duckdb_connection con, duckdb_table_function function);
  // TODO table function

  // DUCKDB_C_API void *duckdb_bind_get_extra_info(duckdb_bind_info info);
  // TODO bind info

  // DUCKDB_C_API void duckdb_table_function_get_client_context(duckdb_bind_info info, duckdb_client_context *out_context);
  // TODO bind info

  // DUCKDB_C_API void duckdb_bind_add_result_column(duckdb_bind_info info, const char *name, duckdb_logical_type type);
  // TODO bind info

  // DUCKDB_C_API idx_t duckdb_bind_get_parameter_count(duckdb_bind_info info);
  // TODO bind info

  // DUCKDB_C_API duckdb_value duckdb_bind_get_parameter(duckdb_bind_info info, idx_t index);
  // TODO bind info

  // DUCKDB_C_API duckdb_value duckdb_bind_get_named_parameter(duckdb_bind_info info, const char *name);
  // TODO bind info

  // DUCKDB_C_API void duckdb_bind_set_bind_data(duckdb_bind_info info, void *bind_data, duckdb_delete_callback_t destroy);
  // TODO bind info

  // DUCKDB_C_API void duckdb_bind_set_cardinality(duckdb_bind_info info, idx_t cardinality, bool is_exact);
  // TODO bind info

  // DUCKDB_C_API void duckdb_bind_set_error(duckdb_bind_info info, const char *error);
  // TODO bind info

  // DUCKDB_C_API void *duckdb_init_get_extra_info(duckdb_init_info info);
  // TODO init info

  // DUCKDB_C_API void *duckdb_init_get_bind_data(duckdb_init_info info);
  // TODO init info

  // DUCKDB_C_API void duckdb_init_set_init_data(duckdb_init_info info, void *init_data, duckdb_delete_callback_t destroy);
  // TODO init info

  // DUCKDB_C_API idx_t duckdb_init_get_column_count(duckdb_init_info info);
  // TODO init info

  // DUCKDB_C_API idx_t duckdb_init_get_column_index(duckdb_init_info info, idx_t column_index);
  // TODO init info

  // DUCKDB_C_API void duckdb_init_set_max_threads(duckdb_init_info info, idx_t max_threads);
  // TODO init info

  // DUCKDB_C_API void duckdb_init_set_error(duckdb_init_info info, const char *error);
  // TODO init info

  // DUCKDB_C_API void *duckdb_function_get_extra_info(duckdb_function_info info);
  // TODO function info

  // DUCKDB_C_API void *duckdb_function_get_bind_data(duckdb_function_info info);
  // TODO function info

  // DUCKDB_C_API void *duckdb_function_get_init_data(duckdb_function_info info);
  // TODO function info

  // DUCKDB_C_API void *duckdb_function_get_local_init_data(duckdb_function_info info);
  // TODO function info

  // DUCKDB_C_API void duckdb_function_set_error(duckdb_function_info info, const char *error);
  // TODO function info

  // DUCKDB_C_API void duckdb_add_replacement_scan(duckdb_database db, duckdb_replacement_callback_t replacement, void *extra_data, duckdb_delete_callback_t delete_callback);
  // TODO replacement scan

  // DUCKDB_C_API void duckdb_replacement_scan_set_function_name(duckdb_replacement_scan_info info, const char *function_name);
  // TODO replacement scan

  // DUCKDB_C_API void duckdb_replacement_scan_add_parameter(duckdb_replacement_scan_info info, duckdb_value parameter);
  // TODO replacement scan

  // DUCKDB_C_API void duckdb_replacement_scan_set_error(duckdb_replacement_scan_info info, const char *error);
  // TODO replacement scan

  // DUCKDB_C_API duckdb_profiling_info duckdb_get_profiling_info(duckdb_connection connection);
  // TODO profiling info

  // DUCKDB_C_API duckdb_value duckdb_profiling_info_get_value(duckdb_profiling_info info, const char *key);
  // TODO profiling info

  // DUCKDB_C_API duckdb_value duckdb_profiling_info_get_metrics(duckdb_profiling_info info);
  // TODO profiling info

  // DUCKDB_C_API idx_t duckdb_profiling_info_get_child_count(duckdb_profiling_info info);
  // TODO profiling info

  // DUCKDB_C_API duckdb_profiling_info duckdb_profiling_info_get_child(duckdb_profiling_info info, idx_t index);
  // TODO profiling info

  // DUCKDB_C_API duckdb_state duckdb_appender_create(duckdb_connection connection, const char *schema, const char *table, duckdb_appender *out_appender);
  // function appender_create(connection: Connection, schema: string | null, table: string): Appender
  Napi::Value appender_create(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto connection = GetConnectionFromExternal(env, info[0]);
    if (!connection) {
      throw Napi::Error::New(env, "Failed to create appender: connection disconnected");
    }
    std::string schema = info[1].IsNull() ? std::string() : info[1].As<Napi::String>();
    std::string table = info[2].As<Napi::String>();
    duckdb_appender appender;
    if (
      duckdb_appender_create(
        connection,
        info[1].IsNull() ? nullptr : schema.c_str(),
        table.c_str(),
        &appender
      )
    ) {
      std::string error = duckdb_appender_error(appender);
      duckdb_appender_destroy(&appender);
      throw Napi::Error::New(env, error);
    }
    return CreateExternalForAppender(env, appender);
  }

  // DUCKDB_C_API duckdb_state duckdb_appender_create_ext(duckdb_connection connection, const char *catalog, const char *schema, const char *table, duckdb_appender *out_appender);
  // function appender_create_ext(connection: Connection, catalog: string | null, schema: string | null, table: string): Appender
  Napi::Value appender_create_ext(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto connection = GetConnectionFromExternal(env, info[0]);
    if (!connection) {
      throw Napi::Error::New(env, "Failed to create appender: connection disconnected");
    }
    std::string catalog = info[1].IsNull() ? std::string() : info[1].As<Napi::String>();
    std::string schema = info[2].IsNull() ? std::string() : info[2].As<Napi::String>();
    std::string table = info[3].As<Napi::String>();
    duckdb_appender appender;
    if (
      duckdb_appender_create_ext(
        connection,
        info[1].IsNull() ? nullptr : catalog.c_str(),
        info[2].IsNull() ? nullptr : schema.c_str(),
        table.c_str(),
        &appender
      )
    ) {
      std::string error = duckdb_appender_error(appender);
      duckdb_appender_destroy(&appender);
      throw Napi::Error::New(env, error);
    }
    return CreateExternalForAppender(env, appender);
  }

  // DUCKDB_C_API duckdb_state duckdb_appender_create_query(duckdb_connection connection, const char *query, idx_t column_count, duckdb_logical_type *types, const char *table_name, const char **column_names, duckdb_appender *out_appender);
  // TODO appender create query

  // DUCKDB_C_API idx_t duckdb_appender_column_count(duckdb_appender appender);
  // function appender_column_count(appender: Appender): number
  Napi::Value appender_column_count(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto column_count = duckdb_appender_column_count(appender);
    return Napi::Number::New(env, column_count);
  }

  // DUCKDB_C_API duckdb_logical_type duckdb_appender_column_type(duckdb_appender appender, idx_t col_idx);
  // function appender_column_type(appender: Appender, column_index: number): LogicalType
  Napi::Value appender_column_type(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto column_index = info[1].As<Napi::Number>().Uint32Value();
    auto logical_type = duckdb_appender_column_type(appender, column_index);
    return CreateExternalForLogicalType(env, logical_type);
  }

  // #ifndef DUCKDB_API_NO_DEPRECATED

  // DUCKDB_C_API const char *duckdb_appender_error(duckdb_appender appender);
  // not exposed: other appender functions throw

  // #endif

  // DUCKDB_C_API duckdb_error_data duckdb_appender_error_data(duckdb_appender appender);
  // TODO appender error data

  // DUCKDB_C_API duckdb_state duckdb_appender_flush(duckdb_appender appender);
  // function appender_flush(appender: Appender): void
  Napi::Value appender_flush_sync(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    if (duckdb_appender_flush(appender)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_appender_close(duckdb_appender appender);
  // function appender_close(appender: Appender): void
  Napi::Value appender_close_sync(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    if (duckdb_appender_close(appender)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_appender_destroy(duckdb_appender *appender);
  // not exposed: destroyed in finalizer

  // DUCKDB_C_API duckdb_state duckdb_appender_add_column(duckdb_appender appender, const char *name);
  // TODO appender columns

  // DUCKDB_C_API duckdb_state duckdb_appender_clear_columns(duckdb_appender appender);
  // TODO appender columns

  // DUCKDB_C_API duckdb_state duckdb_appender_begin_row(duckdb_appender appender);
  // not exposed: no-op

  // DUCKDB_C_API duckdb_state duckdb_appender_end_row(duckdb_appender appender);
  // function appender_end_row(appender: Appender): void
  Napi::Value appender_end_row(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    if (duckdb_appender_end_row(appender)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_default(duckdb_appender appender);
  // function append_default(appender: Appender): void
  Napi::Value append_default(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    if (duckdb_append_default(appender)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_default_to_chunk(duckdb_appender appender, duckdb_data_chunk chunk, idx_t col, idx_t row);
  // TODO appender default

  // DUCKDB_C_API duckdb_state duckdb_append_bool(duckdb_appender appender, bool value);
  // function append_bool(appender: Appender, bool: boolean): void
  Napi::Value append_bool(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto bool_value = info[1].As<Napi::Boolean>();
    if (duckdb_append_bool(appender, bool_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_int8(duckdb_appender appender, int8_t value);
  // function append_int8(appender: Appender, int8: number): void
  Napi::Value append_int8(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto int8_value = info[1].As<Napi::Number>().Int32Value();
    if (duckdb_append_int8(appender, int8_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_int16(duckdb_appender appender, int16_t value);
  // function append_int16(appender: Appender, int16: number): void
  Napi::Value append_int16(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto int16_value = info[1].As<Napi::Number>().Int32Value();
    if (duckdb_append_int16(appender, int16_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_int32(duckdb_appender appender, int32_t value);
  // function append_int32(appender: Appender, int32: number): void
  Napi::Value append_int32(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto int32_value = info[1].As<Napi::Number>().Int32Value();
    if (duckdb_append_int32(appender, int32_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_int64(duckdb_appender appender, int64_t value);
  // function append_int64(appender: Appender, int64: bigint): void
  Napi::Value append_int64(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    bool lossless;
    auto int64_value = info[1].As<Napi::BigInt>().Int64Value(&lossless);
    if (!lossless) {
      throw Napi::Error::New(env, "bigint out of int64 range");
    }
    if (duckdb_append_int64(appender, int64_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_hugeint(duckdb_appender appender, duckdb_hugeint value);
  // function append_hugeint(appender: Appender, hugeint: bigint): void
  Napi::Value append_hugeint(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto bigint = info[1].As<Napi::BigInt>();
    auto hugeint_value = GetHugeIntFromBigInt(env, bigint);
    if (duckdb_append_hugeint(appender, hugeint_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_uint8(duckdb_appender appender, uint8_t value);
  // function append_uint8(appender: Appender, uint8: number): void
  Napi::Value append_uint8(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto uint8_value = info[1].As<Napi::Number>().Uint32Value();
    if (duckdb_append_uint8(appender, uint8_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_uint16(duckdb_appender appender, uint16_t value);
  // function append_uint16(appender: Appender, uint16: number): void
  Napi::Value append_uint16(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto uint16_value = info[1].As<Napi::Number>().Uint32Value();
    if (duckdb_append_uint16(appender, uint16_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_uint32(duckdb_appender appender, uint32_t value);
  // function append_uint32(appender: Appender, uint32: number): void
  Napi::Value append_uint32(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto uint32_value = info[1].As<Napi::Number>().Uint32Value();
    if (duckdb_append_uint32(appender, uint32_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_uint64(duckdb_appender appender, uint64_t value);
  // function append_uint64(appender: Appender, uint64: bigint): void
  Napi::Value append_uint64(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    bool lossless;
    auto uint64_value = info[1].As<Napi::BigInt>().Uint64Value(&lossless);
    if (!lossless) {
      throw Napi::Error::New(env, "bigint out of uint64 range");
    }
    if (duckdb_append_uint64(appender, uint64_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_uhugeint(duckdb_appender appender, duckdb_uhugeint value);
  // function append_uhugeint(appender: Appender, uhugeint: bigint): void
  Napi::Value append_uhugeint(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto bigint = info[1].As<Napi::BigInt>();
    auto uhugeint_value = GetUHugeIntFromBigInt(env, bigint);
    if (duckdb_append_uhugeint(appender, uhugeint_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_float(duckdb_appender appender, float value);
  // function append_float(appender: Appender, float: number): void
  Napi::Value append_float(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto float_value = info[1].As<Napi::Number>().FloatValue();
    if (duckdb_append_float(appender, float_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_double(duckdb_appender appender, double value);
  // function append_double(appender: Appender, double: number): void
  Napi::Value append_double(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto double_value = info[1].As<Napi::Number>().DoubleValue();
    if (duckdb_append_double(appender, double_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_date(duckdb_appender appender, duckdb_date value);
  // function append_date(appender: Appender, date: Date_): void
  Napi::Value append_date(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto date_value = GetDateFromObject(info[1].As<Napi::Object>());
    if (duckdb_append_date(appender, date_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_time(duckdb_appender appender, duckdb_time value);
  // function append_time(appender: Appender, time: Time): void
  Napi::Value append_time(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto time_value = GetTimeFromObject(env, info[1].As<Napi::Object>());
    if (duckdb_append_time(appender, time_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_timestamp(duckdb_appender appender, duckdb_timestamp value);
  // function append_timestamp(appender: Appender, timestamp: Timestamp): void
  Napi::Value append_timestamp(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto timestamp_value = GetTimestampFromObject(env, info[1].As<Napi::Object>());
    if (duckdb_append_timestamp(appender, timestamp_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_interval(duckdb_appender appender, duckdb_interval value);
  // function append_interval(appender: Appender, interval: Interval): void
  Napi::Value append_interval(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto interval_value = GetIntervalFromObject(env, info[1].As<Napi::Object>());
    if (duckdb_append_interval(appender, interval_value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_varchar(duckdb_appender appender, const char *val);
  // function append_varchar(appender: Appender, varchar: string): void
  Napi::Value append_varchar(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    std::string str = info[1].As<Napi::String>();
    if (duckdb_append_varchar_length(appender, str.c_str(), str.size())) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_varchar_length(duckdb_appender appender, const char *val, idx_t length);
  // not exposed: JS string includes length

  // DUCKDB_C_API duckdb_state duckdb_append_blob(duckdb_appender appender, const void *data, idx_t length);
  // function append_blob(appender: Appender, data: Uint8Array): void
  Napi::Value append_blob(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto array = info[1].As<Napi::Uint8Array>();
    auto data = reinterpret_cast<void*>(array.Data());
    auto length = array.ByteLength();
    if (duckdb_append_blob(appender, data, length)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_null(duckdb_appender appender);
  // function append_null(appender: Appender): void
  Napi::Value append_null(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    if (duckdb_append_null(appender)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_value(duckdb_appender appender, duckdb_value value);
  // function append_value(appender: Appender, value: Value): void
  Napi::Value append_value(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto value = GetValueFromExternal(env, info[1]);
    if (duckdb_append_value(appender, value)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_append_data_chunk(duckdb_appender appender, duckdb_data_chunk chunk);
  // function append_data_chunk(appender: Appender, chunk: DataChunk): void
  Napi::Value append_data_chunk(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto appender = GetAppenderFromExternal(env, info[0]);
    auto chunk = GetDataChunkFromExternal(env, info[1]);
    if (duckdb_append_data_chunk(appender, chunk)) {
      throw Napi::Error::New(env, duckdb_appender_error(appender));
    }
    return env.Undefined();
  }

  // DUCKDB_C_API duckdb_state duckdb_table_description_create(duckdb_connection connection, const char *schema, const char *table, duckdb_table_description *out);
  // TODO table description

  // DUCKDB_C_API duckdb_state duckdb_table_description_create_ext(duckdb_connection connection, const char *catalog, const char *schema, const char *table, duckdb_table_description *out);
  // TODO table description

  // DUCKDB_C_API void duckdb_table_description_destroy(duckdb_table_description *table_description);
  // TODO table description

  // DUCKDB_C_API const char *duckdb_table_description_error(duckdb_table_description table_description);
  // TODO table description

  // DUCKDB_C_API duckdb_state duckdb_column_has_default(duckdb_table_description table_description, idx_t index, bool *out);
  // TODO table description

  // DUCKDB_C_API char *duckdb_table_description_get_column_name(duckdb_table_description table_description, idx_t index);
  // TODO table description

  // DUCKDB_C_API duckdb_error_data duckdb_to_arrow_schema(duckdb_arrow_options arrow_options, duckdb_logical_type *types, const char **names, idx_t column_count, struct ArrowSchema *out_schema);
  // TODO arrow

  // DUCKDB_C_API duckdb_error_data duckdb_data_chunk_to_arrow(duckdb_arrow_options arrow_options, duckdb_data_chunk chunk, struct ArrowArray *out_arrow_array);
  // TODO arrow

  // DUCKDB_C_API duckdb_error_data duckdb_schema_from_arrow(duckdb_connection connection, struct ArrowSchema *schema, duckdb_arrow_converted_schema *out_types);
  // TODO arrow

  // DUCKDB_C_API duckdb_error_data duckdb_data_chunk_from_arrow(duckdb_connection connection, struct ArrowArray *arrow_array, duckdb_arrow_converted_schema converted_schema, duckdb_data_chunk *out_chunk);
  // TODO arrow

  // DUCKDB_C_API void duckdb_destroy_arrow_converted_schema(duckdb_arrow_converted_schema *arrow_converted_schema);
  // TODO arrow

  // #ifndef DUCKDB_API_NO_DEPRECATED

  // DUCKDB_C_API duckdb_state duckdb_query_arrow(duckdb_connection connection, const char *query, duckdb_arrow *out_result);
  // deprecated

  // DUCKDB_C_API duckdb_state duckdb_query_arrow_schema(duckdb_arrow result, duckdb_arrow_schema *out_schema);
  // deprecated

  // DUCKDB_C_API duckdb_state duckdb_prepared_arrow_schema(duckdb_prepared_statement prepared, duckdb_arrow_schema *out_schema);
  // deprecated

  // DUCKDB_C_API void duckdb_result_arrow_array(duckdb_result result, duckdb_data_chunk chunk, duckdb_arrow_array *out_array);
  // deprecated

  // DUCKDB_C_API duckdb_state duckdb_query_arrow_array(duckdb_arrow result, duckdb_arrow_array *out_array);
  // deprecated

  // DUCKDB_C_API idx_t duckdb_arrow_column_count(duckdb_arrow result);
  // deprecated

  // DUCKDB_C_API idx_t duckdb_arrow_row_count(duckdb_arrow result);
  // deprecated

  // DUCKDB_C_API idx_t duckdb_arrow_rows_changed(duckdb_arrow result);
  // deprecated

  // DUCKDB_C_API const char *duckdb_query_arrow_error(duckdb_arrow result);
  // deprecated

  // DUCKDB_C_API void duckdb_destroy_arrow(duckdb_arrow *result);
  // deprecated

  // DUCKDB_C_API void duckdb_destroy_arrow_stream(duckdb_arrow_stream *stream_p);
  // deprecated

  // DUCKDB_C_API duckdb_state duckdb_execute_prepared_arrow(duckdb_prepared_statement prepared_statement, duckdb_arrow *out_result);
  // deprecated

  // DUCKDB_C_API duckdb_state duckdb_arrow_scan(duckdb_connection connection, const char *table_name, duckdb_arrow_stream arrow);
  // deprecated

  // DUCKDB_C_API duckdb_state duckdb_arrow_array_scan(duckdb_connection connection, const char *table_name, duckdb_arrow_schema arrow_schema, duckdb_arrow_array arrow_array, duckdb_arrow_stream *out_stream);
  // deprecated

  // #endif

  // DUCKDB_C_API void duckdb_execute_tasks(duckdb_database database, idx_t max_tasks);
  // TODO tasks

  // DUCKDB_C_API duckdb_task_state duckdb_create_task_state(duckdb_database database);
  // TODO tasks

  // DUCKDB_C_API void duckdb_execute_tasks_state(duckdb_task_state state);
  // TODO tasks

  // DUCKDB_C_API idx_t duckdb_execute_n_tasks_state(duckdb_task_state state, idx_t max_tasks);
  // TODO tasks

  // DUCKDB_C_API void duckdb_finish_execution(duckdb_task_state state);
  // TODO tasks

  // DUCKDB_C_API bool duckdb_task_state_is_finished(duckdb_task_state state);
  // TODO tasks

  // DUCKDB_C_API void duckdb_destroy_task_state(duckdb_task_state state);
  // TODO tasks

  // DUCKDB_C_API bool duckdb_execution_is_finished(duckdb_connection con);
  // TODO tasks

  // #ifndef DUCKDB_API_NO_DEPRECATED

  // DUCKDB_C_API duckdb_data_chunk duckdb_stream_fetch_chunk(duckdb_result result);
  // deprecated

  // #endif

  // DUCKDB_C_API duckdb_data_chunk duckdb_fetch_chunk(duckdb_result result);
  // function fetch_chunk(result: Result): Promise<DataChunk | null>
  Napi::Value fetch_chunk(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto result_ptr = GetResultFromExternal(env, info[0]);
    auto worker = new FetchWorker(env, result_ptr);
    worker->Queue();
    return worker->Promise();
  }

  // DUCKDB_C_API duckdb_cast_function duckdb_create_cast_function();
  // TODO cast function

  // DUCKDB_C_API void duckdb_cast_function_set_source_type(duckdb_cast_function cast_function, duckdb_logical_type source_type);
  // TODO cast function

  // DUCKDB_C_API void duckdb_cast_function_set_target_type(duckdb_cast_function cast_function, duckdb_logical_type target_type);
  // TODO cast function

  // DUCKDB_C_API void duckdb_cast_function_set_implicit_cast_cost(duckdb_cast_function cast_function, int64_t cost);
  // TODO cast function

  // DUCKDB_C_API void duckdb_cast_function_set_function(duckdb_cast_function cast_function, duckdb_cast_function_t function);
  // TODO cast function

  // DUCKDB_C_API void duckdb_cast_function_set_extra_info(duckdb_cast_function cast_function, void *extra_info, duckdb_delete_callback_t destroy);
  // TODO cast function

  // DUCKDB_C_API void *duckdb_cast_function_get_extra_info(duckdb_function_info info);
  // TODO cast function

  // DUCKDB_C_API duckdb_cast_mode duckdb_cast_function_get_cast_mode(duckdb_function_info info);
  // TODO cast function

  // DUCKDB_C_API void duckdb_cast_function_set_error(duckdb_function_info info, const char *error);
  // TODO cast function

  // DUCKDB_C_API void duckdb_cast_function_set_row_error(duckdb_function_info info, const char *error, idx_t row, duckdb_vector output);
  // TODO cast function

  // DUCKDB_C_API duckdb_state duckdb_register_cast_function(duckdb_connection con, duckdb_cast_function cast_function);
  // TODO cast function

  // DUCKDB_C_API void duckdb_destroy_cast_function(duckdb_cast_function *cast_function);
  // TODO cast function

  // DUCKDB_C_API void duckdb_destroy_expression(duckdb_expression *expr);
  // TODO expression

  // DUCKDB_C_API duckdb_logical_type duckdb_expression_return_type(duckdb_expression expr);
  // TODO expression

  // DUCKDB_C_API bool duckdb_expression_is_foldable(duckdb_expression expr);
  // TODO expression

  // DUCKDB_C_API duckdb_error_data duckdb_expression_fold(duckdb_client_context context, duckdb_expression expr, duckdb_value *out_value);
  // TODO expression

  // ADDED
  // function get_data_from_pointer(array_buffer: ArrayBuffer, pointer_offset: number, byte_count: number): Uint8Array
  Napi::Value get_data_from_pointer(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto data = reinterpret_cast<uint8_t*>(info[0].As<Napi::ArrayBuffer>().Data());
    auto pointer_offset = info[1].As<Napi::Number>().Uint32Value();
    auto byte_count = info[2].As<Napi::Number>().Uint32Value();
    auto pointer_pointer = reinterpret_cast<uint8_t**>(data + pointer_offset);
    auto pointer = *pointer_pointer;
    return Napi::Buffer<uint8_t>::NewOrCopy(env, pointer, byte_count);
  }

  // ADDED
  // function copy_data_to_vector(target_vector: Vector, target_byte_offset: number, source_buffer: ArrayBuffer, source_byte_offset: number, source_byte_count: number): void
  Napi::Value copy_data_to_vector(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto target_vector = GetVectorFromExternal(env, info[0]);
    auto target_byte_offset = info[1].As<Napi::Number>().Uint32Value();
    auto source_data = reinterpret_cast<uint8_t*>(info[2].As<Napi::ArrayBuffer>().Data());
    auto source_byte_offset = info[3].As<Napi::Number>().Uint32Value();
    auto source_byte_count = info[4].As<Napi::Number>().Uint32Value();
    auto target_data = reinterpret_cast<uint8_t*>(duckdb_vector_get_data(target_vector));
    memcpy(target_data + target_byte_offset, source_data + source_byte_offset, source_byte_count);
    return env.Undefined();
  }

  // ADDED
  // function copy_data_to_vector_validity(target_vector: Vector, target_byte_offset: number, source_buffer: ArrayBuffer, source_byte_offset: number, source_byte_count: number): void
  Napi::Value copy_data_to_vector_validity(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto target_vector = GetVectorFromExternal(env, info[0]);
    auto target_byte_offset = info[1].As<Napi::Number>().Uint32Value();
    auto source_data = reinterpret_cast<uint8_t*>(info[2].As<Napi::ArrayBuffer>().Data());
    auto source_byte_offset = info[3].As<Napi::Number>().Uint32Value();
    auto source_byte_count = info[4].As<Napi::Number>().Uint32Value();
    auto target_data = reinterpret_cast<uint8_t*>(duckdb_vector_get_validity(target_vector));
    memcpy(target_data + target_byte_offset, source_data + source_byte_offset, source_byte_count);
    return env.Undefined();
  }

};

NODE_API_ADDON(DuckDBNodeAddon)

/*

459 DUCKDB_C_API
    264 function
     24 not exposed
     41 deprecated
    130 TODO
        8 arrow
        5 error data
        1 value to string
        1 register logical type
        2 vector creation
        4 vector manipulation
        7 scalar function bind
        4 scalar function set
        2 scalar function expression
        3 selection vector
       12 aggregate function
        4 aggregate function set
       12 table function
        9 bind info
        7 init info
        5 function info
        4 replacement scan
        5 profiling info
        1 appender create query
        1 appender error data
        2 appender columns
        1 appender default
        6 table description
        8 tasks
       12 cast function
        4 expression
  3 ADDED
---
462 total

regexes:
// DUCKDB_C_API.*\n  // (function|not exposed|deprecated|TODO)
// DUCKDB_C_API.*\n  // (function)
// DUCKDB_C_API.*\n  // (not exposed)
// DUCKDB_C_API.*\n  // (deprecated)
// DUCKDB_C_API.*\n  // (TODO)
// (ADDED)

*/
