#pragma once

#include "napi_setup.h"
#include "duckdb.h"
#include <cstddef>

// Napi helpers

inline Napi::Reference<Napi::Value> MakeValueRef(Napi::Value value) {
  return value.IsUndefined()
    ? Napi::Reference<Napi::Value>() 
    : Napi::Reference<Napi::Value>::New(value, 1);
}

// Conversion betweeen structs and objects

inline Napi::Object MakeDateObject(Napi::Env env, duckdb_date date) {
  auto date_obj = Napi::Object::New(env);
  date_obj.Set("days", Napi::Number::New(env, date.days));
  return date_obj;
}

inline duckdb_date GetDateFromObject(Napi::Object date_obj) {
  auto days = date_obj.Get("days").As<Napi::Number>().Int32Value();
  return { days };
}

inline Napi::Object MakeDatePartsObject(Napi::Env env, duckdb_date_struct date_parts) {
  auto date_parts_obj = Napi::Object::New(env);
  date_parts_obj.Set("year", Napi::Number::New(env, date_parts.year));
  date_parts_obj.Set("month", Napi::Number::New(env, date_parts.month));
  date_parts_obj.Set("day", Napi::Number::New(env, date_parts.day));
  return date_parts_obj;
}

inline duckdb_date_struct GetDatePartsFromObject(Napi::Object date_parts_obj) {
  int32_t year = date_parts_obj.Get("year").As<Napi::Number>().Int32Value();
  int8_t month = date_parts_obj.Get("month").As<Napi::Number>().Int32Value();
  int8_t day = date_parts_obj.Get("day").As<Napi::Number>().Int32Value();
  return { year, month, day };
}

inline Napi::Object MakeTimeObject(Napi::Env env, duckdb_time time) {
  auto time_obj = Napi::Object::New(env);
  time_obj.Set("micros", Napi::BigInt::New(env, time.micros));
  return time_obj;
}

inline duckdb_time GetTimeFromObject(Napi::Env env, Napi::Object time_obj) {
  bool lossless;
  auto micros = time_obj.Get("micros").As<Napi::BigInt>().Int64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "micros out of int64 range");
  }
  return { micros };
}

inline Napi::Object MakeTimePartsObject(Napi::Env env, duckdb_time_struct time_parts) {
  auto time_parts_obj = Napi::Object::New(env);
  time_parts_obj.Set("hour", Napi::Number::New(env, time_parts.hour));
  time_parts_obj.Set("min", Napi::Number::New(env, time_parts.min));
  time_parts_obj.Set("sec", Napi::Number::New(env, time_parts.sec));
  time_parts_obj.Set("micros", Napi::Number::New(env, time_parts.micros));
  return time_parts_obj;
}

inline duckdb_time_struct GetTimePartsFromObject(Napi::Object time_parts_obj) {
  int8_t hour = time_parts_obj.Get("hour").As<Napi::Number>().Int32Value();
  int8_t min = time_parts_obj.Get("min").As<Napi::Number>().Int32Value();
  int8_t sec = time_parts_obj.Get("sec").As<Napi::Number>().Int32Value();
  int32_t micros = time_parts_obj.Get("micros").As<Napi::Number>().Int32Value();
  return { hour, min, sec, micros };
}

inline Napi::Object MakeTimeNSObject(Napi::Env env, duckdb_time_ns time_ns) {
  auto time_ns_obj = Napi::Object::New(env);
  time_ns_obj.Set("nanos", Napi::BigInt::New(env, time_ns.nanos));
  return time_ns_obj;
}

inline duckdb_time_ns GetTimeNSFromObject(Napi::Env env, Napi::Object time_ns_obj) {
  bool lossless;
  auto nanos = time_ns_obj.Get("nanos").As<Napi::BigInt>().Int64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "nanos out of int64 range");
  }
  return { nanos };
}

inline Napi::Object MakeTimeTZObject(Napi::Env env, duckdb_time_tz time_tz) {
  auto time_tz_obj = Napi::Object::New(env);
  time_tz_obj.Set("bits", Napi::BigInt::New(env, time_tz.bits));
  return time_tz_obj;
}

inline duckdb_time_tz GetTimeTZFromObject(Napi::Env env, Napi::Object time_tz_obj) {
  bool lossless;
  auto bits = time_tz_obj.Get("bits").As<Napi::BigInt>().Uint64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "bits out of uint64 range");
  }
  return { bits };
}

inline Napi::Object MakeTimeTZPartsObject(Napi::Env env, duckdb_time_tz_struct time_tz_parts) {
  auto time_tz_parts_obj = Napi::Object::New(env);
  time_tz_parts_obj.Set("time", MakeTimePartsObject(env, time_tz_parts.time));
  time_tz_parts_obj.Set("offset", Napi::Number::New(env, time_tz_parts.offset));
  return time_tz_parts_obj;
}

// GetTimeTZPartsFromObject not used

inline Napi::Object MakeTimestampObject(Napi::Env env, duckdb_timestamp timestamp) {
  auto timestamp_obj = Napi::Object::New(env);
  timestamp_obj.Set("micros", Napi::BigInt::New(env, timestamp.micros));
  return timestamp_obj;
}

inline duckdb_timestamp GetTimestampFromObject(Napi::Env env, Napi::Object timestamp_obj) {
  bool lossless;
  auto micros = timestamp_obj.Get("micros").As<Napi::BigInt>().Int64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "micros out of int64 range");
  }
  return { micros };
}

inline Napi::Object MakeTimestampSecondsObject(Napi::Env env, duckdb_timestamp_s timestamp) {
  auto timestamp_s_obj = Napi::Object::New(env);
  timestamp_s_obj.Set("seconds", Napi::BigInt::New(env, timestamp.seconds));
  return timestamp_s_obj;
}

inline duckdb_timestamp_s GetTimestampSecondsFromObject(Napi::Env env, Napi::Object timestamp_s_obj) {
  bool lossless;
  auto seconds = timestamp_s_obj.Get("seconds").As<Napi::BigInt>().Int64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "seconds out of int64 range");
  }
  return { seconds };
}

inline Napi::Object MakeTimestampMillisecondsObject(Napi::Env env, duckdb_timestamp_ms timestamp) {
  auto timestamp_ms_obj = Napi::Object::New(env);
  timestamp_ms_obj.Set("millis", Napi::BigInt::New(env, timestamp.millis));
  return timestamp_ms_obj;
}

inline duckdb_timestamp_ms GetTimestampMillisecondsFromObject(Napi::Env env, Napi::Object timestamp_ms_obj) {
  bool lossless;
  auto millis = timestamp_ms_obj.Get("millis").As<Napi::BigInt>().Int64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "millis out of int64 range");
  }
  return { millis };
}

inline Napi::Object MakeTimestampNanosecondsObject(Napi::Env env, duckdb_timestamp_ns timestamp) {
  auto timestamp_ns_obj = Napi::Object::New(env);
  timestamp_ns_obj.Set("nanos", Napi::BigInt::New(env, timestamp.nanos));
  return timestamp_ns_obj;
}

inline duckdb_timestamp_ns GetTimestampNanosecondsFromObject(Napi::Env env, Napi::Object timestamp_ns_obj) {
  bool lossless;
  auto nanos = timestamp_ns_obj.Get("nanos").As<Napi::BigInt>().Int64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "nanos out of int64 range");
  }
  return { nanos };
}

inline Napi::Object MakeTimestampPartsObject(Napi::Env env, duckdb_timestamp_struct timestamp_parts) {
  auto timestamp_parts_obj = Napi::Object::New(env);
  timestamp_parts_obj.Set("date", MakeDatePartsObject(env, timestamp_parts.date));
  timestamp_parts_obj.Set("time", MakeTimePartsObject(env, timestamp_parts.time));
  return timestamp_parts_obj;
}

inline duckdb_timestamp_struct GetTimestampPartsFromObject(Napi::Object timestamp_parts_obj) {
  auto date = GetDatePartsFromObject(timestamp_parts_obj.Get("date").As<Napi::Object>());
  auto time = GetTimePartsFromObject(timestamp_parts_obj.Get("time").As<Napi::Object>());
  return { date, time };
}

inline Napi::Object MakeIntervalObject(Napi::Env env, duckdb_interval interval) {
  auto interval_obj = Napi::Object::New(env);
  interval_obj.Set("months", Napi::Number::New(env, interval.months));
  interval_obj.Set("days", Napi::Number::New(env, interval.days));
  interval_obj.Set("micros", Napi::BigInt::New(env, interval.micros));
  return interval_obj;
}

inline duckdb_interval GetIntervalFromObject(Napi::Env env, Napi::Object interval_obj) {
  int32_t months = interval_obj.Get("months").As<Napi::Number>().Int32Value();
  int32_t days = interval_obj.Get("days").As<Napi::Number>().Int32Value();
  bool lossless;
  int64_t micros = interval_obj.Get("micros").As<Napi::BigInt>().Int64Value(&lossless);
  if (!lossless) {
    throw Napi::Error::New(env, "micros out of int64 range");
  }
  return { months, days, micros };
}

inline duckdb_hugeint GetHugeIntFromBigInt(Napi::Env env, Napi::BigInt bigint) {
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

inline Napi::BigInt MakeBigIntFromHugeInt(Napi::Env env, duckdb_hugeint hugeint) {
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

inline duckdb_uhugeint GetUHugeIntFromBigInt(Napi::Env env, Napi::BigInt bigint) {
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

inline Napi::BigInt MakeBigIntFromUHugeInt(Napi::Env env, duckdb_uhugeint uhugeint) {
  int sign_bit = 0;
  size_t word_count = 2;
  uint64_t words[2];
  words[0] = uhugeint.lower;
  words[1] = uhugeint.upper;
  return Napi::BigInt::New(env, sign_bit, word_count, words);
}

inline duckdb_bignum GetBigNumFromBigInt(Napi::Env env, Napi::BigInt bigint) {
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

inline Napi::BigInt MakeBigIntFromBigNum(Napi::Env env, duckdb_bignum bignum) {
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

inline Napi::Object MakeDecimalObject(Napi::Env env, duckdb_decimal decimal) {
  auto decimal_obj = Napi::Object::New(env);
  decimal_obj.Set("width", Napi::Number::New(env, decimal.width));
  decimal_obj.Set("scale", Napi::Number::New(env, decimal.scale));
  decimal_obj.Set("value", MakeBigIntFromHugeInt(env, decimal.value));
  return decimal_obj;
}

inline duckdb_decimal GetDecimalFromObject(Napi::Env env, Napi::Object decimal_obj) {
  uint8_t width = decimal_obj.Get("width").As<Napi::Number>().Uint32Value();
  uint8_t scale = decimal_obj.Get("scale").As<Napi::Number>().Uint32Value();
  auto value = GetHugeIntFromBigInt(env, decimal_obj.Get("value").As<Napi::BigInt>());
  return { width, scale, value };
}
