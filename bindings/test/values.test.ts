import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { expectLogicalType } from './utils/expectLogicalType';
import {
  BIGINT,
  BLOB,
  BOOLEAN,
  DATE,
  DOUBLE,
  FLOAT,
  HUGEINT,
  INTEGER,
  INTERVAL,
  SMALLINT,
  TIME,
  TIME_TZ,
  TIMESTAMP,
  TINYINT,
  UBIGINT,
  UHUGEINT,
  UINTEGER,
  USMALLINT,
  UTINYINT,
  VARCHAR,
} from './utils/expectedLogicalTypes';

suite('values', () => {
  test('bool', () => {
    const input = true;
    const bool_value = duckdb.create_bool(input);
    try {
      expectLogicalType(duckdb.get_value_type(bool_value), BOOLEAN);
      expect(duckdb.get_bool(bool_value)).toBe(input);
    } finally {
      duckdb.destroy_value(bool_value);
    }
  });
  test('int8', () => {
    const input = 127;
    const int8_value = duckdb.create_int8(input);
    try {
      expectLogicalType(duckdb.get_value_type(int8_value), TINYINT);
      expect(duckdb.get_int8(int8_value)).toBe(input);
    } finally {
      duckdb.destroy_value(int8_value);
    }
  });
  test('uint8', () => {
    const input = 255;
    const uint8_value = duckdb.create_uint8(input);
    try {
      expectLogicalType(duckdb.get_value_type(uint8_value), UTINYINT);
      expect(duckdb.get_uint8(uint8_value)).toBe(input);
    } finally {
      duckdb.destroy_value(uint8_value);
    }
  });
  test('int16', () => {
    const input = 32767;
    const int16_value = duckdb.create_int16(input);
    try {
      expectLogicalType(duckdb.get_value_type(int16_value), SMALLINT);
      expect(duckdb.get_int16(int16_value)).toBe(input);
    } finally {
      duckdb.destroy_value(int16_value);
    }
  });
  test('uint16', () => {
    const input = 65535;
    const uint16_value = duckdb.create_uint16(input);
    try {
      expectLogicalType(duckdb.get_value_type(uint16_value), USMALLINT);
      expect(duckdb.get_uint16(uint16_value)).toBe(input);
    } finally {
      duckdb.destroy_value(uint16_value);
    }
  });
  test('int32', () => {
    const input = 2147483647;
    const int32_value = duckdb.create_int32(input);
    try {
      expectLogicalType(duckdb.get_value_type(int32_value), INTEGER);
      expect(duckdb.get_int32(int32_value)).toBe(input);
    } finally {
      duckdb.destroy_value(int32_value);
    }
  });
  test('uint32', () => {
    const input = 4294967295;
    const uint32_value = duckdb.create_uint32(input);
    try {
      expectLogicalType(duckdb.get_value_type(uint32_value), UINTEGER);
      expect(duckdb.get_uint32(uint32_value)).toBe(input);
    } finally {
      duckdb.destroy_value(uint32_value);
    }
  });
  test('int64', () => {
    const input = 9223372036854775807n;
    const int64_value = duckdb.create_int64(input);
    try {
      expectLogicalType(duckdb.get_value_type(int64_value), BIGINT);
      expect(duckdb.get_int64(int64_value)).toBe(input);
    } finally {
      duckdb.destroy_value(int64_value);
    }
  });
  test('uint64', () => {
    const input = 18446744073709551615n;
    const uint64_value = duckdb.create_uint64(input);
    try {
      expectLogicalType(duckdb.get_value_type(uint64_value), UBIGINT);
      expect(duckdb.get_uint64(uint64_value)).toBe(input);
    } finally {
      duckdb.destroy_value(uint64_value);
    }
  });
  test('hugeint', () => {
    const input = 170141183460469231731687303715884105727n;
    const hugeint_value = duckdb.create_hugeint(input);
    try {
      expectLogicalType(duckdb.get_value_type(hugeint_value), HUGEINT);
      expect(duckdb.get_hugeint(hugeint_value)).toBe(input);
    } finally {
      duckdb.destroy_value(hugeint_value);
    }
  });
  test('uhugeint', () => {
    const input = 340282366920938463463374607431768211455n;
    const uhugeint_value = duckdb.create_uhugeint(input);
    try {
      expectLogicalType(duckdb.get_value_type(uhugeint_value), UHUGEINT);
      expect(duckdb.get_uhugeint(uhugeint_value)).toBe(input);
    } finally {
      duckdb.destroy_value(uhugeint_value);
    }
  });
  test('float', () => {
    const input = 3.4028234663852886e+38;
    const float_value = duckdb.create_float(input);
    try {
      expectLogicalType(duckdb.get_value_type(float_value), FLOAT);
      expect(duckdb.get_float(float_value)).toBe(input);
    } finally {
      duckdb.destroy_value(float_value);
    }
  });
  test('double', () => {
    const input = 1.7976931348623157e+308;
    const double_value = duckdb.create_double(input);
    try {
      expectLogicalType(duckdb.get_value_type(double_value), DOUBLE);
      expect(duckdb.get_double(double_value)).toBe(input);
    } finally {
      duckdb.destroy_value(double_value);
    }
  });
  test('date', () => {
    const input = { days: 2147483646 };
    const date_value = duckdb.create_date(input);
    try {
      expectLogicalType(duckdb.get_value_type(date_value), DATE);
      expect(duckdb.get_date(date_value)).toStrictEqual(input);
    } finally {
      duckdb.destroy_value(date_value);
    }
  });
  test('time', () => {
    const input = { micros: 86400000000 };
    const time_value = duckdb.create_time(input);
    try {
      expectLogicalType(duckdb.get_value_type(time_value), TIME);
      expect(duckdb.get_time(time_value)).toStrictEqual(input);
    } finally {
      duckdb.destroy_value(time_value);
    }
  });
  test('time_tz', () => {
    const input = { bits: 1449551462400115198n };
    const time_tz_value = duckdb.create_time_tz_value(input);
    try {
      expectLogicalType(duckdb.get_value_type(time_tz_value), TIME_TZ);
      expect(duckdb.get_time_tz(time_tz_value)).toStrictEqual(input);
    } finally {
      duckdb.destroy_value(time_tz_value);
    }
  });
  test('timestamp', () => {
    const input = { micros: 9223372036854775806n };
    const timestamp_value = duckdb.create_timestamp(input);
    try {
      expectLogicalType(duckdb.get_value_type(timestamp_value), TIMESTAMP);
      expect(duckdb.get_timestamp(timestamp_value)).toStrictEqual(input);
    } finally {
      duckdb.destroy_value(timestamp_value);
    }
  });
  test('interval', () => {
    const input = { months: 999, days: 999, micros: 999999999n };
    const interval_value = duckdb.create_interval(input);
    try {
      expectLogicalType(duckdb.get_value_type(interval_value), INTERVAL);
      expect(duckdb.get_interval(interval_value)).toStrictEqual(input);
    } finally {
      duckdb.destroy_value(interval_value);
    }
  });
  test('blob', () => {
    const input = Buffer.from('thisisalongblob\x00withnullbytes');
    const blob_value = duckdb.create_blob(input);
    try {
      expectLogicalType(duckdb.get_value_type(blob_value), BLOB);
      expect(duckdb.get_blob(blob_value)).toStrictEqual(input);
    } finally {
      duckdb.destroy_value(blob_value);
    }
  });
  test('varchar', () => {
    const input = 'varchar_text';
    const varchar_value = duckdb.create_varchar(input);
    try {
      expectLogicalType(duckdb.get_value_type(varchar_value), VARCHAR);
      expect(duckdb.get_varchar(varchar_value)).toBe(input);
    } finally {
      duckdb.destroy_value(varchar_value);
    }
  });
});
