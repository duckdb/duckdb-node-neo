
import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';

suite('conversion', () => {
  suite('from_date', () => {
    test('mid-range', () => {
      expect(duckdb.from_date({ days: 19877 })).toStrictEqual({ year: 2024, month: 6, day: 3 });
    });
    test('max', () => {
      expect(duckdb.from_date({ days: 0x7FFFFFFE })).toStrictEqual({ year: 5881580, month: 7, day: 10 });
    });
    test('min', () => {
      expect(duckdb.from_date({ days: -0x7FFFFFFE })).toStrictEqual({ year: -5877641, month: 6, day: 25 });
    });
  });
  suite('to_date', () => {
    test('mid-range', () => {
      expect(duckdb.to_date({ year: 2024, month: 6, day: 3 })).toStrictEqual({ days: 19877 });
    });
    test('max', () => {
      expect(duckdb.to_date({ year: 5881580, month: 7, day: 10 })).toStrictEqual({ days: 0x7FFFFFFE });
    });
    test('min', () => {
      expect(duckdb.to_date({ year: -5877641, month: 6, day: 25 })).toStrictEqual({ days: -0x7FFFFFFE });
    });
  });
  suite('is_finite_date', () => {
    test('finite', () => {
      expect(duckdb.is_finite_date({ days: 19877 })).toBe(true);
    });
    test('infinity', () => {
      expect(duckdb.is_finite_date({ days: 0x7FFFFFFF })).toBe(false);
    });
    test('-infinity', () => {
      expect(duckdb.is_finite_date({ days: -0x7FFFFFFF })).toBe(false);
    });
  });
  suite('from_time', () => {
    test('mid-range', () => {
      // 45296789123 = 1000000 * (60 * (60 * 12 + 34) + 56) + 789123 = 12:34:56.789123
      expect(duckdb.from_time({ micros: 45296789123 })).toStrictEqual({ hour: 12, min: 34, sec: 56, micros: 789123 });
    });
    test('min', () => {
      expect(duckdb.from_time({ micros: 0 })).toStrictEqual({ hour: 0, min: 0, sec: 0, micros: 0 });
    });
    test('max', () => {
      // 86400000000 = 1000000 * (60 * (60 * 24 + 0) + 0) + 0 = 24:00:00.000000
      expect(duckdb.from_time({ micros: 86400000000 })).toStrictEqual({ hour: 24, min: 0, sec: 0, micros: 0 });
    });
  });
  suite('create_time_tz', () => {
    // See datetime.hpp for format of "bits" field. Summary:
    //   40 bits for micros, then 24 bits for encoded offset in seconds.
    //   Max absolute unencoded offset = 15:59:59 = 60 * (60 * 15 + 59) + 59 = 57599.
    //   Encoded offset is unencoded offset inverted then shifted (by +57599) to unsigned.
    //   Max unencoded offset = 57599 -> -57599 -> 0 encoded.
    //   Min unencoded offset = -57599 -> 57599 -> 115198 encoded.
    test('mid-range', () => {
      // 45296789123 = 1000000 * (60 * (60 * 12 + 34) + 56) + 789123 = 12:34:56.789123
      // 759954015223079167n = (45296789123n << 24n) + 57599n
      expect(duckdb.create_time_tz(45296789123, 0)).toStrictEqual({ bits: 759954015223079167n });
    });
    test('min', () => {
      expect(duckdb.create_time_tz(0, 57599)).toStrictEqual({ bits: 0n });
    });
    test('max', () => {
      // 1449551462400115198n = (86400000000n << 24n) + 2n * 57599n
      expect(duckdb.create_time_tz(86400000000, -57599)).toStrictEqual({ bits: 1449551462400115198n });
    });
  });
  suite('from_time_tz', () => {
    test('mid-range', () => {
      expect(duckdb.from_time_tz({ bits: 759954015223079167n })).toStrictEqual({ time: { hour: 12, min: 34, sec: 56, micros: 789123 }, offset: 0 });
    });
    test('min', () => {
      expect(duckdb.from_time_tz({ bits: 0n })).toStrictEqual({ time: { hour: 0, min: 0, sec: 0, micros: 0 }, offset: 57599 });
    });
    test('max', () => {
      expect(duckdb.from_time_tz({ bits: 1449551462400115198n })).toStrictEqual({ time: { hour: 24, min: 0, sec: 0, micros: 0 }, offset: -57599 });
    });
    test('out of range', () => {
      expect(() => duckdb.from_time_tz({ bits: 2n ** 64n })).toThrowError('bits out of uint64 range');
    });
  });
  suite('to_time', () => {
    test('mid-range', () => {
      expect(duckdb.to_time({ hour: 12, min: 34, sec: 56, micros: 789123 })).toStrictEqual({ micros: 45296789123 });
    });
    test('min', () => {
      expect(duckdb.to_time({ hour: 0, min: 0, sec: 0, micros: 0 })).toStrictEqual({ micros: 0 });
    });
    test('max', () => {
      expect(duckdb.to_time({ hour: 24, min: 0, sec: 0, micros: 0 })).toStrictEqual({ micros: 86400000000 });
    });
  });
});
