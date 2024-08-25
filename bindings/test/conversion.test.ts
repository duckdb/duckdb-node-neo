
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
});
