import duckdb from 'duckdb';
import { expect, suite, test } from 'vitest';

suite('logical_type', () => {
  test('create, get, and destroy', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    try {
      expect(duckdb.get_type_id(int_type)).toBe(duckdb.Type.INTEGER);
      expect(duckdb.logical_type_get_alias(int_type)).toBeNull();
    } finally {
      duckdb.destroy_logical_type(int_type);
    }
  });
  test('decimal (SMALLINT)', () => {
    const decimal_type = duckdb.create_decimal_type(4, 1);
    try {
      expect(duckdb.get_type_id(decimal_type)).toBe(duckdb.Type.DECIMAL);
      expect(duckdb.logical_type_get_alias(decimal_type)).toBeNull();
      expect(duckdb.decimal_width(decimal_type)).toBe(4);
      expect(duckdb.decimal_scale(decimal_type)).toBe(1);
      expect(duckdb.decimal_internal_type(decimal_type)).toBe(duckdb.Type.SMALLINT);
    } finally {
      duckdb.destroy_logical_type(decimal_type);
    }
  });
  test('decimal (INTEGER)', () => {
    const decimal_type = duckdb.create_decimal_type(9, 4);
    try {
      expect(duckdb.get_type_id(decimal_type)).toBe(duckdb.Type.DECIMAL);
      expect(duckdb.logical_type_get_alias(decimal_type)).toBeNull();
      expect(duckdb.decimal_width(decimal_type)).toBe(9);
      expect(duckdb.decimal_scale(decimal_type)).toBe(4);
      expect(duckdb.decimal_internal_type(decimal_type)).toBe(duckdb.Type.INTEGER);
    } finally {
      duckdb.destroy_logical_type(decimal_type);
    }
  });
  test('decimal (BIGINT)', () => {
    const decimal_type = duckdb.create_decimal_type(18, 6);
    try {
      expect(duckdb.get_type_id(decimal_type)).toBe(duckdb.Type.DECIMAL);
      expect(duckdb.logical_type_get_alias(decimal_type)).toBeNull();
      expect(duckdb.decimal_width(decimal_type)).toBe(18);
      expect(duckdb.decimal_scale(decimal_type)).toBe(6);
      expect(duckdb.decimal_internal_type(decimal_type)).toBe(duckdb.Type.BIGINT);
    } finally {
      duckdb.destroy_logical_type(decimal_type);
    }
  });
  test('decimal (HUGEINT)', () => {
    const decimal_type = duckdb.create_decimal_type(38, 10);
    try {
      expect(duckdb.get_type_id(decimal_type)).toBe(duckdb.Type.DECIMAL);
      expect(duckdb.logical_type_get_alias(decimal_type)).toBeNull();
      expect(duckdb.decimal_width(decimal_type)).toBe(38);
      expect(duckdb.decimal_scale(decimal_type)).toBe(10);
      expect(duckdb.decimal_internal_type(decimal_type)).toBe(duckdb.Type.HUGEINT);
    } finally {
      duckdb.destroy_logical_type(decimal_type);
    }
  });
  test('list', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    try {
      const list_type = duckdb.create_list_type(int_type);
      try {
        expect(duckdb.get_type_id(list_type)).toBe(duckdb.Type.LIST);
        expect(duckdb.logical_type_get_alias(list_type)).toBeNull();
        const child_type = duckdb.list_type_child_type(list_type);
        try {
          expect(duckdb.get_type_id(child_type)).toBe(duckdb.Type.INTEGER);
        } finally {
          duckdb.destroy_logical_type(child_type);
        }
      } finally {
        duckdb.destroy_logical_type(list_type);
      }
    } finally {
      duckdb.destroy_logical_type(int_type);
    }
  });
});
