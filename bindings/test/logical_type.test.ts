import duckdb from '@databrainhq/node-bindings';
import { expect, suite, test } from 'vitest';

suite('logical_type', () => {
  test('create, get id, get/set alias, and destroy', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    expect(duckdb.get_type_id(int_type)).toBe(duckdb.Type.INTEGER);
    expect(duckdb.logical_type_get_alias(int_type)).toBeNull();
    duckdb.logical_type_set_alias(int_type, 'my_logical_type');
    expect(duckdb.logical_type_get_alias(int_type)).toBe('my_logical_type');
  });
  test('array', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const array_type = duckdb.create_array_type(int_type, 3);
    expect(duckdb.get_type_id(array_type)).toBe(duckdb.Type.ARRAY);
    expect(duckdb.logical_type_get_alias(array_type)).toBeNull();
    expect(duckdb.array_type_array_size(array_type)).toBe(3);
    const child_type = duckdb.array_type_child_type(array_type);
    expect(duckdb.get_type_id(child_type)).toBe(duckdb.Type.INTEGER);
  });
  test('decimal (SMALLINT)', () => {
    const decimal_type = duckdb.create_decimal_type(4, 1);
    expect(duckdb.get_type_id(decimal_type)).toBe(duckdb.Type.DECIMAL);
    expect(duckdb.logical_type_get_alias(decimal_type)).toBeNull();
    expect(duckdb.decimal_width(decimal_type)).toBe(4);
    expect(duckdb.decimal_scale(decimal_type)).toBe(1);
    expect(duckdb.decimal_internal_type(decimal_type)).toBe(
      duckdb.Type.SMALLINT,
    );
  });
  test('decimal (INTEGER)', () => {
    const decimal_type = duckdb.create_decimal_type(9, 4);
    expect(duckdb.get_type_id(decimal_type)).toBe(duckdb.Type.DECIMAL);
    expect(duckdb.logical_type_get_alias(decimal_type)).toBeNull();
    expect(duckdb.decimal_width(decimal_type)).toBe(9);
    expect(duckdb.decimal_scale(decimal_type)).toBe(4);
    expect(duckdb.decimal_internal_type(decimal_type)).toBe(
      duckdb.Type.INTEGER,
    );
  });
  test('decimal (BIGINT)', () => {
    const decimal_type = duckdb.create_decimal_type(18, 6);
    expect(duckdb.get_type_id(decimal_type)).toBe(duckdb.Type.DECIMAL);
    expect(duckdb.logical_type_get_alias(decimal_type)).toBeNull();
    expect(duckdb.decimal_width(decimal_type)).toBe(18);
    expect(duckdb.decimal_scale(decimal_type)).toBe(6);
    expect(duckdb.decimal_internal_type(decimal_type)).toBe(duckdb.Type.BIGINT);
  });
  test('decimal (HUGEINT)', () => {
    const decimal_type = duckdb.create_decimal_type(38, 10);
    expect(duckdb.get_type_id(decimal_type)).toBe(duckdb.Type.DECIMAL);
    expect(duckdb.logical_type_get_alias(decimal_type)).toBeNull();
    expect(duckdb.decimal_width(decimal_type)).toBe(38);
    expect(duckdb.decimal_scale(decimal_type)).toBe(10);
    expect(duckdb.decimal_internal_type(decimal_type)).toBe(
      duckdb.Type.HUGEINT,
    );
  });
  test('enum (small)', () => {
    const enum_type = duckdb.create_enum_type(['DUCK_DUCK_ENUM', 'GOOSE']);
    expect(duckdb.get_type_id(enum_type)).toBe(duckdb.Type.ENUM);
    expect(duckdb.logical_type_get_alias(enum_type)).toBeNull();
    expect(duckdb.enum_internal_type(enum_type)).toBe(duckdb.Type.UTINYINT);
    expect(duckdb.enum_dictionary_size(enum_type)).toBe(2);
    expect(duckdb.enum_dictionary_value(enum_type, 0)).toBe('DUCK_DUCK_ENUM');
    expect(duckdb.enum_dictionary_value(enum_type, 1)).toBe('GOOSE');
  });
  test('enum (medium)', () => {
    const enum_type = duckdb.create_enum_type(
      Array.from({ length: 300 }).map((_, i) => `enum_${i}`),
    );
    expect(duckdb.get_type_id(enum_type)).toBe(duckdb.Type.ENUM);
    expect(duckdb.logical_type_get_alias(enum_type)).toBeNull();
    expect(duckdb.enum_internal_type(enum_type)).toBe(duckdb.Type.USMALLINT);
    expect(duckdb.enum_dictionary_size(enum_type)).toBe(300);
    expect(duckdb.enum_dictionary_value(enum_type, 0)).toBe('enum_0');
    expect(duckdb.enum_dictionary_value(enum_type, 299)).toBe('enum_299');
  });
  test('enum (large)', () => {
    const enum_type = duckdb.create_enum_type(
      Array.from({ length: 70000 }).map((_, i) => `enum_${i}`),
    );
    expect(duckdb.get_type_id(enum_type)).toBe(duckdb.Type.ENUM);
    expect(duckdb.logical_type_get_alias(enum_type)).toBeNull();
    expect(duckdb.enum_internal_type(enum_type)).toBe(duckdb.Type.UINTEGER);
    expect(duckdb.enum_dictionary_size(enum_type)).toBe(70000);
    expect(duckdb.enum_dictionary_value(enum_type, 0)).toBe('enum_0');
    expect(duckdb.enum_dictionary_value(enum_type, 69999)).toBe('enum_69999');
  });
  test('empty enum', () => {
    const enum_type = duckdb.create_enum_type([]);
    expect(duckdb.get_type_id(enum_type)).toBe(duckdb.Type.ENUM);
    expect(duckdb.logical_type_get_alias(enum_type)).toBeNull();
    expect(duckdb.enum_internal_type(enum_type)).toBe(duckdb.Type.UTINYINT);
    expect(duckdb.enum_dictionary_size(enum_type)).toBe(0);
  });
  test('list', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const list_type = duckdb.create_list_type(int_type);
    expect(duckdb.get_type_id(list_type)).toBe(duckdb.Type.LIST);
    expect(duckdb.logical_type_get_alias(list_type)).toBeNull();
    const child_type = duckdb.list_type_child_type(list_type);
    expect(duckdb.get_type_id(child_type)).toBe(duckdb.Type.INTEGER);
  });
  test('map', () => {
    const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const map_type = duckdb.create_map_type(varchar_type, int_type);
    expect(duckdb.get_type_id(map_type)).toBe(duckdb.Type.MAP);
    expect(duckdb.logical_type_get_alias(map_type)).toBeNull();
    const key_type = duckdb.map_type_key_type(map_type);
    expect(duckdb.get_type_id(key_type)).toBe(duckdb.Type.VARCHAR);
    const value_type = duckdb.map_type_value_type(map_type);
    expect(duckdb.get_type_id(value_type)).toBe(duckdb.Type.INTEGER);
  });
  test('struct', () => {
    const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
    const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
    const struct_type = duckdb.create_struct_type(
      [int_type, varchar_type],
      ['a', 'b'],
    );
    expect(duckdb.get_type_id(struct_type)).toBe(duckdb.Type.STRUCT);
    expect(duckdb.logical_type_get_alias(struct_type)).toBeNull();
    expect(duckdb.struct_type_child_count(struct_type)).toBe(2);
    expect(duckdb.struct_type_child_name(struct_type, 0)).toBe('a');
    expect(duckdb.struct_type_child_name(struct_type, 1)).toBe('b');
    const member_type_0 = duckdb.struct_type_child_type(struct_type, 0);
    expect(duckdb.get_type_id(member_type_0)).toBe(duckdb.Type.INTEGER);
    const member_type_1 = duckdb.struct_type_child_type(struct_type, 1);
    expect(duckdb.get_type_id(member_type_1)).toBe(duckdb.Type.VARCHAR);
  });
  test('empty struct', () => {
    const struct_type = duckdb.create_struct_type([], []);
    expect(duckdb.get_type_id(struct_type)).toBe(duckdb.Type.STRUCT);
    expect(duckdb.logical_type_get_alias(struct_type)).toBeNull();
    expect(duckdb.struct_type_child_count(struct_type)).toBe(0);
  });
  test('union', () => {
    const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
    const smallint_type = duckdb.create_logical_type(duckdb.Type.SMALLINT);
    const union_type = duckdb.create_union_type(
      [varchar_type, smallint_type],
      ['name', 'age'],
    );
    expect(duckdb.get_type_id(union_type)).toBe(duckdb.Type.UNION);
    expect(duckdb.logical_type_get_alias(union_type)).toBeNull();
    expect(duckdb.union_type_member_count(union_type)).toBe(2);
    expect(duckdb.union_type_member_name(union_type, 0)).toBe('name');
    expect(duckdb.union_type_member_name(union_type, 1)).toBe('age');
    const member_type_0 = duckdb.union_type_member_type(union_type, 0);
    expect(duckdb.get_type_id(member_type_0)).toBe(duckdb.Type.VARCHAR);
    const member_type_1 = duckdb.union_type_member_type(union_type, 1);
    expect(duckdb.get_type_id(member_type_1)).toBe(duckdb.Type.SMALLINT);
  });
  test('empty union', () => {
    const union_type = duckdb.create_union_type([], []);
    expect(duckdb.get_type_id(union_type)).toBe(duckdb.Type.UNION);
    expect(duckdb.logical_type_get_alias(union_type)).toBeNull();
    expect(duckdb.union_type_member_count(union_type)).toBe(0);
  });
});
