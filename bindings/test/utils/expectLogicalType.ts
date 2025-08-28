import duckdb from '@databrainhq/node-bindings';
import { expect } from 'vitest';
import { ExpectedLogicalType } from './ExpectedLogicalType';

export function expectLogicalType(
  logical_type: duckdb.LogicalType,
  expectedLogicalType: ExpectedLogicalType,
  message?: string,
) {
  expect(duckdb.get_type_id(logical_type), message).toBe(
    expectedLogicalType.typeId,
  );
  switch (expectedLogicalType.typeId) {
    case duckdb.Type.ARRAY:
      expectLogicalType(
        duckdb.array_type_child_type(logical_type),
        expectedLogicalType.valueType,
      );
      expect(duckdb.array_type_array_size(logical_type)).toBe(
        expectedLogicalType.size,
      );
      break;
    case duckdb.Type.DECIMAL:
      expect(duckdb.decimal_width(logical_type)).toBe(
        expectedLogicalType.width,
      );
      expect(duckdb.decimal_scale(logical_type)).toBe(
        expectedLogicalType.scale,
      );
      expect(duckdb.decimal_internal_type(logical_type)).toBe(
        expectedLogicalType.internalType,
      );
      break;
    case duckdb.Type.ENUM:
      {
        expect(duckdb.enum_internal_type(logical_type)).toBe(
          expectedLogicalType.internalType,
        );
        expect(duckdb.enum_dictionary_size(logical_type)).toBe(
          expectedLogicalType.values.length,
        );
        for (let i = 0; i < expectedLogicalType.values.length; i++) {
          expect(duckdb.enum_dictionary_value(logical_type, i)).toBe(
            expectedLogicalType.values[i],
          );
        }
      }
      break;
    case duckdb.Type.LIST:
      expectLogicalType(
        duckdb.list_type_child_type(logical_type),
        expectedLogicalType.valueType,
      );
      break;
    case duckdb.Type.MAP:
      expectLogicalType(
        duckdb.map_type_key_type(logical_type),
        expectedLogicalType.keyType,
      );
      expectLogicalType(
        duckdb.map_type_value_type(logical_type),
        expectedLogicalType.valueType,
      );
      break;
    case duckdb.Type.STRUCT:
      expect(duckdb.struct_type_child_count(logical_type)).toBe(
        expectedLogicalType.entries.length,
      );
      for (let i = 0; i < expectedLogicalType.entries.length; i++) {
        expect(duckdb.struct_type_child_name(logical_type, i)).toBe(
          expectedLogicalType.entries[i].name,
        );
        expectLogicalType(
          duckdb.struct_type_child_type(logical_type, i),
          expectedLogicalType.entries[i].type,
        );
      }
      break;
    case duckdb.Type.UNION:
      expect(duckdb.union_type_member_count(logical_type)).toBe(
        expectedLogicalType.alternatives.length,
      );
      for (let i = 0; i < expectedLogicalType.alternatives.length; i++) {
        expect(duckdb.union_type_member_name(logical_type, i)).toBe(
          expectedLogicalType.alternatives[i].tag,
        );
        expectLogicalType(
          duckdb.union_type_member_type(logical_type, i),
          expectedLogicalType.alternatives[i].type,
        );
      }
      break;
  }
}
