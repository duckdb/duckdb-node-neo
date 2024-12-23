import duckdb from '@duckdb/node-bindings';
import { expect } from 'vitest';
import { ExpectedLogicalType } from './ExpectedLogicalType';
import { UTINYINT } from './expectedLogicalTypes';
import {
  ExpectedArrayVector,
  ExpectedDataVector,
  ExpectedListVector,
  ExpectedMapVector,
  ExpectedStructVector,
  ExpectedUnionVector,
  ExpectedVector,
} from './ExpectedVector';
import { expectValidity } from './expectValidity';
import { getListEntry, getValue } from './getValue';

export function expectVector(vector: duckdb.Vector, expectedVector: ExpectedVector, expectedLogicalType: ExpectedLogicalType, vectorName: string) {
  switch (expectedVector.kind) {
    case 'array':
      expectArrayVector(vector, expectedVector, expectedLogicalType, vectorName);
      break;
    case 'data':
      expectDataVector(vector, expectedVector, expectedLogicalType, vectorName);
      break;
    case 'list':
      expectListVector(vector, expectedVector, expectedLogicalType, vectorName);
      break;
    case 'map':
      expectMapVector(vector, expectedVector, expectedLogicalType, vectorName);
      break;
    case 'struct':
      expectStructVector(vector, expectedVector, expectedLogicalType, vectorName);
      break;
    case 'union':
      expectUnionVector(vector, expectedVector, expectedLogicalType, vectorName);
      break;
  }
}

function getVectorValidity(vector: duckdb.Vector, itemCount: number): { validity: BigUint64Array | null, validityBytes: Uint8Array | null } {
  const validityUInt64Count = Math.ceil(itemCount / 64);
  const validityByteCount = validityUInt64Count * 8;
  const validityBytes = duckdb.vector_get_validity(vector, validityByteCount);
  if (!validityBytes) {
    return { validity: null, validityBytes: null };
  }
  const validity = new BigUint64Array(validityBytes.buffer, 0, validityUInt64Count);
  return { validity, validityBytes };
}

function getVectorData(vector: duckdb.Vector, itemCount: number, itemBytes: number) {
  const bytes = duckdb.vector_get_data(vector, itemCount * itemBytes);
  return new DataView(bytes.buffer);
}

function expectArrayVector(vector: duckdb.Vector, expectedVector: ExpectedArrayVector, expectedLogicalType: ExpectedLogicalType, vectorName: string) {
  expect(expectedLogicalType.typeId).toBe(duckdb.Type.ARRAY);
  if (expectedLogicalType.typeId !== duckdb.Type.ARRAY) {
    return;
  }

  const itemCount = expectedVector.itemCount;
  const { validity, validityBytes } = getVectorValidity(vector, itemCount);
  for (let row = 0; row < itemCount; row++) {
    expectValidity(validityBytes, validity, row, expectedVector.validity ? expectedVector.validity[row] : true, `${vectorName} row[${row}]`);
  }

  const childVector = duckdb.array_vector_get_child(vector);
  expectVector(childVector, expectedVector.child, expectedLogicalType.valueType, `${vectorName} array_child`);
}

function expectDataVector(vector: duckdb.Vector, expectedVector: ExpectedDataVector, expectedLogicalType: ExpectedLogicalType, vectorName: string) {
  const itemCount = expectedVector.values.length;
  const { validity, validityBytes } = getVectorValidity(vector, itemCount);
  const dv = getVectorData(vector, itemCount, expectedVector.itemBytes);
  for (let row = 0; row < itemCount; row++) {
    expectValidity(validityBytes, validity, row, expectedVector.validity ? expectedVector.validity[row] : true, `${vectorName} row[${row}]`);
    expect(getValue(expectedLogicalType, validity, dv, row), `${vectorName} row[${row}]`).toStrictEqual(expectedVector.values[row]);
  }
}

function expectListVector(vector: duckdb.Vector, expectedVector: ExpectedListVector, expectedLogicalType: ExpectedLogicalType, vectorName: string) {
  expect(expectedLogicalType.typeId).toBe(duckdb.Type.LIST);
  if (expectedLogicalType.typeId !== duckdb.Type.LIST) {
    return;
  }

  const itemCount = expectedVector.entries.length;
  const { validity, validityBytes } = getVectorValidity(vector, itemCount);
  const entriesDV = getVectorData(vector, itemCount, 16);
  for (let row = 0; row < itemCount; row++) {
    expectValidity(validityBytes, validity, row, expectedVector.validity ? expectedVector.validity[row] : true, `${vectorName} row[${row}]`);
    expect(getListEntry(validity, entriesDV, row)).toStrictEqual(expectedVector.entries[row]);
  }

  const childItemCount = duckdb.list_vector_get_size(vector);
  const childVector = duckdb.list_vector_get_child(vector);
  expect(childItemCount).toBe(expectedVector.childItemCount);
  expectVector(childVector, expectedVector.child, expectedLogicalType.valueType, `${vectorName} list_child`);
}

function expectMapVector(vector: duckdb.Vector, expectedVector: ExpectedMapVector, expectedLogicalType: ExpectedLogicalType, vectorName: string) {
  expect(expectedLogicalType.typeId).toBe(duckdb.Type.MAP);
  if (expectedLogicalType.typeId !== duckdb.Type.MAP) {
    return;
  }

  const itemCount = expectedVector.entries.length;
  const { validity, validityBytes } = getVectorValidity(vector, itemCount);
  const entriesDV = getVectorData(vector, itemCount, 16);
  for (let row = 0; row < itemCount; row++) {
    expectValidity(validityBytes, validity, row, expectedVector.validity ? expectedVector.validity[row] : true, `${vectorName} row[${row}]`);
    expect(getListEntry(validity, entriesDV, row)).toStrictEqual(expectedVector.entries[row]);
  }

  const childItemCount = duckdb.list_vector_get_size(vector);
  const childVector = duckdb.list_vector_get_child(vector);
  expect(childItemCount).toBe(2);
  const keysVector = duckdb.struct_vector_get_child(childVector, 0);
  const valuesVector = duckdb.struct_vector_get_child(childVector, 1);
  expectVector(keysVector, expectedVector.keys, expectedLogicalType.keyType, `${vectorName} map_keys`);
  expectVector(valuesVector, expectedVector.values, expectedLogicalType.valueType, `${vectorName} map_values`);
}

function expectStructVector(vector: duckdb.Vector, expectedVector: ExpectedStructVector, expectedLogicalType: ExpectedLogicalType, vectorName: string) {
  expect(expectedLogicalType.typeId).toBe(duckdb.Type.STRUCT);
  if (expectedLogicalType.typeId !== duckdb.Type.STRUCT) {
    return;
  }

  const itemCount = expectedVector.itemCount;
  const { validity, validityBytes } = getVectorValidity(vector, itemCount);
  for (let row = 0; row < itemCount; row++) {
    expectValidity(validityBytes, validity, row, expectedVector.validity ? expectedVector.validity[row] : true, `${vectorName} row[${row}]`);
  }

  for (let i = 0; i < expectedVector.children.length; i++) {
    const childVector = duckdb.struct_vector_get_child(vector, i);
    expectVector(childVector, expectedVector.children[i], expectedLogicalType.entries[i].type, `${vectorName} struct_child[${i}]`);
  }
}

function expectUnionVector(vector: duckdb.Vector, expectedVector: ExpectedUnionVector, expectedLogicalType: ExpectedLogicalType, vectorName: string) {
  expect(expectedLogicalType.typeId).toBe(duckdb.Type.UNION);
  if (expectedLogicalType.typeId !== duckdb.Type.UNION) {
    return;
  }

  const tagsVector = duckdb.struct_vector_get_child(vector, 0);
  expectVector(tagsVector, expectedVector.children[0], UTINYINT, `${vectorName} union_tags`);
  for (let i = 1; i < expectedVector.children.length; i++) {
    const childVector = duckdb.struct_vector_get_child(vector, i);
    expectVector(childVector, expectedVector.children[i], expectedLogicalType.alternatives[i-1].type, `${vectorName} union_child[${i}]`);
  }
}
