import duckdb from '@duckdb/node-bindings';
import { expect } from 'vitest';

export function isValid(validity: BigUint64Array, bit: number): boolean {
  return (validity[Math.floor(bit / 64)] & (1n << BigInt(bit % 64))) !== 0n;
}

export function expectValidity(validity_bytes: Uint8Array, validity: BigUint64Array, bit: number, expected: boolean) {
  expect(duckdb.validity_row_is_valid(validity_bytes, bit)).toBe(expected);
  expect(isValid(validity, bit)).toBe(expected);
}
