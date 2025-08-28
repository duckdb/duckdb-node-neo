import duckdb from '@databrainhq/node-bindings';
import { expect } from 'vitest';
import { isValid } from './isValid';

export function expectValidity(
  validity_bytes: Uint8Array | null,
  validity: BigUint64Array | null,
  bit: number,
  expected: boolean,
  vectorName: string,
) {
  expect(
    duckdb.validity_row_is_valid(validity_bytes, bit),
    `${vectorName} validity_bytes_bit[${bit}]`,
  ).toBe(expected);
  expect(isValid(validity, bit), `${vectorName} validity_bit[${bit}]`).toBe(
    expected,
  );
}
