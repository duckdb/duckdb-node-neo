import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';

// The C API takes a validity mask as a bare pointer and indexes into it
// without checking, so a row index past the end read or wrote past the buffer:
// silently for a small overrun, with a segfault for a large one. The mask
// arrives here as a Uint8Array, so its extent is known exactly.
suite('validity bounds', () => {
  // 64 bytes of mask cover 512 rows.
  const mask = () => new Uint8Array(64).fill(0xff);

  test('reading past the end of the mask is refused', () => {
    expect(duckdb.validity_row_is_valid(mask(), 511)).toBe(true);
    expect(() => duckdb.validity_row_is_valid(mask(), 512)).toThrowError(
      'row index 512 is out of range for a validity mask of 64 bytes (512 rows)'
    );
  });

  test('writing past the end of the mask is refused', () => {
    // This one silently corrupted whatever followed the buffer.
    expect(() => duckdb.validity_set_row_invalid(mask(), 512)).toThrowError(
      'is out of range for a validity mask'
    );
    expect(() => duckdb.validity_set_row_valid(mask(), 100000)).toThrowError(
      'is out of range for a validity mask'
    );
    expect(() =>
      duckdb.validity_set_row_validity(mask(), 600, true)
    ).toThrowError('is out of range for a validity mask');
    // And this one segfaulted.
    expect(() => duckdb.validity_set_row_invalid(mask(), 2 ** 31)).toThrowError(
      'is out of range for a validity mask'
    );
  });

  test('rows within the mask still read and write', () => {
    const m = new Uint8Array(64).fill(0);
    duckdb.validity_set_row_valid(m, 0);
    duckdb.validity_set_row_valid(m, 511);
    expect(duckdb.validity_row_is_valid(m, 0)).toBe(true);
    expect(duckdb.validity_row_is_valid(m, 511)).toBe(true);
    expect(duckdb.validity_row_is_valid(m, 1)).toBe(false);
    duckdb.validity_set_row_invalid(m, 0);
    expect(duckdb.validity_row_is_valid(m, 0)).toBe(false);
  });

  test('a null mask means every row is valid, at any index', () => {
    // duckdb_validity_row_is_valid treats a null mask as all-valid, so there
    // is no buffer to bound and the index does not matter.
    expect(duckdb.validity_row_is_valid(null, 0)).toBe(true);
    expect(duckdb.validity_row_is_valid(null, 99999)).toBe(true);
  });
});
