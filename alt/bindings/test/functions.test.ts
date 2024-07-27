import duckdb from 'duckdb';
import { expect, suite, test } from 'vitest';

suite('functions', () => {
  test('sizeof_bool', () => {
    expect(duckdb.sizeof_bool).toBe(1);
  });
  test('library_version', () => {
    expect(duckdb.library_version()).toBe('v1.0.0');
  });
  test('vector_size', () => {
    expect(duckdb.vector_size()).toBe(2048);
  });
});
