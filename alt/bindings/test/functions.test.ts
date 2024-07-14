import duckdb from 'duckdb';
import { expect, suite, test } from 'vitest';

suite('functions', () => {
  test('library_version', () => {
    expect(duckdb.library_version()).toBe('v1.0.0');
  });
});
