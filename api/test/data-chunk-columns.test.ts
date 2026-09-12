import { expect, test } from 'vitest';
import { DuckDBDataChunk, INTEGER, VARCHAR } from '../src';

test('setColumns refuses a column count that does not match the chunk', () => {
  const chunk = DuckDBDataChunk.create([INTEGER, VARCHAR]);
  // Without this the second vector is never written and never flushed while
  // the chunk reports two rows, and reading or appending it then crashes the
  // process instead of raising.
  expect(() => chunk.setColumns([[1, 2]])).toThrowError(
    'Provided number of columns (1) does not match chunk column count (2)'
  );
  expect(() => chunk.setColumns([[1, 2], ['x', 'y'], [3, 4]])).toThrowError(
    'Provided number of columns (3) does not match chunk column count (2)'
  );
  expect(() => chunk.setColumns([[1, 2], ['x', 'y']])).not.toThrow();
  expect(chunk.getRows()).toEqual([
    [1, 'x'],
    [2, 'y'],
  ]);
});
