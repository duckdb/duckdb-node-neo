import { assert, describe, expect, test } from 'vitest';
import {
  BIGINT,
  DuckDBDataChunk,
  DuckDBType,
  DuckDBValue,
  HUGEINT,
  INTEGER,
  SMALLINT,
  TINYINT,
  UBIGINT,
  UHUGEINT,
  UINTEGER,
  USMALLINT,
  UTINYINT,
} from '../src';
import { withConnection } from './util/testHelpers';

/**
 * Writing a number to a typed array, or a bigint to a DataView, wraps values that
 * don't fit rather than reporting them. Vectors used to do that silently, storing
 * a value other than the one supplied. See duckdb/duckdb-node-neo#469.
 */

interface VectorCase {
  name: string;
  type: DuckDBType;
  min: DuckDBValue;
  max: DuckDBValue;
  aboveMax: DuckDBValue;
  belowMin: DuckDBValue;
  rangeError: string;
}

function numberCase(
  name: string,
  type: DuckDBType,
  min: number,
  max: number
): VectorCase {
  return {
    name,
    type,
    min,
    max,
    aboveMax: max + 1,
    belowMin: min - 1,
    rangeError: `number out of ${name} range`,
  };
}

function bigintCase(
  name: string,
  type: DuckDBType,
  min: bigint,
  max: bigint
): VectorCase {
  return {
    name,
    type,
    min,
    max,
    aboveMax: max + 1n,
    belowMin: min - 1n,
    rangeError: `bigint out of ${name} range`,
  };
}

const numberVectors: VectorCase[] = [
  numberCase('int8', TINYINT, -128, 127),
  numberCase('uint8', UTINYINT, 0, 255),
  numberCase('int16', SMALLINT, -32768, 32767),
  numberCase('uint16', USMALLINT, 0, 65535),
  numberCase('int32', INTEGER, -2147483648, 2147483647),
  numberCase('uint32', UINTEGER, 0, 4294967295),
];

const bigintVectors: VectorCase[] = [
  bigintCase('int64', BIGINT, -(2n ** 63n), 2n ** 63n - 1n),
  bigintCase('uint64', UBIGINT, 0n, 2n ** 64n - 1n),
  bigintCase('int128', HUGEINT, -(2n ** 127n), 2n ** 127n - 1n),
  bigintCase('uint128', UHUGEINT, 0n, 2n ** 128n - 1n),
];

// A chunk owns the memory its vectors are views over, and a vector holds no
// reference back to its chunk, so the chunk has to stay reachable for as long as
// the vector is used. Each test below keeps it in a local.
function chunkFor(type: DuckDBType) {
  return DuckDBDataChunk.create([type], 2);
}

describe('vector setItem range checks', () => {
  for (const vectorCase of [...numberVectors, ...bigintVectors]) {
    describe(vectorCase.name, () => {
      test('min and max round-trip', () => {
        const chunk = chunkFor(vectorCase.type);
        const vector = chunk.getColumnVector(0);
        vector.setItem(0, vectorCase.min);
        vector.setItem(1, vectorCase.max);
        assert.equal(vector.getItem(0), vectorCase.min);
        assert.equal(vector.getItem(1), vectorCase.max);
      });
      test('above max', () => {
        const chunk = chunkFor(vectorCase.type);
        const vector = chunk.getColumnVector(0);
        expect(() => vector.setItem(0, vectorCase.aboveMax)).toThrowError(
          vectorCase.rangeError
        );
      });
      test('below min', () => {
        const chunk = chunkFor(vectorCase.type);
        const vector = chunk.getColumnVector(0);
        expect(() => vector.setItem(0, vectorCase.belowMin)).toThrowError(
          vectorCase.rangeError
        );
      });
    });
  }

  // Only the number-backed vectors can be handed a non-integer.
  for (const vectorCase of numberVectors) {
    describe(`${vectorCase.name} non-integers`, () => {
      for (const [name, input] of [
        ['fractional', 1.5],
        ['NaN', NaN],
        ['Infinity', Infinity],
        ['-Infinity', -Infinity],
      ] as const) {
        test(name, () => {
          const chunk = chunkFor(vectorCase.type);
          const vector = chunk.getColumnVector(0);
          expect(() => vector.setItem(0, input)).toThrowError(
            'number is not an integer'
          );
        });
      }
    });
  }

  test('setColumnValues reports an out-of-range value', () => {
    const chunk = DuckDBDataChunk.create([UINTEGER], 2);
    expect(() => chunk.setColumnValues(0, [42, 2 ** 32 + 1])).toThrowError(
      'number out of uint32 range'
    );
  });
});

describe('appender and prepared statement range checks', () => {
  // The report in duckdb/duckdb-node-neo#469.
  test('appendUInteger rejects values that do not fit', async () => {
    await withConnection(async (connection) => {
      await connection.run('create table t (a uinteger, b uinteger)');
      const appender = await connection.createAppender('t');
      expect(() => appender.appendUInteger(2 ** 32 + 1)).toThrowError(
        'number out of uint32 range'
      );
      expect(() => appender.appendUInteger(-1)).toThrowError(
        'number out of uint32 range'
      );
      expect(() => appender.appendUInteger(1.5)).toThrowError(
        'number is not an integer'
      );
      expect(() => appender.appendUInteger(4294967295)).not.toThrow();
    });
  });

  test('bindUInteger rejects values that do not fit', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare('select ?::uinteger as v');
      expect(() => prepared.bindUInteger(1, 2 ** 32 + 1)).toThrowError(
        'number out of uint32 range'
      );
      expect(() => prepared.bindUInteger(1, 4294967295)).not.toThrow();
    });
  });

  test('bindValue rejects values that do not fit the given type', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare('select ? as v');
      expect(() => prepared.bindValue(1, 2 ** 32 + 1, UINTEGER)).toThrowError(
        'number out of uint32 range'
      );
    });
  });
});
