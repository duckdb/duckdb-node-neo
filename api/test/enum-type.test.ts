import { assert, describe, expect, test } from 'vitest';
import {
  DuckDBDataChunk,
  DuckDBEnum16Vector,
  DuckDBEnum32Vector,
  DuckDBEnum8Vector,
  DuckDBEnumType,
  ENUM,
} from '../src';
import { withConnection } from './util/testHelpers';

/**
 * An unknown enum member used to be looked up as undefined and written straight
 * into the backing typed array, which coerced it to 0 — silently substituting the
 * enum's first member. See duckdb/duckdb-node-neo#478.
 */

const color = ENUM(['red', 'green', 'blue']);
const notAMember = `'chartreuse' is not a member of ENUM('red', 'green', 'blue')`;

describe('DuckDBEnumType.indexForValue', () => {
  test('returns the index of a member', () => {
    assert.equal(color.indexForValue('red'), 0);
    assert.equal(color.indexForValue('green'), 1);
    assert.equal(color.indexForValue('blue'), 2);
  });
  test('rejects a value that is not a member', () => {
    expect(() => color.indexForValue('chartreuse')).toThrowError(notAMember);
  });
  test('rejects a value differing only in case', () => {
    expect(() => color.indexForValue('RED')).toThrowError(
      `'RED' is not a member of ENUM('red', 'green', 'blue')`
    );
  });
  // A plain object literal inherits from Object.prototype, so these used to
  // resolve to functions rather than undefined and land on index 0.
  for (const inherited of [
    'toString',
    'constructor',
    '__proto__',
    'hasOwnProperty',
    'valueOf',
  ]) {
    test(`rejects the inherited property ${inherited}`, () => {
      expect(() => color.indexForValue(inherited)).toThrowError(
        `is not a member of`
      );
    });
  }
  test('valueIndexes holds only members', () => {
    assert.deepEqual(Object.keys(color.valueIndexes), ['red', 'green', 'blue']);
    assert.equal(Object.getPrototypeOf(color.valueIndexes), null);
    assert.isUndefined(color.valueIndexes['toString']);
  });
  test('escapes quotes in the message', () => {
    expect(() => ENUM(["it's"]).indexForValue("other's")).toThrowError(
      `'other''s' is not a member of ENUM('it''s')`
    );
  });
  test('names the type by its alias when it has one', () => {
    expect(() => ENUM(['a', 'b'], 'mood').indexForValue('c')).toThrowError(
      `'c' is not a member of mood`
    );
  });
  test('truncates the member list for a large enum', () => {
    const big = ENUM(Array.from({ length: 300 }, (_, i) => `v${i}`));
    expect(() => big.indexForValue('nope')).toThrowError(
      `'nope' is not a member of ENUM('v0', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', and 292 more)`
    );
  });
});

describe('members named like inherited properties', () => {
  // Assigning to '__proto__' on a plain object hits the inherited setter instead
  // of creating an own property, so an enum with a member of that name could not
  // be indexed at all: writing '__proto__' stored the enum's first member.
  test('round-trip through a vector', () => {
    const proto = ENUM(['ok', '__proto__', 'toString']);
    const chunk = DuckDBDataChunk.create([proto], 3);
    const vector = chunk.getColumnVector(0);
    vector.setItem(0, '__proto__');
    vector.setItem(1, 'toString');
    vector.setItem(2, 'ok');
    assert.deepEqual(chunk.getColumnValues(0), ['__proto__', 'toString', 'ok']);
  });
});

describe('enum vector setItem', () => {
  // ENUM picks its backing width from the member count.
  const widths: [string, DuckDBEnumType, unknown][] = [
    ['ENUM8', color, DuckDBEnum8Vector],
    [
      'ENUM16',
      ENUM(Array.from({ length: 300 }, (_, i) => `v${i}`)),
      DuckDBEnum16Vector,
    ],
    [
      'ENUM32',
      ENUM(Array.from({ length: 70000 }, (_, i) => `v${i}`)),
      DuckDBEnum32Vector,
    ],
  ];
  for (const [name, type, vectorClass] of widths) {
    describe(name, () => {
      test('stores a member and rejects a non-member', () => {
        const chunk = DuckDBDataChunk.create([type], 2);
        const vector = chunk.getColumnVector(0);
        assert.instanceOf(vector, vectorClass as never);
        vector.setItem(0, type.values[1]);
        assert.equal(vector.getItem(0), type.values[1]);
        expect(() => vector.setItem(1, 'chartreuse')).toThrowError(
          `is not a member of`
        );
      });
      test('still accepts null', () => {
        const chunk = DuckDBDataChunk.create([type], 1);
        const vector = chunk.getColumnVector(0);
        vector.setItem(0, null);
        assert.isNull(vector.getItem(0));
      });
    });
  }
  test('setColumnValues reports the offending value', () => {
    const chunk = DuckDBDataChunk.create([color], 2);
    expect(() =>
      chunk.setColumnValues(0, ['blue', 'chartreuse'])
    ).toThrowError(notAMember);
  });
});

describe('enum value creation', () => {
  test('bindValue rejects a non-member', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare('select ? as v');
      expect(() =>
        prepared.bindValue(1, 'chartreuse', color)
      ).toThrowError(notAMember);
      expect(() => prepared.bindValue(1, 'blue', color)).not.toThrow();
    });
  });
});
