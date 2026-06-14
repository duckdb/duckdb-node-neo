import { assert, beforeAll, describe, test } from 'vitest';
import {
  uuidValue,
} from '../src';
import {
  setDefaultTimezone,
  withConnection,
} from './util/testHelpers';

// Regression coverage for the MSB-flip-with-signed-storage contract:
// DuckDBUUIDValue always holds DuckDB's signed-int128 storage form, and
// `fromUint128` must convert into that form (including for inputs whose
// unsigned value sits below 2**127, which after the MSB flip end up with
// bit 127 set and therefore need a negative bigint to represent).
describe('DuckDBUUIDValue.fromUint128', () => {
  beforeAll(setDefaultTimezone);

  test('low-MSB input maps to a negative signed-int128 hugeint', async () => {
    // 0x0011… has bit 127 clear; after MSB flip it has bit 127 set, which
    // must surface as a negative bigint to match DuckDB's int128 storage.
    const v = uuidValue(0x00112233_4455_6677_8899_aabbccddeeffn);
    assert.strictEqual(
      v.hugeint,
      BigInt.asIntN(128, 0x80112233_4455_6677_8899_aabbccddeeffn),
    );
    assert.isTrue(v.hugeint < 0n, 'hugeint should be negative');
    assert.strictEqual(v.toString(), '00112233-4455-6677-8899-aabbccddeeff');
    // And: the same UUID round-tripped through a UUID column matches
    // exactly (the column decoder uses `fromStoredHugeInt` + `getInt128`).
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `select '00112233-4455-6677-8899-aabbccddeeff'::UUID as u`,
      );
      const decoded = reader.getRows()[0][0];
      assert.deepStrictEqual(decoded, v);
    });
  });

  test('high-MSB input maps to a positive hugeint', async () => {
    // 0xf0e1… has bit 127 set; after MSB flip the high bit is clear and
    // the value fits in a positive signed int128.
    const v = uuidValue(0xf0e1d2c3b4a596870123456789abcdefn);
    assert.strictEqual(
      v.hugeint,
      0x70e1d2c3b4a596870123456789abcdefn,
    );
    assert.isTrue(v.hugeint > 0n, 'hugeint should be positive');
    assert.strictEqual(v.toString(), 'f0e1d2c3-b4a5-9687-0123-456789abcdef');
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `select 'f0e1d2c3-b4a5-9687-0123-456789abcdef'::UUID as u`,
      );
      const decoded = reader.getRows()[0][0];
      assert.deepStrictEqual(decoded, v);
    });
  });

  test('toUint128 round-trips both halves of the range', () => {
    const low = uuidValue(0x00112233_4455_6677_8899_aabbccddeeffn);
    assert.strictEqual(
      low.toUint128(),
      0x00112233_4455_6677_8899_aabbccddeeffn,
    );
    const high = uuidValue(0xf0e1d2c3b4a596870123456789abcdefn);
    assert.strictEqual(
      high.toUint128(),
      0xf0e1d2c3b4a596870123456789abcdefn,
    );
    // Extremes.
    const allZeros = uuidValue(0n);
    assert.strictEqual(allZeros.toUint128(), 0n);
    const allOnes = uuidValue(2n ** 128n - 1n);
    assert.strictEqual(allOnes.toUint128(), 2n ** 128n - 1n);
  });
});
