import { assert, beforeAll, describe, test } from 'vitest';
import { varintDecode } from '../src/conversion/varintDecode';
import {
  setDefaultTimezone,
} from './util/testHelpers';

describe('varintDecode', () => {
  beforeAll(setDefaultTimezone);

  function makeView(bytes: number[]): DataView {
    const buf = new Uint8Array(bytes);
    return new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  }
  test('single-byte values (< 0x80)', () => {
    assert.deepStrictEqual(varintDecode(makeView([0x00]), 0), {
      value: 0,
      nextOffset: 1,
    });
    assert.deepStrictEqual(varintDecode(makeView([0x01]), 0), {
      value: 1,
      nextOffset: 1,
    });
    assert.deepStrictEqual(varintDecode(makeView([0x7f]), 0), {
      value: 127,
      nextOffset: 1,
    });
  });
  test('two-byte values', () => {
    // 128 = 0b1000_0000 → bytes [0x80, 0x01]
    assert.deepStrictEqual(varintDecode(makeView([0x80, 0x01]), 0), {
      value: 128,
      nextOffset: 2,
    });
    // 300 = 0b1_0010_1100 → bytes [0xac, 0x02]
    assert.deepStrictEqual(varintDecode(makeView([0xac, 0x02]), 0), {
      value: 300,
      nextOffset: 2,
    });
  });
  test('respects starting offset', () => {
    // Skip a leading byte; decode 300 starting at offset 1.
    const view = makeView([0xff, 0xac, 0x02, 0x99]);
    assert.deepStrictEqual(varintDecode(view, 1), {
      value: 300,
      nextOffset: 3,
    });
  });
  test('five-byte value (uint32 max)', () => {
    // 0xffffffff → bytes [0xff, 0xff, 0xff, 0xff, 0x0f]
    assert.deepStrictEqual(
      varintDecode(makeView([0xff, 0xff, 0xff, 0xff, 0x0f]), 0),
      { value: 0xffffffff, nextOffset: 5 },
    );
  });
  test('throws on overflow (6+ byte varint)', () => {
    // Six continuation bytes would shift past 32 bits.
    assert.throws(
      () =>
        varintDecode(
          makeView([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]),
          0,
        ),
      /varint overflow/,
    );
  });
  test('throws on overflow (5-byte value > uint32 max)', () => {
    // [0xff,0xff,0xff,0xff,0x10] decodes to 2**32 — one above uint32 max.
    assert.throws(
      () =>
        varintDecode(makeView([0xff, 0xff, 0xff, 0xff, 0x10]), 0),
      /varint overflow/,
    );
    // [0xff,0xff,0xff,0xff,0x7f] decodes to 2**35-1 — a 35-bit value
    // that the previous (looser) guard accepted.
    assert.throws(
      () =>
        varintDecode(makeView([0xff, 0xff, 0xff, 0xff, 0x7f]), 0),
      /varint overflow/,
    );
  });
  test('throws on truncated input (continuation bit set on final byte)', () => {
    assert.throws(
      () => varintDecode(makeView([0x80]), 0),
      /varint truncated/,
    );
    assert.throws(
      () => varintDecode(makeView([0xff, 0xff, 0xff]), 0),
      /varint truncated/,
    );
  });
});
