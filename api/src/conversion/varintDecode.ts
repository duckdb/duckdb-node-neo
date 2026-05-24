/**
 * Decodes an unsigned LEB128 (little-endian base-128) varint at
 * `view[offset]`. Each byte contributes 7 bits of payload; the high bit is
 * a continuation flag.
 *
 * Returns the decoded `value` and the byte position `nextOffset` immediately
 * after the varint. Throws if the varint:
 *   - reads past the end of `view` (truncated),
 *   - exceeds 2**32-1 (overflow).
 *
 * Uses arithmetic (not bitwise) accumulation so values up to 2**32-1
 * round-trip correctly — JS bitwise operators coerce to signed int32 and
 * would turn 0xffffffff into -1.
 */
export function varintDecode(
  view: DataView,
  offset: number
): { value: number; nextOffset: number } {
  let value = 0;
  let multiplier = 1;
  let cur = offset;
  while (true) {
    if (cur >= view.byteLength) {
      throw new Error('varint truncated');
    }
    const byte = view.getUint8(cur++);
    value += (byte & 0x7f) * multiplier;
    if ((byte & 0x80) === 0) {
      break;
    }
    multiplier *= 128;
    if (multiplier > 0x80000000) {
      // multiplier == 128**5 means we're about to process a 6th byte —
      // even a single payload bit beyond would push past 2**32-1.
      throw new Error('varint overflow');
    }
  }
  if (value > 0xffffffff) {
    throw new Error('varint overflow');
  }
  return { value, nextOffset: cur };
}
