/**
 * Decodes an unsigned LEB128 (little-endian base-128) varint at
 * `view[offset]`. Each byte contributes 7 bits of payload; the high bit is
 * a continuation flag.
 *
 * Returns the decoded `value` and the byte position `nextOffset` immediately
 * after the varint. Throws if the varint would overflow a 32-bit unsigned
 * value (the format never produces wider varints in this codebase).
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
  let bits = 0;
  while (true) {
    const byte = view.getUint8(cur++);
    value += (byte & 0x7f) * multiplier;
    if ((byte & 0x80) === 0) {
      break;
    }
    multiplier *= 128;
    bits += 7;
    if (bits > 32) {
      throw new Error('varint overflow');
    }
  }
  return { value, nextOffset: cur };
}
