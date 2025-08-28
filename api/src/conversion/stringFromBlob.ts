/** Matches BLOB-to-VARCHAR conversion behavior of DuckDB. */
export function stringFromBlob(bytes: Uint8Array): string {
  // String concatenation appears to be faster for this function at smaller sizes.
  // Threshold of (2^16) experimentally determined on a MacBook Pro (M2 Max).
  // See stringFromBlob.bench.ts.
  if (bytes.length <= 65536) {
    return stringFromBlobStringConcat(bytes);
  }
  return stringFromBlobArrayJoin(bytes);
}

export function stringFromBlobStringConcat(bytes: Uint8Array): string {
  let byteString: string = '';

  for (const byte of bytes) {
    if (
      byte <= 0x1f ||
      byte === 0x22 /* double quote */ ||
      byte === 0x27 /* single quote */ ||
      byte >= 0x7f
    ) {
      byteString += `\\x${byte.toString(16).toUpperCase().padStart(2, '0')}`;
    } else {
      byteString += String.fromCharCode(byte);
    }
  }
  return byteString;
}

export function stringFromBlobArrayJoin(bytes: Uint8Array): string {
  const byteStrings: string[] = [];

  for (const byte of bytes) {
    if (
      byte <= 0x1f ||
      byte === 0x22 /* double quote */ ||
      byte === 0x27 /* single quote */ ||
      byte >= 0x7f
    ) {
      byteStrings.push(
        `\\x${byte.toString(16).toUpperCase().padStart(2, '0')}`,
      );
    } else {
      byteStrings.push(String.fromCharCode(byte));
    }
  }
  return byteStrings.join('');
}
