/** Matches BLOB-to-VARCHAR conversion behavior of DuckDB. */
export function stringFromBlob(bytes: Uint8Array): string {
  const byteStrings: string[] = [];
  for (const byte of bytes) {
    if (
        byte <= 0x1f ||
        byte === 0x22 /* double quote */ ||
        byte === 0x27 /* single quote */ ||
        byte >= 0x7f
    ) {
      byteStrings.push(`\\x${byte.toString(16).toUpperCase().padStart(2, '0')}`);
    } else {
      byteStrings.push(String.fromCharCode(byte));
    }
  }
  return byteStrings.join('');
}