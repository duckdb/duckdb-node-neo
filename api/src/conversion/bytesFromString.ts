const textEncoder = new TextEncoder();

export function bytesFromString(str: string): Uint8Array {
  return textEncoder.encode(str);
}
