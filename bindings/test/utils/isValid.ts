export function isValid(validity: BigUint64Array | null, bit: number): boolean {
  if (!validity) {
    return true;
  }
  return (validity[Math.floor(bit / 64)] & (1n << BigInt(bit % 64))) !== 0n;
}
