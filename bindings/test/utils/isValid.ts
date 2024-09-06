export function isValid(validity: BigUint64Array, bit: number): boolean {
  return (validity[Math.floor(bit / 64)] & (1n << BigInt(bit % 64))) !== 0n;
}
