import { DuckDBBitType } from '../DuckDBType';

export class DuckDBBitValue {
  public readonly data: Uint8Array;

  constructor(data: Uint8Array) {
    this.data = data;
  }

  public get type(): DuckDBBitType {
    return DuckDBBitType.instance;
  }

  private padding(): number {
    return this.data[0];
  }

  public length(): number {
    return (this.data.length - 1) * 8 - this.padding();
  }

  public getBool(index: number): boolean {
    const dataIndex = Math.floor(index / 8) + 1;
    return (this.data[dataIndex] & (1 << (index % 8))) !== 0;
  }

  public getBit(index: number): 0 | 1 {
    return this.getBool(index) ? 1 : 0;
  }

  public toString(): string {
    const chars = Array.from<string>({ length: this.length() });
    for (let i = 0; i < this.length(); i++) {
      chars[i] = this.getBool(i) ? '1' : '0';
    }
    return chars.join('');
  }

  public static fromString(str: string): DuckDBBitValue {
    if (!/^[01]*$/.test(str)) {
      throw new Error(`input string must only contain '0's and '1's`);
    }

    const byteCount = Math.ceil(str.length / 8) + 1;
    const paddingBitCount = (8 - (str.length % 8)) % 8;

    const data = new Uint8Array(byteCount);
    let byteIndex = 0;

    // first byte contains count of padding bits
    data[byteIndex++] = paddingBitCount;

    let byte = 0;
    let bitIndex = 0;

    // padding consists of 1s in MSB of second byte
    while (bitIndex < paddingBitCount) {
      byte <<= 1;
      byte |= 1;
      bitIndex++;
    }

    let charIndex = 0;

    while (byteIndex < byteCount) {
      while (bitIndex < 8) {
        byte <<= 1;
        if (str[charIndex++] === '1') {
          byte |= 1;
        }
        bitIndex++;
      }
      data[byteIndex++] = byte;
      byte = 0;
      bitIndex = 0;
    }

    return new DuckDBBitValue(data);
  }
}
