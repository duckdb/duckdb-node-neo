import { TimeTZ } from '@duckdb/node-bindings';
import { DuckDBTimeTZType } from '../DuckDBType';

export class DuckDBTimeTZValue implements TimeTZ {
  public readonly bits: bigint;

  /** Ranges from 0 to 86400000000 (= 24 * 60 * 60 * 1000 * 1000) */
  public readonly microseconds: number;

  /** In seconds, ranges from -57599 to 57599 (= 16 * 60 * 60 - 1) */
  public readonly offset: number;

  public constructor(bits: bigint, microseconds: number, offset: number) {
    this.bits = bits;
    this.microseconds = microseconds;
    this.offset = offset;
  }

  public get type(): DuckDBTimeTZType {
    return DuckDBTimeTZType.instance;
  }

  public static TIME_BITS = 40;
	public static OFFSET_BITS = 24;
	public static MAX_OFFSET = 16 * 60 * 60 - 1; // ±15:59:59 = 57599 seconds

  public static fromBits(bits: bigint): DuckDBTimeTZValue {
    const microseconds = Number(BigInt.asUintN(DuckDBTimeTZValue.TIME_BITS, bits >> BigInt(DuckDBTimeTZValue.OFFSET_BITS)));
    const offset = DuckDBTimeTZValue.MAX_OFFSET - Number(BigInt.asUintN(DuckDBTimeTZValue.OFFSET_BITS, bits));
    return new DuckDBTimeTZValue(bits, microseconds, offset);
  }

  public static fromParts(microseconds: number, offset: number): DuckDBTimeTZValue {
    const bits = BigInt.asUintN(DuckDBTimeTZValue.TIME_BITS, BigInt(microseconds)) << BigInt(DuckDBTimeTZValue.OFFSET_BITS)
               | BigInt.asUintN(DuckDBTimeTZValue.OFFSET_BITS, BigInt(DuckDBTimeTZValue.MAX_OFFSET - offset));
    return new DuckDBTimeTZValue(bits, microseconds, offset);
  }
}
