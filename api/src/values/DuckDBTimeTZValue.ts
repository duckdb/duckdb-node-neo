import { TimeTZ } from '@duckdb/node-bindings';
import { getDuckDBTimeStringFromMicrosecondsInDay } from '../conversion/dateTimeStringConversion';

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

  public toString(): string {
    // TODO: display offset
    return getDuckDBTimeStringFromMicrosecondsInDay(BigInt(this.microseconds));
  }

  public static TimeBits = 40;
	public static OffsetBits = 24;
	public static MaxOffset = 16 * 60 * 60 - 1; // ±15:59:59 = 57599 seconds
  public static MinOffset = -DuckDBTimeTZValue.MaxOffset;
  public static MaxMicroseconds = 24 * 60 * 60 * 1000 * 1000; // 86400000000
  public static MinMicroseconds = 0;

  public static fromBits(bits: bigint): DuckDBTimeTZValue {
    const microseconds = Number(BigInt.asUintN(DuckDBTimeTZValue.TimeBits, bits >> BigInt(DuckDBTimeTZValue.OffsetBits)));
    const offset = DuckDBTimeTZValue.MaxOffset - Number(BigInt.asUintN(DuckDBTimeTZValue.OffsetBits, bits));
    return new DuckDBTimeTZValue(bits, microseconds, offset);
  }

  public static fromParts(microseconds: number, offset: number): DuckDBTimeTZValue {
    const bits = BigInt.asUintN(DuckDBTimeTZValue.TimeBits, BigInt(microseconds)) << BigInt(DuckDBTimeTZValue.OffsetBits)
               | BigInt.asUintN(DuckDBTimeTZValue.OffsetBits, BigInt(DuckDBTimeTZValue.MaxOffset - offset));
    return new DuckDBTimeTZValue(bits, microseconds, offset);
  }

  public static readonly Max = DuckDBTimeTZValue.fromParts(DuckDBTimeTZValue.MaxMicroseconds, DuckDBTimeTZValue.MinOffset);
  public static readonly Min = DuckDBTimeTZValue.fromParts(DuckDBTimeTZValue.MinMicroseconds, DuckDBTimeTZValue.MaxOffset);
}

export function timeTZValue(microseconds: number, offset: number): DuckDBTimeTZValue {
  return DuckDBTimeTZValue.fromParts(microseconds, offset);
}
