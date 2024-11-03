import duckdb, { TimeTZ, TimeTZParts } from '@duckdb/node-bindings';
import { getDuckDBTimeStringFromMicrosecondsInDay } from '../conversion/dateTimeStringConversion';

export type { TimeTZParts };

export class DuckDBTimeTZValue implements TimeTZ {
  /**
  * 40 bits for micros, then 24 bits for encoded offset in seconds.
  * 
  * Max absolute unencoded offset = 15:59:59 = 60 * (60 * 15 + 59) + 59 = 57599.
  * 
  * Encoded offset is unencoded offset inverted then shifted (by +57599) to unsigned.
  * 
  * Max unencoded offset = 57599 -> -57599 -> 0 encoded.
  * 
  * Min unencoded offset = -57599 -> 57599 -> 115198 encoded.
  */
  public readonly bits: bigint;

  /** Ranges from 0 to 86400000000 (= 24 * 60 * 60 * 1000 * 1000) */
  public readonly micros: bigint;

  /** In seconds, ranges from -57599 to 57599 (= 16 * 60 * 60 - 1) */
  public readonly offset: number;

  public constructor(bits: bigint, micros: bigint, offset: number) {
    this.bits = bits;
    this.micros = micros;
    this.offset = offset;
  }

  public toString(): string {
    // TODO: display offset
    return getDuckDBTimeStringFromMicrosecondsInDay(this.micros);
  }

  public toParts(): TimeTZParts {
    return duckdb.from_time_tz(this);
  }

  public static TimeBits = 40;
	public static OffsetBits = 24;
	public static MaxOffset = 16 * 60 * 60 - 1; // ±15:59:59 = 57599 seconds
  public static MinOffset = -DuckDBTimeTZValue.MaxOffset;
  public static MaxMicros = 24n * 60n * 60n * 1000n * 1000n; // 86400000000
  public static MinMicros = 0n;

  public static fromBits(bits: bigint): DuckDBTimeTZValue {
    const micros = BigInt.asUintN(DuckDBTimeTZValue.TimeBits, bits >> BigInt(DuckDBTimeTZValue.OffsetBits));
    const offset = DuckDBTimeTZValue.MaxOffset - Number(BigInt.asUintN(DuckDBTimeTZValue.OffsetBits, bits));
    return new DuckDBTimeTZValue(bits, micros, offset);
  }

  public static fromMicrosAndOffset(micros: bigint, offset: number): DuckDBTimeTZValue {
    const bits = BigInt.asUintN(DuckDBTimeTZValue.TimeBits, micros) << BigInt(DuckDBTimeTZValue.OffsetBits)
               | BigInt.asUintN(DuckDBTimeTZValue.OffsetBits, BigInt(DuckDBTimeTZValue.MaxOffset - offset));
    return new DuckDBTimeTZValue(bits, micros, offset);
  }

  public static fromParts(parts: TimeTZParts): DuckDBTimeTZValue {
    return DuckDBTimeTZValue.fromMicrosAndOffset(duckdb.to_time(parts.time).micros, parts.offset);
  }

  public static readonly Max = DuckDBTimeTZValue.fromMicrosAndOffset(DuckDBTimeTZValue.MaxMicros, DuckDBTimeTZValue.MinOffset);
  public static readonly Min = DuckDBTimeTZValue.fromMicrosAndOffset(DuckDBTimeTZValue.MinMicros, DuckDBTimeTZValue.MaxOffset);
}

export function timeTZValue(micros: bigint, offset: number): DuckDBTimeTZValue {
  return DuckDBTimeTZValue.fromMicrosAndOffset(micros, offset);
}
