import { TimeNS } from '@duckdb/node-bindings';
import { getDuckDBTimeStringFromNanosecondsInDay } from '../conversion/dateTimeStringConversion';

export class DuckDBTimeNSValue implements TimeNS {
  public readonly nanos: bigint;

  public constructor(nanos: bigint) {
    this.nanos = nanos;
  }

  public toString(): string {
    return getDuckDBTimeStringFromNanosecondsInDay(this.nanos);
  }

  public static readonly Max = new DuckDBTimeNSValue(
    24n * 60n * 60n * 1000n * 1000n * 1000n
  );
  public static readonly Min = new DuckDBTimeNSValue(0n);
}

export function timeNSValue(nanos: bigint): DuckDBTimeNSValue {
  return new DuckDBTimeNSValue(nanos);
}
