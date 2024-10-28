import { Decimal } from '@duckdb/node-bindings';
import { DuckDBDecimalType } from '../DuckDBType';

export class DuckDBDecimalValue<T extends number | bigint = number | bigint> implements Decimal {
  public readonly type: DuckDBDecimalType;
  public readonly scaledValue: T;

  public constructor(type: DuckDBDecimalType, scaledValue: T) {
    this.type = type;
    this.scaledValue = scaledValue;
  }

  get width(): number {
    return this.type.width;
  }

  get scale(): number {
    return this.type.scale;
  }

  get value(): bigint {
    return BigInt(this.scaledValue);
  }
}
