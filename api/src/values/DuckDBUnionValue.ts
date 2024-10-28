import { DuckDBValue } from './DuckDBValue';

export class DuckDBUnionValue<TValue extends DuckDBValue = DuckDBValue> {
  public readonly tag: string;
  public readonly value: TValue;

  public constructor(tag: string, value: TValue) {
    this.tag = tag;
    this.value = value;
  }
}
