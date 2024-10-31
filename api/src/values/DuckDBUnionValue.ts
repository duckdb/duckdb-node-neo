import { DuckDBValue } from './DuckDBValue';

export class DuckDBUnionValue {
  public readonly tag: string;
  public readonly value: DuckDBValue;

  public constructor(tag: string, value: DuckDBValue) {
    this.tag = tag;
    this.value = value;
  }

  public toString(): string {
    if (this.value == null) {
      return 'NULL';
    }
    return this.value.toString();
  }
}

export function unionValue(tag: string, value: DuckDBValue): DuckDBUnionValue {
  return new DuckDBUnionValue(tag, value);
}
