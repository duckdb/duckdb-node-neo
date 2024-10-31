import { displayStringForDuckDBValue } from '../conversion/displayStringForDuckDBValue';
import { DuckDBValue } from './DuckDBValue';

export class DuckDBListValue {
  public readonly items: readonly DuckDBValue[];

  public constructor(items: readonly DuckDBValue[]) {
    this.items = items;
  }

  public toString(): string {
    return `[${this.items.map(displayStringForDuckDBValue).join(', ')}]`;
  }
}

export function listValue(items: readonly DuckDBValue[]): DuckDBListValue {
  return new DuckDBListValue(items);
}
