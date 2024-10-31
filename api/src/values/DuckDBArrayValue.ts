import { displayStringForDuckDBValue } from '../conversion/displayStringForDuckDBValue';
import { DuckDBValue } from './DuckDBValue';

export class DuckDBArrayValue {
  public readonly items: readonly DuckDBValue[];

  public constructor(items: readonly DuckDBValue[]) {
    this.items = items;
  }

  public toString(): string {
    return `[${this.items.map(displayStringForDuckDBValue).join(', ')}]`;
  }
}

export function arrayValue(items: readonly DuckDBValue[]): DuckDBArrayValue {
  return new DuckDBArrayValue(items);
}
