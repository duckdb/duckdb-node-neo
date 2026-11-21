import { displayStringForDuckDBValue } from '../conversion/displayStringForDuckDBValue';
import { DuckDBValue } from './DuckDBValue';

/**
 * A value of a tuple type: an unnamed STRUCT, such as `(1, 'a')` or
 * `row(1, 'a')`. Its items are in entry order.
 */
export class DuckDBTupleValue {
  public readonly items: readonly DuckDBValue[];

  public constructor(items: readonly DuckDBValue[]) {
    this.items = items;
  }

  public toString(): string {
    return `(${this.items.map(displayStringForDuckDBValue).join(', ')})`;
  }
}

export function tupleValue(items: readonly DuckDBValue[]): DuckDBTupleValue {
  return new DuckDBTupleValue(items);
}
