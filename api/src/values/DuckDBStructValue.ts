import { displayStringForDuckDBValue } from '../conversion/displayStringForDuckDBValue';
import { DuckDBValue } from './DuckDBValue';

export interface DuckDBStructEntries {
  readonly [name: string]: DuckDBValue;
}

export class DuckDBStructValue {
  public readonly entries: DuckDBStructEntries;

  public constructor(entries: DuckDBStructEntries) {
    this.entries = entries;
  }

  public toString(): string {
    const parts: string[] = [];
    for (const name in this.entries) {
      parts.push(`${displayStringForDuckDBValue(name)}: ${displayStringForDuckDBValue(this.entries[name])}`);
    }
    return `{${parts.join(', ')}}`;
  }
}

export function structValue(entries: DuckDBStructEntries): DuckDBStructValue {
  return new DuckDBStructValue(entries);
}
