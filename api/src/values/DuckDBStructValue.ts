import { displayStringForDuckDBValue } from '../conversion/displayStringForDuckDBValue';
import { DuckDBValue } from './DuckDBValue';

export class DuckDBStructValue {
  public readonly entries: Readonly<Record<string, DuckDBValue>>;

  public constructor(entries: Readonly<Record<string, DuckDBValue>>) {
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

export function structValue(entries: Readonly<Record<string, DuckDBValue>>): DuckDBStructValue {
  return new DuckDBStructValue(entries);
}
