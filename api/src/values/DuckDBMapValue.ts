import { displayStringForDuckDBValue } from '../conversion/displayStringForDuckDBValue';
import { DuckDBValue } from './DuckDBValue';

export interface DuckDBMapEntry {
  key: DuckDBValue;
  value: DuckDBValue;
}

export class DuckDBMapValue {
  public readonly entries: DuckDBMapEntry[];

  public constructor(entries: DuckDBMapEntry[]) {
    this.entries = entries;
  }

  public toString(): string {
    return `{${this.entries.map(({ key, value }) =>
      `${displayStringForDuckDBValue(key)}: ${displayStringForDuckDBValue(value)}`
    ).join(', ')}}`;
  }
}

export function mapValue(entries: DuckDBMapEntry[]): DuckDBMapValue {
  return new DuckDBMapValue(entries);
}
