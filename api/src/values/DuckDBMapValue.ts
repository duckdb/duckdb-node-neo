import { DuckDBMapType } from '../DuckDBType';
import { DuckDBValue } from './DuckDBValue';

export interface DuckDBMapEntry<
  TKey extends DuckDBValue = DuckDBValue,
  TValue extends DuckDBValue = DuckDBValue,
> {
  key: TKey;
  value: TValue;
}

export class DuckDBMapValue<
  TKey extends DuckDBValue = DuckDBValue,
  TValue extends DuckDBValue = DuckDBValue,
> {
  public readonly type: DuckDBMapType;
  public readonly entries: DuckDBMapEntry<TKey, TValue>[];

  public constructor(type: DuckDBMapType, entries: DuckDBMapEntry<TKey, TValue>[]) {
    this.type = type;
    this.entries = entries;
  }
}
