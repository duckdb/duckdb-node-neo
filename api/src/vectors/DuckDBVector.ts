import duckdb from '@duckdb/node-bindings';
import { DuckDBType } from '../DuckDBType';
import { DuckDBValue } from '../values';
import { vectorRegistry } from './vectorRegistry';

export abstract class DuckDBVector<TValue extends DuckDBValue = DuckDBValue> {
  public static standardSize(): number {
    return duckdb.vector_size();
  }
  public static create(
    vector: duckdb.Vector,
    itemCount: number,
    knownType?: DuckDBType
  ): DuckDBVector {
    if (!vectorRegistry.create) {
      throw new Error('DuckDBVector factory has not been registered');
    }
    return vectorRegistry.create(vector, itemCount, knownType);
  }
  public abstract get type(): DuckDBType;
  public abstract get itemCount(): number;
  public abstract getItem(itemIndex: number): TValue | null;
  public abstract setItem(itemIndex: number, value: TValue | null): void;
  public abstract flush(): void;
  public abstract slice(offset: number, length: number): DuckDBVector<TValue>;
  public toArray(): (TValue | null)[] {
    const items: (TValue | null)[] = [];
    for (let i = 0; i < this.itemCount; i++) {
      items.push(this.getItem(i));
    }
    return items;
  }
}
