import duckdb from '@duckdb/node-bindings';
import {
  DuckDBEnumType,
} from '../DuckDBType';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  vectorData,
} from './dataAccessors';

export class DuckDBEnum16Vector extends DuckDBVector<string> {
  private readonly enumType: DuckDBEnumType;
  private readonly items: Uint16Array;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  constructor(
    enumType: DuckDBEnumType,
    items: Uint16Array,
    validity: DuckDBValidity,
    vector: duckdb.Vector
  ) {
    super();
    this.enumType = enumType;
    this.items = items;
    this.validity = validity;
    this.vector = vector;
  }
  static fromRawVector(
    enumType: DuckDBEnumType,
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBEnum16Vector {
    const data = vectorData(vector, itemCount * 2);
    const items = new Uint16Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBEnum16Vector(enumType, items, validity, vector);
  }
  public override get type(): DuckDBEnumType {
    return this.enumType;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): string | null {
    return this.validity.itemValid(itemIndex)
      ? this.enumType.values[this.items[itemIndex]]
      : null;
  }
  public override setItem(itemIndex: number, value: string | null) {
    if (value != null) {
      this.items[itemIndex] = this.enumType.indexForValue(value);
      this.validity.setItemValid(itemIndex, true);
    } else {
      this.validity.setItemValid(itemIndex, false);
    }
  }
  public override flush() {
    duckdb.copy_data_to_vector(
      this.vector,
      0,
      this.items.buffer as ArrayBuffer,
      this.items.byteOffset,
      this.items.byteLength
    );
    this.validity.flush(this.vector);
  }
  public override slice(offset: number, length: number): DuckDBEnum16Vector {
    return new DuckDBEnum16Vector(
      this.enumType,
      this.items.slice(offset, offset + length),
      this.validity.slice(offset, length),
      this.vector
    );
  }
}
