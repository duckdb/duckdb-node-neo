import duckdb from '@duckdb/node-bindings';
import {
  DuckDBUSmallIntType,
} from '../DuckDBType';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  vectorData,
} from './dataAccessors';

export class DuckDBUSmallIntVector extends DuckDBVector<number> {
  private readonly items: Uint16Array;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  constructor(
    items: Uint16Array,
    validity: DuckDBValidity,
    vector: duckdb.Vector
  ) {
    super();
    this.items = items;
    this.validity = validity;
    this.vector = vector;
  }
  static fromRawVector(
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBUSmallIntVector {
    const data = vectorData(vector, itemCount * Uint16Array.BYTES_PER_ELEMENT);
    const items = new Uint16Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBUSmallIntVector(items, validity, vector);
  }
  public override get type(): DuckDBUSmallIntType {
    return DuckDBUSmallIntType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): number | null {
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override setItem(itemIndex: number, value: number | null) {
    if (value != null) {
      this.items[itemIndex] = value;
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
  public override slice(offset: number, length: number): DuckDBUSmallIntVector {
    return new DuckDBUSmallIntVector(
      this.items.slice(offset, offset + length),
      this.validity.slice(offset, length),
      this.vector
    );
  }
}
