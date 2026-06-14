import duckdb from '@duckdb/node-bindings';
import {
  DuckDBTinyIntType,
} from '../DuckDBType';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  vectorData,
} from './dataAccessors';

export class DuckDBTinyIntVector extends DuckDBVector<number> {
  private readonly items: Int8Array;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  constructor(
    items: Int8Array,
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
  ): DuckDBTinyIntVector {
    const data = vectorData(vector, itemCount * Int8Array.BYTES_PER_ELEMENT);
    const items = new Int8Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTinyIntVector(items, validity, vector);
  }
  public override get type(): DuckDBTinyIntType {
    return DuckDBTinyIntType.instance;
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
  public override slice(offset: number, length: number): DuckDBTinyIntVector {
    return new DuckDBTinyIntVector(
      this.items.slice(offset, offset + length),
      this.validity.slice(offset, length),
      this.vector
    );
  }
}
