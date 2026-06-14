import duckdb from '@duckdb/node-bindings';
import {
  DuckDBUBigIntType,
} from '../DuckDBType';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  vectorData,
} from './dataAccessors';

export class DuckDBUBigIntVector extends DuckDBVector<bigint> {
  private readonly items: BigUint64Array;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  constructor(
    items: BigUint64Array,
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
  ): DuckDBUBigIntVector {
    const data = vectorData(
      vector,
      itemCount * BigUint64Array.BYTES_PER_ELEMENT
    );
    const items = new BigUint64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBUBigIntVector(items, validity, vector);
  }
  public override get type(): DuckDBUBigIntType {
    return DuckDBUBigIntType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): bigint | null {
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override setItem(itemIndex: number, value: bigint | null) {
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
  public override slice(offset: number, length: number): DuckDBUBigIntVector {
    return new DuckDBUBigIntVector(
      this.items.slice(offset, offset + length),
      this.validity.slice(offset, length),
      this.vector
    );
  }
}
