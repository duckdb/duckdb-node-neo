import duckdb from '@duckdb/node-bindings';
import {
  DuckDBUTinyIntType,
} from '../DuckDBType';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  vectorData,
} from './dataAccessors';

export class DuckDBUTinyIntVector extends DuckDBVector<number> {
  private readonly items: Uint8Array;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  constructor(
    items: Uint8Array,
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
  ): DuckDBUTinyIntVector {
    const data = vectorData(vector, itemCount * Uint8Array.BYTES_PER_ELEMENT);
    const items = new Uint8Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBUTinyIntVector(items, validity, vector);
  }
  public override get type(): DuckDBUTinyIntType {
    return DuckDBUTinyIntType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): number | null {
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override flatReadArray() {
    return this.items;
  }
  public override getValidity() {
    return this.validity;
  }
  public override appendTo(
    target: (number | null)[],
    targetOffset: number
  ): void {
    const items = this.items;
    const n = items.length;
    const validity = this.validity;
    if (validity.allValid()) {
      for (let i = 0; i < n; i++) {
        target[targetOffset + i] = items[i];
      }
    } else {
      for (let i = 0; i < n; i++) {
        target[targetOffset + i] = validity.itemValid(i) ? items[i] : null;
      }
    }
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
  public override slice(offset: number, length: number): DuckDBUTinyIntVector {
    return new DuckDBUTinyIntVector(
      this.items.slice(offset, offset + length),
      this.validity.slice(offset, length),
      this.vector
    );
  }
}
