import duckdb from '@duckdb/node-bindings';
import {
  DuckDBBitType,
} from '../DuckDBType';
import {
  DuckDBBitValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  getBuffer,
  vectorData,
} from './dataAccessors';

export class DuckDBBitVector extends DuckDBVector<DuckDBBitValue> {
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  private readonly itemOffset: number;
  private readonly _itemCount: number;
  // Indexed from the root vector's first row, so a slice shares these with
  // its parent: a write through either is visible to both.
  private readonly itemCache: (DuckDBBitValue | null | undefined)[];
  private readonly itemCacheDirty: boolean[];
  constructor(
    dataView: DataView,
    validity: DuckDBValidity,
    vector: duckdb.Vector,
    itemOffset: number,
    itemCount: number,
    itemCache: (DuckDBBitValue | null | undefined)[] = [],
    itemCacheDirty: boolean[] = []
  ) {
    super();
    this.dataView = dataView;
    this.validity = validity;
    this.vector = vector;
    this.itemOffset = itemOffset;
    this._itemCount = itemCount;
    this.itemCache = itemCache;
    this.itemCacheDirty = itemCacheDirty;
  }
  static fromRawVector(
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBBitVector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(
      data.buffer,
      data.byteOffset,
      data.byteLength
    );
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBBitVector(dataView, validity, vector, 0, itemCount);
  }
  public override get type(): DuckDBBitType {
    return DuckDBBitType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBBitValue | null {
    const cachedItem = this.itemCache[this.itemOffset + itemIndex];
    if (cachedItem !== undefined) {
      return cachedItem;
    }
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    // getBuffer rather than getStringBytes: the latter returns a view into the
    // vector's memory for payloads of twelve bytes or fewer, and the value it is
    // handed to keeps that reference, so it would dangle once the vector goes.
    const bytes = getBuffer(this.dataView, itemIndex * 16);
    return bytes ? new DuckDBBitValue(bytes) : null;
  }
  public override setItem(itemIndex: number, value: DuckDBBitValue | null) {
    this.itemCache[this.itemOffset + itemIndex] = value;
    this.validity.setItemValid(itemIndex, value != null);
    this.itemCacheDirty[this.itemOffset + itemIndex] = true;
  }
  public override flush() {
    for (let itemIndex = 0; itemIndex < this._itemCount; itemIndex++) {
      if (this.itemCacheDirty[this.itemOffset + itemIndex]) {
        const cachedItem = this.itemCache[this.itemOffset + itemIndex];
        if (cachedItem !== undefined && cachedItem !== null) {
          duckdb.vector_assign_string_element_len(
            this.vector,
            this.itemOffset + itemIndex,
            cachedItem.data
          );
        }
        this.itemCacheDirty[this.itemOffset + itemIndex] = false;
      }
    }
    this.validity.flush(this.vector);
  }
  public override slice(offset: number, length: number): DuckDBBitVector {
    return new DuckDBBitVector(
      new DataView(
        this.dataView.buffer,
        this.dataView.byteOffset + offset * 16,
        length * 16
      ),
      this.validity.slice(offset),
      this.vector,
      this.itemOffset + offset,
      length,
      this.itemCache,
      this.itemCacheDirty
    );
  }
}
