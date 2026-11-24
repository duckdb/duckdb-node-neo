import duckdb from '@duckdb/node-bindings';
import {
  DuckDBBigNumType,
} from '../DuckDBType';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  getBigNumFromBytes,
  getBytesFromBigNum,
  getStringBytes,
  vectorData,
} from './dataAccessors';

export class DuckDBBigNumVector extends DuckDBVector<bigint> {
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  private readonly itemOffset: number;
  private readonly _itemCount: number;
  // Indexed from the root vector's first row, so a slice shares these with
  // its parent: a write through either is visible to both.
  private readonly itemCache: (bigint | null | undefined)[];
  private readonly itemCacheDirty: boolean[];
  constructor(
    dataView: DataView,
    validity: DuckDBValidity,
    vector: duckdb.Vector,
    itemOffset: number,
    itemCount: number,
    itemCache: (bigint | null | undefined)[] = [],
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
  ): DuckDBBigNumVector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(
      data.buffer,
      data.byteOffset,
      data.byteLength
    );
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBBigNumVector(dataView, validity, vector, 0, itemCount);
  }
  public override get type(): DuckDBBigNumType {
    return DuckDBBigNumType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): bigint | null {
    const cachedItem = this.itemCache[this.itemOffset + itemIndex];
    if (cachedItem !== undefined) {
      return cachedItem;
    }
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    const bytes = getStringBytes(this.dataView, itemIndex * 16);
    return bytes ? getBigNumFromBytes(bytes) : null;
  }
  public override setItem(itemIndex: number, value: bigint | null) {
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
            getBytesFromBigNum(cachedItem)
          );
        }
        this.itemCacheDirty[this.itemOffset + itemIndex] = false;
      }
    }
    this.validity.flush(this.vector);
  }
  public override slice(offset: number, length: number): DuckDBBigNumVector {
    return new DuckDBBigNumVector(
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
