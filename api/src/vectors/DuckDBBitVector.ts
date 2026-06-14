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
  getStringBytes,
  vectorData,
} from './dataAccessors';

export class DuckDBBitVector extends DuckDBVector<DuckDBBitValue> {
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  private readonly itemOffset: number;
  private readonly _itemCount: number;
  private readonly itemCache: (DuckDBBitValue | null | undefined)[];
  private readonly itemCacheDirty: boolean[];
  constructor(
    dataView: DataView,
    validity: DuckDBValidity,
    vector: duckdb.Vector,
    itemOffset: number,
    itemCount: number
  ) {
    super();
    this.dataView = dataView;
    this.validity = validity;
    this.vector = vector;
    this.itemOffset = itemOffset;
    this._itemCount = itemCount;
    this.itemCache = [];
    this.itemCacheDirty = [];
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
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    const bytes = getStringBytes(this.dataView, itemIndex * 16);
    return bytes ? new DuckDBBitValue(bytes) : null;
  }
  public override setItem(itemIndex: number, value: DuckDBBitValue | null) {
    this.itemCache[itemIndex] = value;
    this.validity.setItemValid(itemIndex, value != null);
    this.itemCacheDirty[itemIndex] = true;
  }
  public override flush() {
    for (let itemIndex = 0; itemIndex < this._itemCount; itemIndex++) {
      if (this.itemCacheDirty[itemIndex]) {
        const cachedItem = this.itemCache[itemIndex];
        if (cachedItem !== undefined && cachedItem !== null) {
          duckdb.vector_assign_string_element_len(
            this.vector,
            this.itemOffset + itemIndex,
            cachedItem.data
          );
        }
        this.itemCacheDirty[itemIndex] = false;
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
      this.validity.slice(offset, length),
      this.vector,
      offset,
      length
    );
  }
}
