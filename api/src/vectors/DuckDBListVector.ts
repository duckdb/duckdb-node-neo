import duckdb from '@duckdb/node-bindings';
import {
  DuckDBListType,
} from '../DuckDBType';
import {
  DuckDBListValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  vectorData,
} from './dataAccessors';

export class DuckDBListVector extends DuckDBVector<DuckDBListValue> {
  private readonly parentList: DuckDBListVector | null;
  private readonly listType: DuckDBListType;
  private readonly entryData: BigUint64Array;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  private childData: DuckDBVector;
  private readonly itemOffset: number;
  private readonly _itemCount: number;
  private readonly itemCache: (DuckDBListValue | null | undefined)[];
  constructor(
    parentList: DuckDBListVector | null,
    listType: DuckDBListType,
    entryData: BigUint64Array,
    validity: DuckDBValidity,
    vector: duckdb.Vector,
    childData: DuckDBVector,
    itemOffset: number,
    itemCount: number
  ) {
    super();
    this.parentList = parentList;
    this.listType = listType;
    this.entryData = entryData;
    this.validity = validity;
    this.vector = vector;
    this.childData = childData;
    this.itemOffset = itemOffset;
    this._itemCount = itemCount;
    this.itemCache = [];
  }
  static fromRawVector(
    listType: DuckDBListType,
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBListVector {
    const data = vectorData(
      vector,
      itemCount * BigUint64Array.BYTES_PER_ELEMENT * 2
    );
    const entryData = new BigUint64Array(
      data.buffer,
      data.byteOffset,
      itemCount * 2
    );

    const validity = DuckDBValidity.fromVector(vector, itemCount);

    const child_vector = duckdb.list_vector_get_child(vector);
    const child_vector_size = duckdb.list_vector_get_size(vector);
    const childData = DuckDBVector.create(
      child_vector,
      child_vector_size,
      listType.valueType
    );

    return new DuckDBListVector(
      null,
      listType,
      entryData,
      validity,
      vector,
      childData,
      0,
      itemCount
    );
  }
  public override get type(): DuckDBListType {
    return this.listType;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  // A slice reads and writes through its parent, which holds the cache and,
  // after a flush, the current entries and child vector; so a write through
  // either is visible to both.

  public getItemVector(itemIndex: number): DuckDBVector | null {
    if (this.parentList) {
      return this.parentList.getItemVector(this.itemOffset + itemIndex);
    }
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    const entryDataStartIndex = itemIndex * 2;
    const offset = Number(this.entryData[entryDataStartIndex]);
    const length = Number(this.entryData[entryDataStartIndex + 1]);
    return this.childData.slice(offset, length);
  }
  /**
   * Returns the offset into `childVector` where this row's elements begin.
   * Combined with `childVector`, this allows callers to index directly into
   * the flat backing storage without allocating a per-row sliced vector.
   */
  public getEntryOffset(itemIndex: number): number {
    if (this.parentList) {
      return this.parentList.getEntryOffset(this.itemOffset + itemIndex);
    }
    return Number(this.entryData[itemIndex * 2]);
  }
  /** Returns the number of elements this row contributes to `childVector`. */
  public getEntryLength(itemIndex: number): number {
    if (this.parentList) {
      return this.parentList.getEntryLength(this.itemOffset + itemIndex);
    }
    return Number(this.entryData[itemIndex * 2 + 1]);
  }
  /** The (flat) child vector backing all rows' list elements. */
  public get childVector(): DuckDBVector {
    if (this.parentList) {
      return this.parentList.childVector;
    }
    return this.childData;
  }
  public override getItem(itemIndex: number): DuckDBListValue | null {
    if (this.parentList) {
      return this.parentList.getItem(this.itemOffset + itemIndex);
    }
    const cachedItem = this.itemCache[itemIndex];
    if (cachedItem !== undefined) {
      return cachedItem;
    }
    const vector = this.getItemVector(itemIndex);
    if (!vector) {
      return null;
    }
    const item = new DuckDBListValue(vector.toArray());
    this.itemCache[itemIndex] = item;
    return item;
  }
  public setItem(itemIndex: number, value: DuckDBListValue | null) {
    if (this.parentList) {
      this.parentList.setItem(this.itemOffset + itemIndex, value);
    } else {
      this.itemCache[itemIndex] = value;
      this.validity.setItemValid(itemIndex, value != null);
    }
  }
  public flush() {
    if (this.parentList) {
      this.parentList.flush();
    } else {
      // update entryData offset & lengths
      // calculate new child vector size (sum of all item lengths)
      let totalLength = 0;
      for (let itemIndex = 0; itemIndex < this._itemCount; itemIndex++) {
        const entryDataStartIndex = itemIndex * 2;
        // ensure the cache is populated for all items; this reads the item's
        // current offset, so it must come before that offset is overwritten
        const item = this.getItem(itemIndex);
        this.entryData[entryDataStartIndex] = BigInt(totalLength);
        if (item) {
          this.entryData[entryDataStartIndex + 1] = BigInt(item.items.length);
          totalLength += item.items.length;
        } else {
          this.entryData[entryDataStartIndex + 1] = 0n;
        }
      }

      // grow child vector capacity (set_size does not allocate)
      duckdb.list_vector_reserve(this.vector, totalLength);

      // set new child vector size
      duckdb.list_vector_set_size(this.vector, totalLength);

      // recreate childData after resize
      const child_vector = duckdb.list_vector_get_child(this.vector);
      const child_vector_size = duckdb.list_vector_get_size(this.vector);
      this.childData = DuckDBVector.create(
        child_vector,
        child_vector_size,
        this.listType.valueType
      );

      // set all childData items
      let childItemAbsoluteIndex = 0;
      for (let listIndex = 0; listIndex < this._itemCount; listIndex++) {
        const list = this.getItem(listIndex);
        if (list) {
          for (
            let childItemRelativeIndex = 0;
            childItemRelativeIndex < list.items.length;
            childItemRelativeIndex++
          ) {
            this.childData.setItem(
              childItemAbsoluteIndex++,
              list.items[childItemRelativeIndex]
            );
          }
        }
      }

      // copy childData to child vector
      this.childData.flush();

      // copy entryData to vector
      duckdb.copy_data_to_vector(
        this.vector,
        0,
        this.entryData.buffer as ArrayBuffer,
        this.entryData.byteOffset,
        this.entryData.byteLength
      );

      // flush validity
      this.validity.flush(this.vector);
    }
  }
  public override slice(offset: number, length: number): DuckDBListVector {
    const entryDataStartIndex = offset * 2;
    return new DuckDBListVector(
      this,
      this.listType,
      this.entryData.subarray(
        entryDataStartIndex,
        entryDataStartIndex + length * 2
      ),
      this.validity.slice(offset),
      this.vector,
      this.childData,
      offset,
      length
    );
  }
}
