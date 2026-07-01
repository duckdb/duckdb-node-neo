import duckdb from '@duckdb/node-bindings';
import {
  DuckDBVarCharType,
} from '../DuckDBType';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  decodeUtf8,
  textDecoder,
  vectorData,
} from './dataAccessors';

export class DuckDBVarCharVector extends DuckDBVector<string> {
  private readonly dataView: DataView;
  // Byte view over the same range as dataView, so inline strings can be decoded without
  // allocating a per-cell subarray. `bytes[i]` aligns with `dataView` offset `i`.
  private readonly bytes: Uint8Array;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  private readonly itemOffset: number;
  private readonly _itemCount: number;
  // Allocated lazily, only when setItem is used (the appender write path). Read paths
  // decode on demand and never populate it, avoiding per-cell array read/write/growth.
  private itemCache: (string | null | undefined)[] | undefined;
  private itemCacheDirty: boolean[] | undefined;
  constructor(
    dataView: DataView,
    validity: DuckDBValidity,
    vector: duckdb.Vector,
    itemOffset: number,
    itemCount: number
  ) {
    super();
    this.dataView = dataView;
    this.bytes = new Uint8Array(
      dataView.buffer,
      dataView.byteOffset,
      dataView.byteLength
    );
    this.validity = validity;
    this.vector = vector;
    this.itemOffset = itemOffset;
    this._itemCount = itemCount;
    this.itemCache = undefined;
    this.itemCacheDirty = undefined;
  }
  static fromRawVector(
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBVarCharVector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(
      data.buffer,
      data.byteOffset,
      data.byteLength
    );
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBVarCharVector(dataView, validity, vector, 0, itemCount);
  }
  public override get type(): DuckDBVarCharType {
    return DuckDBVarCharType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  private decode(itemIndex: number): string {
    const offset = itemIndex * 16;
    const lengthInBytes = this.dataView.getUint32(offset, true);
    if (lengthInBytes <= 12) {
      // Inline string: data lives in the buffer right after the 4-byte length.
      return decodeUtf8(this.bytes, offset + 4, lengthInBytes);
    }
    // Long string: data lives in native memory behind a pointer at offset + 8.
    const stringBytes = duckdb.get_data_from_pointer(
      this.dataView.buffer as ArrayBuffer,
      this.dataView.byteOffset + offset + 8,
      lengthInBytes
    );
    return textDecoder.decode(stringBytes);
  }
  public override getItem(itemIndex: number): string | null {
    const itemCache = this.itemCache;
    if (itemCache !== undefined) {
      const cachedItem = itemCache[itemIndex];
      if (cachedItem !== undefined) {
        return cachedItem;
      }
    }
    return this.validity.itemValid(itemIndex) ? this.decode(itemIndex) : null;
  }
  public setItem(itemIndex: number, value: string | null) {
    let itemCache = this.itemCache;
    if (itemCache === undefined) {
      itemCache = [];
      this.itemCache = itemCache;
      this.itemCacheDirty = [];
    }
    itemCache[itemIndex] = value;
    this.validity.setItemValid(itemIndex, value != null);
    this.itemCacheDirty![itemIndex] = true;
  }
  public flush() {
    const itemCache = this.itemCache;
    const itemCacheDirty = this.itemCacheDirty;
    if (itemCache !== undefined && itemCacheDirty !== undefined) {
      for (let itemIndex = 0; itemIndex < this._itemCount; itemIndex++) {
        if (itemCacheDirty[itemIndex]) {
          const cachedItem = itemCache[itemIndex];
          if (cachedItem !== undefined && cachedItem !== null) {
            duckdb.vector_assign_string_element(
              this.vector,
              this.itemOffset + itemIndex,
              cachedItem
            );
          }
          itemCacheDirty[itemIndex] = false;
        }
      }
    }
    this.validity.flush(this.vector);
  }
  public override slice(offset: number, length: number): DuckDBVarCharVector {
    return new DuckDBVarCharVector(
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
