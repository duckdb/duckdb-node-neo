import duckdb from '@duckdb/node-bindings';
import {
  DuckDBVarCharType,
} from '../DuckDBType';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  getString,
  vectorData,
} from './dataAccessors';

export class DuckDBVarCharVector extends DuckDBVector<string> {
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  private readonly itemOffset: number;
  private readonly _itemCount: number;
  private readonly itemCache: (string | null | undefined)[];
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
  public override getItem(itemIndex: number): string | null {
    const cachedItem = this.itemCache[itemIndex];
    if (cachedItem !== undefined) {
      return cachedItem;
    }
    const item = this.validity.itemValid(itemIndex)
      ? getString(this.dataView, itemIndex * 16)
      : null;
    this.itemCache[itemIndex] = item;
    return item;
  }
  public setItem(itemIndex: number, value: string | null) {
    this.itemCache[itemIndex] = value;
    this.validity.setItemValid(itemIndex, value != null);
    this.itemCacheDirty[itemIndex] = true;
  }
  public flush() {
    for (let itemIndex = 0; itemIndex < this._itemCount; itemIndex++) {
      if (this.itemCacheDirty[itemIndex]) {
        const cachedItem = this.itemCache[itemIndex];
        if (cachedItem !== undefined && cachedItem !== null) {
          duckdb.vector_assign_string_element(
            this.vector,
            this.itemOffset + itemIndex,
            cachedItem
          );
        }
        this.itemCacheDirty[itemIndex] = false;
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
