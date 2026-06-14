import duckdb from '@duckdb/node-bindings';
import {
  DuckDBGeometryType,
} from '../DuckDBType';
import {
  DuckDBGeometryValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';
import {
  getBuffer,
  vectorData,
} from './dataAccessors';

export class DuckDBGeometryVector extends DuckDBVector<DuckDBGeometryValue> {
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  private readonly itemOffset: number;
  private readonly _itemCount: number;
  private readonly itemCache: (DuckDBGeometryValue | null | undefined)[];
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
  ): DuckDBGeometryVector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(
      data.buffer,
      data.byteOffset,
      data.byteLength
    );
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBGeometryVector(dataView, validity, vector, 0, itemCount);
  }
  public override get type(): DuckDBGeometryType {
    return DuckDBGeometryType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBGeometryValue | null {
    return this.validity.itemValid(itemIndex)
      ? new DuckDBGeometryValue(getBuffer(this.dataView, itemIndex * 16))
      : null;
  }
  public override setItem(itemIndex: number, value: DuckDBGeometryValue | null) {
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
            cachedItem.bytes
          );
        }
        this.itemCacheDirty[itemIndex] = false;
      }
    }
    this.validity.flush(this.vector);
  }
  public override slice(offset: number, length: number): DuckDBGeometryVector {
    return new DuckDBGeometryVector(
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
