import duckdb from '@duckdb/node-bindings';
import {
  DuckDBStructType,
} from '../DuckDBType';
import {
  DuckDBStructValue,
  DuckDBValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValidity } from './DuckDBValidity';

export class DuckDBStructVector extends DuckDBVector<DuckDBStructValue> {
  private readonly structType: DuckDBStructType;
  private readonly _itemCount: number;
  private readonly entryVectors: readonly DuckDBVector[];
  private readonly validity: DuckDBValidity;
  private readonly vector: duckdb.Vector;
  constructor(
    structType: DuckDBStructType,
    itemCount: number,
    entryVectors: readonly DuckDBVector[],
    validity: DuckDBValidity,
    vector: duckdb.Vector
  ) {
    super();
    this.structType = structType;
    this._itemCount = itemCount;
    this.entryVectors = entryVectors;
    this.validity = validity;
    this.vector = vector;
  }
  static fromRawVector(
    structType: DuckDBStructType,
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBStructVector {
    const entryCount = structType.entryCount;
    const entryVectors: DuckDBVector[] = [];
    for (let i = 0; i < entryCount; i++) {
      const child_vector = duckdb.struct_vector_get_child(vector, i);
      entryVectors.push(
        DuckDBVector.create(child_vector, itemCount, structType.entryTypes[i])
      );
    }
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBStructVector(
      structType,
      itemCount,
      entryVectors,
      validity,
      vector
    );
  }
  public override get type(): DuckDBStructType {
    return this.structType;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBStructValue | null {
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    const entries: { [name: string]: DuckDBValue } = {};
    const entryCount = this.structType.entryCount;
    for (let i = 0; i < entryCount; i++) {
      entries[this.structType.entryNames[i]] =
        this.entryVectors[i].getItem(itemIndex);
    }
    return new DuckDBStructValue(entries);
  }
  public getItemValue(
    itemIndex: number,
    entryIndex: number
  ): DuckDBValue | null {
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    return this.entryVectors[entryIndex].getItem(itemIndex);
  }
  /**
   * Returns the underlying child vector for entry `index`. Useful for
   * callers that want direct access to a child's flat backing storage
   * (e.g. when decoding a physical-layout type that overlays a STRUCT).
   */
  public entryVectorAt(index: number): DuckDBVector {
    return this.entryVectors[index];
  }
  /**
   * Returns whether the top-level row at `itemIndex` is non-NULL. Useful
   * for callers overlaying a logical type on a STRUCT vector that need to
   * distinguish a SQL-NULL row from a row whose children just happen to
   * be NULL.
   */
  public isItemValid(itemIndex: number): boolean {
    return this.validity.itemValid(itemIndex);
  }
  public override setItem(itemIndex: number, value: DuckDBStructValue | null) {
    if (value != null) {
      const entryCount = this.structType.entryCount;
      for (let i = 0; i < entryCount; i++) {
        this.entryVectors[i].setItem(
          itemIndex,
          value.entries[this.structType.entryNames[i]]
        );
      }
      this.validity.setItemValid(itemIndex, true);
    } else {
      const entryCount = this.structType.entryCount;
      for (let i = 0; i < entryCount; i++) {
        this.entryVectors[i].setItem(itemIndex, null);
      }
      this.validity.setItemValid(itemIndex, false);
    }
  }
  public setItemValue(
    itemIndex: number,
    entryIndex: number,
    value: DuckDBValue
  ) {
    return this.entryVectors[entryIndex].setItem(itemIndex, value);
  }
  public override flush() {
    for (const entryVector of this.entryVectors) {
      entryVector.flush();
    }
    this.validity.flush(this.vector);
  }
  public override slice(offset: number, length: number): DuckDBStructVector {
    return new DuckDBStructVector(
      this.structType,
      length,
      this.entryVectors.map((entryVector) => entryVector.slice(offset, length)),
      this.validity.slice(offset, length),
      this.vector
    );
  }
}
