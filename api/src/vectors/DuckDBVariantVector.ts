import duckdb from '@duckdb/node-bindings';
import { varintDecode } from '../conversion/varintDecode';
import {
  BIGINT,
  BIGNUM,
  BIT,
  BLOB,
  BOOLEAN,
  DATE,
  DECIMAL,
  DOUBLE,
  DuckDBType,
  DuckDBVariantType,
  FLOAT,
  GEOMETRY,
  HUGEINT,
  INTEGER,
  INTERVAL,
  LIST,
  SMALLINT,
  SQLNULL,
  STRUCT,
  TIME,
  TIMESTAMP,
  TIMESTAMPTZ,
  TIMESTAMPTZ_NS,
  TIMESTAMP_MS,
  TIMESTAMP_NS,
  TIMESTAMP_S,
  TIMETZ,
  TIME_NS,
  TINYINT,
  UBIGINT,
  UHUGEINT,
  UINTEGER,
  USMALLINT,
  UTINYINT,
  UUID,
  VARCHAR,
  VARIANT,
} from '../DuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { typeForValue } from '../typeForValue';
import {
  DuckDBBitValue,
  DuckDBBlobValue,
  DuckDBDateValue,
  DuckDBDecimalValue,
  DuckDBGeometryValue,
  DuckDBIntervalValue,
  DuckDBListValue,
  DuckDBStructValue,
  DuckDBTimeNSValue,
  DuckDBTimeTZValue,
  DuckDBTimeValue,
  DuckDBTimestampMillisecondsValue,
  DuckDBTimestampNanosecondsValue,
  DuckDBTimestampSecondsValue,
  DuckDBTimestampTZValue,
  DuckDBTimestampTZNanosecondsValue,
  DuckDBTimestampValue,
  DuckDBUUIDValue,
  DuckDBValue,
  DuckDBVariantValue,
} from '../values';
import { DuckDBVector } from './DuckDBVector';
import {
  getBigNumFromBytes,
  getInt128,
  getInt16,
  getInt32,
  getInt64,
  getUInt128,
  getUInt16,
  getUInt32,
  getUInt64,
  getUInt8,
  littleEndian,
  textDecoder,
} from './dataAccessors';
import { DuckDBBlobVector } from './DuckDBBlobVector';
import { DuckDBListVector } from './DuckDBListVector';
import { DuckDBStructVector } from './DuckDBStructVector';
import { DuckDBUIntegerVector } from './DuckDBUIntegerVector';
import { DuckDBUTinyIntVector } from './DuckDBUTinyIntVector';
import { DuckDBVarCharVector } from './DuckDBVarCharVector';
import { encodeVariant } from './encodeVariant';
import { VariantLogicalType } from './VariantLogicalType';

// ---------------------------------------------------------------------------
// VARIANT
// ---------------------------------------------------------------------------
//
// VARIANT is a self-describing tagged value type whose physical layout is a
// STRUCT of four children: `keys` (LIST(VARCHAR)), `children`
// (LIST(STRUCT(keys_index, values_index))), `values`
// (LIST(STRUCT(type_id, byte_offset))), and `data` (BLOB). Each row's root
// node sits at index 0 of its `values` slice; primitive payloads live at
// `byte_offsets[...]` inside the row's blob, and OBJECT/ARRAY nodes
// reference their children through the row's `children` slice.
//
// Reading reuses the existing DuckDBStructVector / DuckDBListVector /
// DuckDBBlobVector construction plus the byte-readers above for primitive
// payloads. Writing encodes each row with `encodeVariant` into a
// DuckDBStructValue of that layout and hands it to the STRUCT vector.

/** Physical STRUCT layout of a VARIANT column. Mirrors `LogicalType::VARIANT()`. */
const VARIANT_PHYSICAL_TYPE = STRUCT({
  keys: LIST(VARCHAR),
  children: LIST(STRUCT({ keys_index: UINTEGER, values_index: UINTEGER })),
  values: LIST(STRUCT({ type_id: UTINYINT, byte_offset: UINTEGER })),
  data: BLOB,
});

export class DuckDBVariantVector extends DuckDBVector<DuckDBVariantValue> {
  private readonly structVec: DuckDBStructVector;
  private readonly keysList: DuckDBListVector;
  private readonly childrenList: DuckDBListVector;
  private readonly valuesList: DuckDBListVector;
  private readonly dataBlob: DuckDBBlobVector;
  // The lists' child vectors: flushing a list replaces its child vector, so
  // these are rebound after every flush.
  private keysEntry!: DuckDBVarCharVector;
  private keysIdxVec!: DuckDBUIntegerVector;
  private valuesIdxVec!: DuckDBUIntegerVector;
  private typeIdVec!: DuckDBUTinyIntVector;
  private byteOffsetVec!: DuckDBUIntegerVector;
  /**
   * Values written with `setItem`, indexed from `cacheOffset`. Reading a
   * written row back from the vector does not work: the data blob vector
   * reads from a snapshot of the native vector taken when it was created.
   * A slice shares its parent's array, so a write through either is visible
   * to both.
   */
  private readonly itemCache: (DuckDBVariantValue | null | undefined)[];
  private readonly cacheOffset: number;

  constructor(
    structVec: DuckDBStructVector,
    itemCache: (DuckDBVariantValue | null | undefined)[] = [],
    cacheOffset = 0
  ) {
    super();
    this.structVec = structVec;
    this.itemCache = itemCache;
    this.cacheOffset = cacheOffset;
    this.keysList = structVec.entryVectorAt(0) as DuckDBListVector;
    this.childrenList = structVec.entryVectorAt(1) as DuckDBListVector;
    this.valuesList = structVec.entryVectorAt(2) as DuckDBListVector;
    this.dataBlob = structVec.entryVectorAt(3) as DuckDBBlobVector;
    this.bindListChildVectors();
  }

  private bindListChildVectors() {
    this.keysEntry = this.keysList.childVector as DuckDBVarCharVector;
    const childrenInner = this.childrenList.childVector as DuckDBStructVector;
    const valuesInner = this.valuesList.childVector as DuckDBStructVector;
    this.keysIdxVec = childrenInner.entryVectorAt(0) as DuckDBUIntegerVector;
    this.valuesIdxVec = childrenInner.entryVectorAt(1) as DuckDBUIntegerVector;
    this.typeIdVec = valuesInner.entryVectorAt(0) as DuckDBUTinyIntVector;
    this.byteOffsetVec = valuesInner.entryVectorAt(1) as DuckDBUIntegerVector;
  }

  static fromRawVector(
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBVariantVector {
    const structVec = DuckDBStructVector.fromRawVector(
      VARIANT_PHYSICAL_TYPE,
      vector,
      itemCount
    );
    return new DuckDBVariantVector(structVec);
  }

  public override get type(): DuckDBVariantType {
    return DuckDBVariantType.instance;
  }

  public override get itemCount(): number {
    return this.structVec.itemCount;
  }

  /**
   * Returns the row's value. A row written with `setItem` returns the value
   * it was given; any other row is decoded from the vector.
   */
  public override getItem(itemIndex: number): DuckDBVariantValue | null {
    const cachedItem = this.itemCache[this.cacheOffset + itemIndex];
    if (cachedItem !== undefined) {
      return cachedItem;
    }
    // Top-level validity is the canonical SQL-NULL indicator. Child rows
    // may still carry data when the row is SQL-NULL; the inverse — that a
    // non-NULL row has non-NULL children — is the only direction
    // guaranteed by the format.
    if (!this.structVec.isItemValid(itemIndex)) {
      return null;
    }
    const blob = this.dataBlob.getItemBytes(itemIndex);
    if (blob === null) {
      // Defensive: a valid row's data BLOB is expected to be present.
      return null;
    }
    const row: VariantRow = {
      blob,
      view: new DataView(blob.buffer, blob.byteOffset, blob.byteLength),
      valuesOffset: this.valuesList.getEntryOffset(itemIndex),
      valuesLength: this.valuesList.getEntryLength(itemIndex),
      childrenOffset: this.childrenList.getEntryOffset(itemIndex),
      childrenLength: this.childrenList.getEntryLength(itemIndex),
      keysOffset: this.keysList.getEntryOffset(itemIndex),
      keysLength: this.keysList.getEntryLength(itemIndex),
    };
    const root = this.decodeNode(0, row);
    return new DuckDBVariantValue(root.value, root.type);
  }

  /**
   * Encodes `value` into VARIANT's physical layout (see `encodeVariant`);
   * `flush` writes it to the native vector.
   *
   * `getItem` then returns `value` as given, not as DuckDB reads it back: a
   * value without a `type` stays without one (`variantValue(42)`, though
   * DuckDB stores an INTEGER), and nested values keep their input shape (a
   * MAP stays a MAP, though DuckDB stores an array of `{key, value}`
   * objects).
   */
  public override setItem(
    itemIndex: number,
    value: DuckDBVariantValue | null
  ): void {
    let encoded: DuckDBStructValue | null = null;
    if (value !== null) {
      try {
        encoded = encodeVariant(value, VARIANT);
      } catch (err) {
        const innerType =
          value instanceof DuckDBVariantValue
            ? (value.type ?? typeForValue(value.value))
            : typeForValue(value);
        throw new Error(
          `Failed to write VARIANT value (inner type ${innerType}): ${
            (err as Error).message
          }`
        );
      }
    }
    this.structVec.setItem(itemIndex, encoded);
    this.itemCache[this.cacheOffset + itemIndex] = value;
  }

  /**
   * Writes the values set with `setItem` to the native vector.
   *
   * A non-NULL row that was not set and whose layout is malformed — as in a
   * fresh output vector, whose memory is uninitialized — is written as NULL.
   * Rows read from a result are well-formed, so they are kept.
   */
  public override flush(): void {
    const itemCount = this.itemCount;
    for (let itemIndex = 0; itemIndex < itemCount; itemIndex++) {
      if (
        this.itemCache[this.cacheOffset + itemIndex] === undefined &&
        this.structVec.isItemValid(itemIndex) &&
        !this.isRowWellFormed(itemIndex)
      ) {
        this.setItem(itemIndex, null);
      }
    }
    this.structVec.flush();
    this.bindListChildVectors();
  }

  /**
   * Whether the row's keys, children and values entries lie within their
   * lists, and it has a root value. Does not read the row's data blob,
   * which in uninitialized memory may hold an invalid pointer.
   */
  private isRowWellFormed(itemIndex: number): boolean {
    for (const list of [this.keysList, this.childrenList, this.valuesList]) {
      const end = list.getEntryOffset(itemIndex) + list.getEntryLength(itemIndex);
      if (end > list.childVector.itemCount) {
        return false;
      }
    }
    return this.valuesList.getEntryLength(itemIndex) > 0;
  }

  public override slice(offset: number, length: number): DuckDBVariantVector {
    return new DuckDBVariantVector(
      this.structVec.slice(offset, length),
      this.itemCache,
      this.cacheOffset + offset
    );
  }

  /**
   * Decode one variant node identified by its row-local `valueIndex`
   * (an index into the row's `values` list). Per-row state is bundled in
   * `row`: the data blob plus the row-local list slice extents and the
   * absolute backing-array offsets they begin at. All recursive calls
   * share the same `row`; only `valueIndex` changes.
   *
   * Returns both the decoded value and the `DuckDBType` corresponding to
   * the node's on-disk tag, so the top-level caller can attach the type
   * to the resulting `DuckDBVariantValue` for round-trip fidelity.
   *
   * Throws if any index decoded from the blob falls outside its row-local
   * list slice — guards against malformed or hostile VARIANT payloads.
   */
  private decodeNode(
    valueIndex: number,
    row: VariantRow
  ): { value: DuckDBValue; type: DuckDBType } {
    if (valueIndex >= row.valuesLength) {
      throw new Error(
        `Malformed VARIANT: value_index ${valueIndex} out of bounds (row values length ${row.valuesLength})`
      );
    }
    const nodeAbs = row.valuesOffset + valueIndex;
    const tag = this.typeIdVec.getItem(nodeAbs) as number;
    const byteOffset = this.byteOffsetVec.getItem(nodeAbs) as number;
    const { blob, view } = row;
    switch (tag as VariantLogicalType) {
      case VariantLogicalType.VARIANT_NULL:
        return { value: null, type: SQLNULL };
      case VariantLogicalType.BOOL_TRUE:
        return { value: true, type: BOOLEAN };
      case VariantLogicalType.BOOL_FALSE:
        return { value: false, type: BOOLEAN };
      case VariantLogicalType.INT8:
        return { value: view.getInt8(byteOffset), type: TINYINT };
      case VariantLogicalType.INT16:
        return { value: getInt16(view, byteOffset), type: SMALLINT };
      case VariantLogicalType.INT32:
        return { value: getInt32(view, byteOffset), type: INTEGER };
      case VariantLogicalType.INT64:
        return { value: getInt64(view, byteOffset), type: BIGINT };
      case VariantLogicalType.INT128:
        return { value: getInt128(view, byteOffset), type: HUGEINT };
      case VariantLogicalType.UINT8:
        return { value: getUInt8(view, byteOffset), type: UTINYINT };
      case VariantLogicalType.UINT16:
        return { value: getUInt16(view, byteOffset), type: USMALLINT };
      case VariantLogicalType.UINT32:
        return { value: getUInt32(view, byteOffset), type: UINTEGER };
      case VariantLogicalType.UINT64:
        return { value: getUInt64(view, byteOffset), type: UBIGINT };
      case VariantLogicalType.UINT128:
        return { value: getUInt128(view, byteOffset), type: UHUGEINT };
      case VariantLogicalType.FLOAT:
        return {
          value: view.getFloat32(byteOffset, littleEndian),
          type: FLOAT,
        };
      case VariantLogicalType.DOUBLE:
        return {
          value: view.getFloat64(byteOffset, littleEndian),
          type: DOUBLE,
        };
      case VariantLogicalType.UUID:
        return {
          value: DuckDBUUIDValue.fromStoredHugeInt(getInt128(view, byteOffset)),
          type: UUID,
        };
      case VariantLogicalType.DATE:
        return {
          value: new DuckDBDateValue(getInt32(view, byteOffset)),
          type: DATE,
        };
      case VariantLogicalType.TIME_MICROS:
        return {
          value: new DuckDBTimeValue(getInt64(view, byteOffset)),
          type: TIME,
        };
      case VariantLogicalType.TIME_NANOS:
        return {
          value: new DuckDBTimeNSValue(getInt64(view, byteOffset)),
          type: TIME_NS,
        };
      case VariantLogicalType.TIME_MICROS_TZ:
        return {
          value: DuckDBTimeTZValue.fromBits(getUInt64(view, byteOffset)),
          type: TIMETZ,
        };
      case VariantLogicalType.TIMESTAMP_SEC:
        return {
          value: new DuckDBTimestampSecondsValue(getInt64(view, byteOffset)),
          type: TIMESTAMP_S,
        };
      case VariantLogicalType.TIMESTAMP_MILIS:
        return {
          value: new DuckDBTimestampMillisecondsValue(
            getInt64(view, byteOffset)
          ),
          type: TIMESTAMP_MS,
        };
      case VariantLogicalType.TIMESTAMP_MICROS:
        return {
          value: new DuckDBTimestampValue(getInt64(view, byteOffset)),
          type: TIMESTAMP,
        };
      case VariantLogicalType.TIMESTAMP_NANOS:
        return {
          value: new DuckDBTimestampNanosecondsValue(
            getInt64(view, byteOffset)
          ),
          type: TIMESTAMP_NS,
        };
      case VariantLogicalType.TIMESTAMP_MICROS_TZ:
        return {
          value: new DuckDBTimestampTZValue(getInt64(view, byteOffset)),
          type: TIMESTAMPTZ,
        };
      case VariantLogicalType.TIMESTAMP_NANOS_TZ:
        return {
          value: new DuckDBTimestampTZNanosecondsValue(
            getInt64(view, byteOffset)
          ),
          type: TIMESTAMPTZ_NS,
        };
      case VariantLogicalType.INTERVAL:
        return {
          value: new DuckDBIntervalValue(
            getInt32(view, byteOffset),
            getInt32(view, byteOffset + 4),
            getInt64(view, byteOffset + 8)
          ),
          type: INTERVAL,
        };
      case VariantLogicalType.VARCHAR: {
        const bytes = readVarintBytes(view, byteOffset, blob);
        return { value: textDecoder.decode(bytes), type: VARCHAR };
      }
      case VariantLogicalType.BLOB: {
        // Copy out — the bytes alias the underlying vector storage, which
        // we don't want to share through to user code.
        const bytes = readVarintBytes(view, byteOffset, blob);
        return {
          value: new DuckDBBlobValue(new Uint8Array(bytes)),
          type: BLOB,
        };
      }
      case VariantLogicalType.BITSTRING: {
        const bytes = readVarintBytes(view, byteOffset, blob);
        return {
          value: new DuckDBBitValue(new Uint8Array(bytes)),
          type: BIT,
        };
      }
      case VariantLogicalType.BIGNUM: {
        const bytes = readVarintBytes(view, byteOffset, blob);
        return { value: getBigNumFromBytes(bytes), type: BIGNUM };
      }
      case VariantLogicalType.GEOMETRY: {
        const bytes = readVarintBytes(view, byteOffset, blob);
        return {
          value: new DuckDBGeometryValue(new Uint8Array(bytes)),
          type: GEOMETRY,
        };
      }
      case VariantLogicalType.DECIMAL: {
        const wParse = varintDecode(view, byteOffset);
        const sParse = varintDecode(view, wParse.nextOffset);
        const width = wParse.value;
        const scale = sParse.value;
        const valueOffset = sParse.nextOffset;
        let intValue: bigint;
        if (width <= 4) {
          intValue = BigInt(getInt16(view, valueOffset));
        } else if (width <= 9) {
          intValue = BigInt(getInt32(view, valueOffset));
        } else if (width <= 18) {
          intValue = getInt64(view, valueOffset);
        } else if (width <= 38) {
          intValue = getInt128(view, valueOffset);
        } else {
          throw new Error(`VARIANT DECIMAL width too large: ${width}`);
        }
        return {
          value: new DuckDBDecimalValue(intValue, width, scale),
          type: DECIMAL(width, scale),
        };
      }
      case VariantLogicalType.OBJECT: {
        const { childCount, childrenIdx } = readNestedHeader(view, byteOffset);
        this.checkChildrenSlice(childrenIdx, childCount, row);
        const entries: { [name: string]: DuckDBValue } = {};
        const fields: { [name: string]: DuckDBType } = {};
        for (let k = 0; k < childCount; k++) {
          const entryAbs = row.childrenOffset + childrenIdx + k;
          const keyIdx = this.keysIdxVec.getItem(entryAbs) as number;
          if (keyIdx >= row.keysLength) {
            throw new Error(
              `Malformed VARIANT: keys_index ${keyIdx} out of bounds (row keys length ${row.keysLength})`
            );
          }
          const keyStr = this.keysEntry.getItem(
            row.keysOffset + keyIdx
          ) as string;
          const valIdx = this.valuesIdxVec.getItem(entryAbs) as number;
          const child = this.decodeNode(valIdx, row);
          entries[keyStr] = child.value;
          fields[keyStr] = child.type;
        }
        return {
          value: new DuckDBStructValue(entries),
          type: STRUCT(fields),
        };
      }
      case VariantLogicalType.ARRAY: {
        const { childCount, childrenIdx } = readNestedHeader(view, byteOffset);
        this.checkChildrenSlice(childrenIdx, childCount, row);
        // Decode all children first so we can decide whether their types
        // are uniform. If they are (treating SQLNULL as compatible with
        // any sibling, so e.g. [1, 2, null] reads as LIST(UBIGINT)), use
        // `LIST(commonType)` with bare items — that's a Value the C API's
        // create_list_value will accept, and it round-trips through
        // bind/append cleanly. Otherwise fall back to `LIST(VARIANT)`
        // with non-null items wrapped to preserve their type; null items
        // remain bare to match how column-level LIST(VARIANT) decodes a
        // null element.
        const children: { value: DuckDBValue; type: DuckDBType }[] = [];
        for (let k = 0; k < childCount; k++) {
          const entryAbs = row.childrenOffset + childrenIdx + k;
          const valIdx = this.valuesIdxVec.getItem(entryAbs) as number;
          children.push(this.decodeNode(valIdx, row));
        }
        const commonType = uniformVariantChildType(children);
        if (commonType !== null) {
          return {
            value: new DuckDBListValue(children.map((c) => c.value)),
            type: LIST(commonType),
          };
        }
        return {
          value: new DuckDBListValue(
            children.map((c) =>
              c.value === null
                ? null
                : new DuckDBVariantValue(c.value, c.type),
            ),
          ),
          type: LIST(VARIANT),
        };
      }
      default:
        throw new Error(`Unknown VARIANT type id: ${tag}`);
    }
  }

  /** Bounds-check an OBJECT / ARRAY's `[childrenIdx, childrenIdx+childCount)` range. */
  private checkChildrenSlice(
    childrenIdx: number,
    childCount: number,
    row: VariantRow
  ): void {
    if (childrenIdx + childCount > row.childrenLength) {
      throw new Error(
        `Malformed VARIANT: children slice [${childrenIdx}, ${childrenIdx + childCount}) out of bounds (row children length ${row.childrenLength})`
      );
    }
  }
}

/**
 * Returns the shared DuckDBType if every non-null child has the same type,
 * or `null` if non-null children differ. Used to pick between
 * `LIST(commonType)` and `LIST(VARIANT)` for a decoded ARRAY.
 *
 * SQLNULL children are treated as compatible with any sibling — that way a
 * very common JSON shape like `[1, 2, null]` decodes as `LIST(UBIGINT)`
 * with a bare null element rather than `LIST(VARIANT)`. An array whose
 * children are all SQLNULL (including the empty array) collapses to
 * SQLNULL itself — DuckDB casts `LIST(SQLNULL)` to VARIANT for round-trip,
 * but a `LIST(VARIANT)` of nulls or an empty `LIST(VARIANT)` doesn't
 * survive `create_list_value`.
 */
function uniformVariantChildType(
  children: { value: DuckDBValue; type: DuckDBType }[]
): DuckDBType | null {
  let common: DuckDBType | null = null;
  let commonKey: string | null = null;
  for (const child of children) {
    if (child.type.typeId === DuckDBTypeId.SQLNULL) {
      continue;
    }
    const key = typeKey(child.type);
    if (common === null) {
      common = child.type;
      commonKey = key;
    } else if (key !== commonKey) {
      return null;
    }
  }
  // All children (if any) are SQLNULL: collapse to SQLNULL.
  return common ?? SQLNULL;
}

/**
 * Returns a canonical string for a DuckDBType, suitable for equality
 * comparison between types produced by the same decode pass. Uses the
 * structured `toJson()` representation rather than the SQL-syntactic
 * `toString()` so nested types compare structurally.
 */
function typeKey(type: DuckDBType): string {
  return JSON.stringify(type.toJson());
}

/** Per-row state shared across recursive VARIANT decode calls. */
interface VariantRow {
  blob: Uint8Array;
  view: DataView;
  valuesOffset: number;
  valuesLength: number;
  childrenOffset: number;
  childrenLength: number;
  keysOffset: number;
  keysLength: number;
}

/**
 * Reads a length-prefixed payload (`<varint length><length bytes>`) from a
 * variant data blob. Returns a (possibly aliased) Uint8Array view over the
 * payload bytes — callers that need to retain the data should copy.
 * Throws if the prefix length would read past the row's blob.
 */
function readVarintBytes(
  view: DataView,
  offset: number,
  blob: Uint8Array
): Uint8Array {
  const { value: length, nextOffset } = varintDecode(view, offset);
  if (nextOffset + length > blob.byteLength) {
    throw new Error(
      `Malformed VARIANT payload: length-prefixed read of ${length} bytes at offset ${nextOffset} exceeds row blob size ${blob.byteLength}`
    );
  }
  return new Uint8Array(blob.buffer, blob.byteOffset + nextOffset, length);
}

/**
 * Reads an OBJECT / ARRAY header: `<varint child_count>` followed by
 * `<varint children_idx>` iff `child_count > 0`.
 */
function readNestedHeader(
  view: DataView,
  offset: number
): { childCount: number; childrenIdx: number } {
  const cParse = varintDecode(view, offset);
  if (cParse.value === 0) {
    return { childCount: 0, childrenIdx: 0 };
  }
  const iParse = varintDecode(view, cParse.nextOffset);
  return { childCount: cParse.value, childrenIdx: iParse.value };
}
