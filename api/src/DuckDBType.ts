import { DuckDBTypeId } from './DuckDBTypeId';
import { quotedIdentifier, quotedString } from './sql';

export abstract class BaseDuckDBType<T extends DuckDBTypeId> {
  public readonly typeId: T;
  protected constructor(typeId: T) {
    this.typeId = typeId;
  }
  public toString(): string {
    return DuckDBTypeId[this.typeId];
  }
}

export class DuckDBBooleanType extends BaseDuckDBType<DuckDBTypeId.BOOLEAN> {
  private constructor() {
    super(DuckDBTypeId.BOOLEAN);
  }
  public static readonly instance = new DuckDBBooleanType();
}

export class DuckDBTinyIntType extends BaseDuckDBType<DuckDBTypeId.TINYINT> {
  private constructor() {
    super(DuckDBTypeId.TINYINT);
  }
  public static readonly instance = new DuckDBTinyIntType();
}

export class DuckDBSmallIntType extends BaseDuckDBType<DuckDBTypeId.SMALLINT> {
  private constructor() {
    super(DuckDBTypeId.SMALLINT);
  }
  public static readonly instance = new DuckDBSmallIntType();
}

export class DuckDBIntegerType extends BaseDuckDBType<DuckDBTypeId.INTEGER> {
  private constructor() {
    super(DuckDBTypeId.INTEGER);
  }
  public static readonly instance = new DuckDBIntegerType();
}

export class DuckDBBigIntType extends BaseDuckDBType<DuckDBTypeId.BIGINT> {
  private constructor() {
    super(DuckDBTypeId.BIGINT);
  }
  public static readonly instance = new DuckDBBigIntType();
}

export class DuckDBUTinyIntType extends BaseDuckDBType<DuckDBTypeId.UTINYINT> {
  private constructor() {
    super(DuckDBTypeId.UTINYINT);
  }
  public static readonly instance = new DuckDBUTinyIntType();
}

export class DuckDBUSmallIntType extends BaseDuckDBType<DuckDBTypeId.USMALLINT> {
  private constructor() {
    super(DuckDBTypeId.USMALLINT);
  }
  public static readonly instance = new DuckDBUSmallIntType();
}

export class DuckDBUIntegerType extends BaseDuckDBType<DuckDBTypeId.UINTEGER> {
  private constructor() {
    super(DuckDBTypeId.UINTEGER);
  }
  public static readonly instance = new DuckDBUIntegerType();
}

export class DuckDBUBigIntType extends BaseDuckDBType<DuckDBTypeId.UBIGINT> {
  private constructor() {
    super(DuckDBTypeId.UBIGINT);
  }
  public static readonly instance = new DuckDBUBigIntType();
}

export class DuckDBFloatType extends BaseDuckDBType<DuckDBTypeId.FLOAT> {
  private constructor() {
    super(DuckDBTypeId.FLOAT);
  }
  public static readonly instance = new DuckDBFloatType();
}

export class DuckDBDoubleType extends BaseDuckDBType<DuckDBTypeId.DOUBLE> {
  private constructor() {
    super(DuckDBTypeId.DOUBLE);
  }
  public static readonly instance = new DuckDBDoubleType();
}

export class DuckDBTimestampType extends BaseDuckDBType<DuckDBTypeId.TIMESTAMP> {
  private constructor() {
    super(DuckDBTypeId.TIMESTAMP);
  }
  public static readonly instance = new DuckDBTimestampType();
}

export type DuckDBTimestampMicrosecondsType = DuckDBTimestampType;
export const DuckDBTimestampMicrosecondsType = DuckDBTimestampType;

export class DuckDBDateType extends BaseDuckDBType<DuckDBTypeId.DATE> {
  private constructor() {
    super(DuckDBTypeId.DATE);
  }
  public static readonly instance = new DuckDBDateType();
}

export class DuckDBTimeType extends BaseDuckDBType<DuckDBTypeId.TIME> {
  private constructor() {
    super(DuckDBTypeId.TIME);
  }
  public static readonly instance = new DuckDBTimeType();
}

export class DuckDBIntervalType extends BaseDuckDBType<DuckDBTypeId.INTERVAL> {
  private constructor() {
    super(DuckDBTypeId.INTERVAL);
  }
  public static readonly instance = new DuckDBIntervalType();
}

export class DuckDBHugeIntType extends BaseDuckDBType<DuckDBTypeId.HUGEINT> {
  private constructor() {
    super(DuckDBTypeId.HUGEINT);
  }
  public static readonly instance = new DuckDBHugeIntType();
}

export class DuckDBUHugeIntType extends BaseDuckDBType<DuckDBTypeId.UHUGEINT> {
  private constructor() {
    super(DuckDBTypeId.UHUGEINT);
  }
  public static readonly instance = new DuckDBUHugeIntType();
}

export class DuckDBVarCharType extends BaseDuckDBType<DuckDBTypeId.VARCHAR> {
  private constructor() {
    super(DuckDBTypeId.VARCHAR);
  }
  public static readonly instance = new DuckDBVarCharType();
}

export class DuckDBBlobType extends BaseDuckDBType<DuckDBTypeId.BLOB> {
  private constructor() {
    super(DuckDBTypeId.BLOB);
  }
  public static readonly instance = new DuckDBBlobType();
}

export class DuckDBDecimalType extends BaseDuckDBType<DuckDBTypeId.DECIMAL> {
  public readonly width: number;
  public readonly scale: number;
  public constructor(width: number, scale: number) {
    super(DuckDBTypeId.DECIMAL);
    this.width = width;
    this.scale = scale;
  }
  public toString(): string {
    return `DECIMAL(${this.width},${this.scale})`;
  }
  public static readonly default = new DuckDBDecimalType(18, 3);
}

export class DuckDBTimestampSecondsType extends BaseDuckDBType<DuckDBTypeId.TIMESTAMP_S> {
  private constructor() {
    super(DuckDBTypeId.TIMESTAMP_S);
  }
  public static readonly instance = new DuckDBTimestampSecondsType();
}

export class DuckDBTimestampMillisecondsType extends BaseDuckDBType<DuckDBTypeId.TIMESTAMP_MS> {
  private constructor() {
    super(DuckDBTypeId.TIMESTAMP_MS);
  }
  public static readonly instance = new DuckDBTimestampMillisecondsType();
}

export class DuckDBTimestampNanosecondsType extends BaseDuckDBType<DuckDBTypeId.TIMESTAMP_NS> {
  private constructor() {
    super(DuckDBTypeId.TIMESTAMP_NS);
  }
  public static readonly instance = new DuckDBTimestampNanosecondsType();
}

export class DuckDBEnumType extends BaseDuckDBType<DuckDBTypeId.ENUM> {
  public readonly values: readonly string[];
  public readonly internalTypeId: DuckDBTypeId;
  public constructor(values: readonly string[], internalTypeId: DuckDBTypeId) {
    super(DuckDBTypeId.ENUM);
    this.values = values;
    this.internalTypeId = internalTypeId;
  }
  public toString(): string {
    return `ENUM(${this.values.map(quotedString).join(', ')})`;
  }
}

export class DuckDBListType extends BaseDuckDBType<DuckDBTypeId.LIST> {
  public readonly valueType: DuckDBType;
  public constructor(valueType: DuckDBType) {
    super(DuckDBTypeId.LIST);
    this.valueType = valueType;
  }
  public toString(): string {
    return `${this.valueType}[]`;
  }
}

export interface DuckDBStructEntryType {
  readonly name: string;
  readonly valueType: DuckDBType;
}

export class DuckDBStructType extends BaseDuckDBType<DuckDBTypeId.STRUCT> {
  public readonly entries: readonly DuckDBStructEntryType[];
  public constructor(entries: readonly DuckDBStructEntryType[]) {
    super(DuckDBTypeId.STRUCT);
    this.entries = entries;
  }
  public toString(): string {
    return `STRUCT(${this.entries.map(
      entry => `${quotedIdentifier(entry.name)} ${entry.valueType}`
    ).join(', ')})`;
  }
}

export class DuckDBMapType extends BaseDuckDBType<DuckDBTypeId.MAP> {
  public readonly keyType: DuckDBType;
  public readonly valueType: DuckDBType;
  public constructor(keyType: DuckDBType, valueType: DuckDBType) {
    super(DuckDBTypeId.MAP);
    this.keyType = keyType;
    this.valueType = valueType;
  }
  public toString(): string {
    return `MAP(${this.keyType}, ${this.valueType})`;
  }
}

export class DuckDBArrayType extends BaseDuckDBType<DuckDBTypeId.ARRAY> {
  public readonly valueType: DuckDBType;
  public readonly length: number;
  public constructor(valueType: DuckDBType, length: number) {
    super(DuckDBTypeId.ARRAY);
    this.valueType = valueType;
    this.length = length;
  }
  public toString(): string {
    return `${this.valueType}[${this.length}]`;
  }
}

export class DuckDBUUIDType extends BaseDuckDBType<DuckDBTypeId.UUID> {
  private constructor() {
    super(DuckDBTypeId.UUID);
  }
  public static readonly instance = new DuckDBUUIDType();
}

export interface DuckDBUnionAlternativeType {
  readonly tag: string;
  readonly valueType: DuckDBType;
}

export class DuckDBUnionType extends BaseDuckDBType<DuckDBTypeId.UNION> {
  public readonly alternatives: readonly DuckDBUnionAlternativeType[];
  public constructor(alternatives: readonly DuckDBUnionAlternativeType[]) {
    super(DuckDBTypeId.UNION);
    this.alternatives = alternatives;
  }
  public toString(): string {
    return `UNION(${this.alternatives.map(
      entry => `${quotedIdentifier(entry.tag)} ${entry.valueType}`
    ).join(', ')})`;
  }
}

export class DuckDBBitType extends BaseDuckDBType<DuckDBTypeId.BIT> {
  private constructor() {
    super(DuckDBTypeId.BIT);
  }
  public static readonly instance = new DuckDBBitType();
}

export class DuckDBTimeTZType extends BaseDuckDBType<DuckDBTypeId.TIME_TZ> {
  private constructor() {
    super(DuckDBTypeId.TIME_TZ);
  }
  public toString(): string {
    return "TIME WITH TIME ZONE";
  }
  public static readonly instance = new DuckDBTimeTZType();
}

export class DuckDBTimestampTZType extends BaseDuckDBType<DuckDBTypeId.TIMESTAMP_TZ> {
  private constructor() {
    super(DuckDBTypeId.TIMESTAMP_TZ);
  }
  public toString(): string {
    return "TIMESTAMP WITH TIME ZONE";
  }
  public static readonly instance = new DuckDBTimestampTZType();
}

export class DuckDBAnyType extends BaseDuckDBType<DuckDBTypeId.ANY> {
  private constructor() {
    super(DuckDBTypeId.ANY);
  }
  public static readonly instance = new DuckDBAnyType();
}

export class DuckDBVarIntType extends BaseDuckDBType<DuckDBTypeId.VARINT> {
  private constructor() {
    super(DuckDBTypeId.VARINT);
  }
  public static readonly instance = new DuckDBVarIntType();
}

export class DuckDBSQLNullType extends BaseDuckDBType<DuckDBTypeId.SQLNULL> {
  private constructor() {
    super(DuckDBTypeId.SQLNULL);
  }
  public static readonly instance = new DuckDBSQLNullType();
}

export type DuckDBType =
  | DuckDBBooleanType
  | DuckDBTinyIntType
  | DuckDBSmallIntType
  | DuckDBIntegerType
  | DuckDBBigIntType
  | DuckDBUTinyIntType
  | DuckDBUSmallIntType
  | DuckDBUIntegerType
  | DuckDBUBigIntType
  | DuckDBFloatType
  | DuckDBDoubleType
  | DuckDBTimestampType
  | DuckDBDateType
  | DuckDBTimeType
  | DuckDBIntervalType
  | DuckDBHugeIntType
  | DuckDBUHugeIntType
  | DuckDBVarCharType
  | DuckDBBlobType
  | DuckDBDecimalType
  | DuckDBTimestampSecondsType
  | DuckDBTimestampMillisecondsType
  | DuckDBTimestampNanosecondsType
  | DuckDBEnumType
  | DuckDBListType
  | DuckDBStructType
  | DuckDBMapType
  | DuckDBArrayType
  | DuckDBUUIDType
  | DuckDBUnionType
  | DuckDBBitType
  | DuckDBTimeTZType
  | DuckDBTimestampTZType
  | DuckDBAnyType
  | DuckDBVarIntType
  | DuckDBSQLNullType
  ;
