import { DuckDBTypeId } from './DuckDBTypeId';
import { quotedIdentifier, quotedString } from './sql';

export abstract class BaseDuckDBType<T extends DuckDBTypeId> {
  public readonly typeId: T;
  public readonly alias?: string;
  protected constructor(typeId: T, alias?: string) {
    this.typeId = typeId;
    this.alias = alias;
  }
  public toString(): string {
    return DuckDBTypeId[this.typeId];
  }
}

export class DuckDBBooleanType extends BaseDuckDBType<DuckDBTypeId.BOOLEAN> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.BOOLEAN, alias);
  }
  public static readonly instance = new DuckDBBooleanType();
  public static create(alias?: string): DuckDBBooleanType {
    return alias ? new DuckDBBooleanType(alias) : DuckDBBooleanType.instance;
  }
}

export class DuckDBTinyIntType extends BaseDuckDBType<DuckDBTypeId.TINYINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TINYINT, alias);
  }
  public static readonly instance = new DuckDBTinyIntType();
  public static create(alias?: string): DuckDBTinyIntType {
    return alias ? new DuckDBTinyIntType(alias) : DuckDBTinyIntType.instance;
  }
  public static readonly Max = 2 ** 7 - 1;
  public static readonly Min = -(2 ** 7);
}

export class DuckDBSmallIntType extends BaseDuckDBType<DuckDBTypeId.SMALLINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.SMALLINT, alias);
  }
  public static readonly instance = new DuckDBSmallIntType();
  public static create(alias?: string): DuckDBSmallIntType {
    return alias ? new DuckDBSmallIntType(alias) : DuckDBSmallIntType.instance;
  }
  public static readonly Max = 2 ** 15 - 1;
  public static readonly Min = -(2 ** 15);
}

export class DuckDBIntegerType extends BaseDuckDBType<DuckDBTypeId.INTEGER> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.INTEGER, alias);
  }
  public static readonly instance = new DuckDBIntegerType();
  public static create(alias?: string): DuckDBIntegerType {
    return alias ? new DuckDBIntegerType(alias) : DuckDBIntegerType.instance;
  }
  public static readonly Max = 2 ** 31 - 1;
  public static readonly Min = -(2 ** 31);
}

export class DuckDBBigIntType extends BaseDuckDBType<DuckDBTypeId.BIGINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.BIGINT, alias);
  }
  public static readonly instance = new DuckDBBigIntType();
  public static create(alias?: string): DuckDBBigIntType {
    return alias ? new DuckDBBigIntType(alias) : DuckDBBigIntType.instance;
  }
  public static readonly Max: bigint = 2n ** 63n - 1n;
  public static readonly Min: bigint = -(2n ** 63n);
}

export class DuckDBUTinyIntType extends BaseDuckDBType<DuckDBTypeId.UTINYINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.UTINYINT, alias);
  }
  public static readonly instance = new DuckDBUTinyIntType();
  public static create(alias?: string): DuckDBUTinyIntType {
    return alias ? new DuckDBUTinyIntType(alias) : DuckDBUTinyIntType.instance;
  }
  public static readonly Max = 2 ** 8 - 1;
  public static readonly Min = 0;
}

export class DuckDBUSmallIntType extends BaseDuckDBType<DuckDBTypeId.USMALLINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.USMALLINT, alias);
  }
  public static readonly instance = new DuckDBUSmallIntType();
  public static create(alias?: string): DuckDBUSmallIntType {
    return alias ? new DuckDBUSmallIntType(alias) : DuckDBUSmallIntType.instance;
  }
  public static readonly Max = 2 ** 16 - 1;
  public static readonly Min = 0;
}

export class DuckDBUIntegerType extends BaseDuckDBType<DuckDBTypeId.UINTEGER> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.UINTEGER, alias);
  }
  public static readonly instance = new DuckDBUIntegerType();
  public static create(alias?: string): DuckDBUIntegerType {
    return alias ? new DuckDBUIntegerType(alias) : DuckDBUIntegerType.instance;
  }
  public static readonly Max = 2 ** 32 - 1;
  public static readonly Min = 0;
}

export class DuckDBUBigIntType extends BaseDuckDBType<DuckDBTypeId.UBIGINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.UBIGINT, alias);
  }
  public static readonly instance = new DuckDBUBigIntType();
  public static create(alias?: string): DuckDBUBigIntType {
    return alias ? new DuckDBUBigIntType(alias) : DuckDBUBigIntType.instance;
  }
  public static readonly Max: bigint = 2n ** 64n - 1n;
  public static readonly Min: bigint = 0n;
}

export class DuckDBFloatType extends BaseDuckDBType<DuckDBTypeId.FLOAT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.FLOAT, alias);
  }
  public static readonly instance = new DuckDBFloatType();
  public static create(alias?: string): DuckDBFloatType {
    return alias ? new DuckDBFloatType(alias) : DuckDBFloatType.instance;
  }
  public static readonly Min = Math.fround(-3.4028235e+38);
  public static readonly Max = Math.fround( 3.4028235e+38);
}

export class DuckDBDoubleType extends BaseDuckDBType<DuckDBTypeId.DOUBLE> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.DOUBLE, alias);
  }
  public static readonly instance = new DuckDBDoubleType();
  public static create(alias?: string): DuckDBDoubleType {
    return alias ? new DuckDBDoubleType(alias) : DuckDBDoubleType.instance;
  }
  public static readonly Min = -Number.MAX_VALUE;
  public static readonly Max = Number.MAX_VALUE;
}

export class DuckDBTimestampType extends BaseDuckDBType<DuckDBTypeId.TIMESTAMP> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TIMESTAMP, alias);
  }
  public static readonly instance = new DuckDBTimestampType();
  public static create(alias?: string): DuckDBTimestampType {
    return alias ? new DuckDBTimestampType(alias) : DuckDBTimestampType.instance;
  }
}

export type DuckDBTimestampMicrosecondsType = DuckDBTimestampType;
export const DuckDBTimestampMicrosecondsType = DuckDBTimestampType;

export class DuckDBDateType extends BaseDuckDBType<DuckDBTypeId.DATE> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.DATE, alias);
  }
  public static readonly instance = new DuckDBDateType();
  public static create(alias?: string): DuckDBDateType {
    return alias ? new DuckDBDateType(alias) : DuckDBDateType.instance;
  }
}

export class DuckDBTimeType extends BaseDuckDBType<DuckDBTypeId.TIME> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TIME, alias);
  }
  public static readonly instance = new DuckDBTimeType();
  public static create(alias?: string): DuckDBTimeType {
    return alias ? new DuckDBTimeType(alias) : DuckDBTimeType.instance;
  }
}

export class DuckDBIntervalType extends BaseDuckDBType<DuckDBTypeId.INTERVAL> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.INTERVAL, alias);
  }
  public static readonly instance = new DuckDBIntervalType();
  public static create(alias?: string): DuckDBIntervalType {
    return alias ? new DuckDBIntervalType(alias) : DuckDBIntervalType.instance;
  }
}

export class DuckDBHugeIntType extends BaseDuckDBType<DuckDBTypeId.HUGEINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.HUGEINT, alias);
  }
  public static readonly instance = new DuckDBHugeIntType();
  public static create(alias?: string): DuckDBHugeIntType {
    return alias ? new DuckDBHugeIntType(alias) : DuckDBHugeIntType.instance;
  }
  public static readonly Max: bigint = 2n ** 127n - 1n;
  public static readonly Min: bigint = -(2n ** 127n);
}

export class DuckDBUHugeIntType extends BaseDuckDBType<DuckDBTypeId.UHUGEINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.UHUGEINT, alias);
  }
  public static readonly instance = new DuckDBUHugeIntType();
  public static create(alias?: string): DuckDBUHugeIntType {
    return alias ? new DuckDBUHugeIntType(alias) : DuckDBUHugeIntType.instance;
  }
  public static readonly Max: bigint = 2n ** 128n - 1n;
  public static readonly Min: bigint = 0n;
}

export class DuckDBVarCharType extends BaseDuckDBType<DuckDBTypeId.VARCHAR> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.VARCHAR, alias);
  }
  public static readonly instance = new DuckDBVarCharType();
  public static create(alias?: string): DuckDBVarCharType {
    return alias ? new DuckDBVarCharType(alias) : DuckDBVarCharType.instance;
  }
}

export class DuckDBBlobType extends BaseDuckDBType<DuckDBTypeId.BLOB> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.BLOB, alias);
  }
  public static readonly instance = new DuckDBBlobType();
  public static create(alias?: string): DuckDBBlobType {
    return alias ? new DuckDBBlobType(alias) : DuckDBBlobType.instance;
  }
}

export class DuckDBDecimalType extends BaseDuckDBType<DuckDBTypeId.DECIMAL> {
  public readonly width: number;
  public readonly scale: number;
  public constructor(width: number, scale: number, alias?: string) {
    super(DuckDBTypeId.DECIMAL, alias);
    this.width = width;
    this.scale = scale;
  }
  public toString(): string {
    return `DECIMAL(${this.width},${this.scale})`;
  }
  public static readonly default = new DuckDBDecimalType(18, 3);
}

export class DuckDBTimestampSecondsType extends BaseDuckDBType<DuckDBTypeId.TIMESTAMP_S> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TIMESTAMP_S, alias);
  }
  public static readonly instance = new DuckDBTimestampSecondsType();
  public static create(alias?: string): DuckDBTimestampSecondsType {
    return alias ? new DuckDBTimestampSecondsType(alias) : DuckDBTimestampSecondsType.instance;
  }
}

export class DuckDBTimestampMillisecondsType extends BaseDuckDBType<DuckDBTypeId.TIMESTAMP_MS> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TIMESTAMP_MS, alias);
  }
  public static readonly instance = new DuckDBTimestampMillisecondsType();
  public static create(alias?: string): DuckDBTimestampMillisecondsType {
    return alias ? new DuckDBTimestampMillisecondsType(alias) : DuckDBTimestampMillisecondsType.instance;
  }
}

export class DuckDBTimestampNanosecondsType extends BaseDuckDBType<DuckDBTypeId.TIMESTAMP_NS> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TIMESTAMP_NS, alias);
  }
  public static readonly instance = new DuckDBTimestampNanosecondsType();
  public static create(alias?: string): DuckDBTimestampNanosecondsType {
    return alias ? new DuckDBTimestampNanosecondsType(alias) : DuckDBTimestampNanosecondsType.instance;
  }
}

export class DuckDBEnumType extends BaseDuckDBType<DuckDBTypeId.ENUM> {
  public readonly values: readonly string[];
  public readonly internalTypeId: DuckDBTypeId;
  public constructor(values: readonly string[], internalTypeId: DuckDBTypeId, alias?: string) {
    super(DuckDBTypeId.ENUM, alias);
    this.values = values;
    this.internalTypeId = internalTypeId;
  }
  public toString(): string {
    return `ENUM(${this.values.map(quotedString).join(', ')})`;
  }
}

export class DuckDBListType extends BaseDuckDBType<DuckDBTypeId.LIST> {
  public readonly valueType: DuckDBType;
  public constructor(valueType: DuckDBType, alias?: string) {
    super(DuckDBTypeId.LIST, alias);
    this.valueType = valueType;
  }
  public toString(): string {
    return `${this.valueType}[]`;
  }
}

export class DuckDBStructType extends BaseDuckDBType<DuckDBTypeId.STRUCT> {
  public readonly entryNames: readonly string[];
  public readonly entryTypes: readonly DuckDBType[];
  public constructor(entryNames: readonly string[], entryTypes: readonly DuckDBType[], alias?: string) {
    super(DuckDBTypeId.STRUCT, alias);
    if (entryNames.length !== entryTypes.length) {
      throw new Error(`Could not create DuckDBStructType: \
        entryNames length (${entryNames.length}) does not match entryTypes length (${entryTypes.length})`);
    }
    this.entryNames = entryNames;
    this.entryTypes = entryTypes;
  }
  public get entryCount() {
    return this.entryNames.length;
  }
  public toString(): string {
    const parts: string[] = [];
    for (let i = 0; i < this.entryNames.length; i++) {
      parts.push(`${quotedIdentifier(this.entryNames[i])} ${this.entryTypes[i]}`);
    }
    return `STRUCT(${parts.join(', ')})`;
  }
}

export class DuckDBMapType extends BaseDuckDBType<DuckDBTypeId.MAP> {
  public readonly keyType: DuckDBType;
  public readonly valueType: DuckDBType;
  public constructor(keyType: DuckDBType, valueType: DuckDBType, alias?: string) {
    super(DuckDBTypeId.MAP, alias);
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
  public constructor(valueType: DuckDBType, length: number, alias?: string) {
    super(DuckDBTypeId.ARRAY, alias);
    this.valueType = valueType;
    this.length = length;
  }
  public toString(): string {
    return `${this.valueType}[${this.length}]`;
  }
}

export class DuckDBUUIDType extends BaseDuckDBType<DuckDBTypeId.UUID> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.UUID, alias);
  }
  public static readonly instance = new DuckDBUUIDType();
  public static create(alias?: string): DuckDBUUIDType {
    return alias ? new DuckDBUUIDType(alias) : DuckDBUUIDType.instance;
  }
}

export class DuckDBUnionType extends BaseDuckDBType<DuckDBTypeId.UNION> {
  public readonly memberTags: readonly string[];
  public readonly memberTypes: readonly DuckDBType[];
  public constructor(memberTags: readonly string[], memberTypes: readonly DuckDBType[], alias?: string) {
    super(DuckDBTypeId.UNION, alias);
    if (memberTags.length !== memberTypes.length) {
      throw new Error(`Could not create DuckDBUnionType: \
        tags length (${memberTags.length}) does not match valueTypes length (${memberTypes.length})`);
    }
    this.memberTags = memberTags;
    this.memberTypes = memberTypes;
  }
  public get memberCount() {
    return this.memberTags.length;
  }
  public toString(): string {
    const parts: string[] = [];
    for (let i = 0; i < this.memberTags.length; i++) {
      parts.push(`${quotedIdentifier(this.memberTags[i])} ${this.memberTypes[i]}`);
    }
    return `UNION(${parts.join(', ')})`;
  }
}

export class DuckDBBitType extends BaseDuckDBType<DuckDBTypeId.BIT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.BIT, alias);
  }
  public static readonly instance = new DuckDBBitType();
  public static create(alias?: string): DuckDBBitType {
    return alias ? new DuckDBBitType(alias) : DuckDBBitType.instance;
  }
}

export class DuckDBTimeTZType extends BaseDuckDBType<DuckDBTypeId.TIME_TZ> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TIME_TZ, alias);
  }
  public toString(): string {
    return "TIME WITH TIME ZONE";
  }
  public static readonly instance = new DuckDBTimeTZType();
  public static create(alias?: string): DuckDBTimeTZType {
    return alias ? new DuckDBTimeTZType(alias) : DuckDBTimeTZType.instance;
  }
}

export class DuckDBTimestampTZType extends BaseDuckDBType<DuckDBTypeId.TIMESTAMP_TZ> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TIMESTAMP_TZ, alias);
  }
  public toString(): string {
    return "TIMESTAMP WITH TIME ZONE";
  }
  public static readonly instance = new DuckDBTimestampTZType();
  public static create(alias?: string): DuckDBTimestampTZType {
    return alias ? new DuckDBTimestampTZType(alias) : DuckDBTimestampTZType.instance;
  }
}

export class DuckDBAnyType extends BaseDuckDBType<DuckDBTypeId.ANY> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.ANY, alias);
  }
  public static readonly instance = new DuckDBAnyType();
  public static create(alias?: string): DuckDBAnyType {
    return alias ? new DuckDBAnyType(alias) : DuckDBAnyType.instance;
  }
}

export class DuckDBVarIntType extends BaseDuckDBType<DuckDBTypeId.VARINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.VARINT, alias);
  }
  public static readonly instance = new DuckDBVarIntType();
  public static create(alias?: string): DuckDBVarIntType {
    return alias ? new DuckDBVarIntType(alias) : DuckDBVarIntType.instance;
  }
  public static readonly Max: bigint =  179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368n;
  public static readonly Min: bigint = -179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368n;
}

export class DuckDBSQLNullType extends BaseDuckDBType<DuckDBTypeId.SQLNULL> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.SQLNULL, alias);
  }
  public static readonly instance = new DuckDBSQLNullType();
  public static create(alias?: string): DuckDBSQLNullType {
    return alias ? new DuckDBSQLNullType(alias) : DuckDBSQLNullType.instance;
  }
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
