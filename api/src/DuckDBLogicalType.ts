import duckdb from '@duckdb/node-bindings';
import {
  DuckDBAnyType,
  DuckDBArrayType,
  DuckDBBigIntType,
  DuckDBBitType,
  DuckDBBlobType,
  DuckDBBooleanType,
  DuckDBDateType,
  DuckDBDecimalType,
  DuckDBDoubleType,
  DuckDBEnumType,
  DuckDBFloatType,
  DuckDBHugeIntType,
  DuckDBIntegerType,
  DuckDBIntervalType,
  DuckDBListType,
  DuckDBMapType,
  DuckDBSQLNullType,
  DuckDBSmallIntType,
  DuckDBStructType,
  DuckDBTimeTZType,
  DuckDBTimeType,
  DuckDBTimestampMillisecondsType,
  DuckDBTimestampNanosecondsType,
  DuckDBTimestampSecondsType,
  DuckDBTimestampTZType,
  DuckDBTimestampType,
  DuckDBTinyIntType,
  DuckDBType,
  DuckDBUBigIntType,
  DuckDBUHugeIntType,
  DuckDBUIntegerType,
  DuckDBUSmallIntType,
  DuckDBUTinyIntType,
  DuckDBUUIDType,
  DuckDBUnionType,
  DuckDBVarCharType,
  DuckDBVarIntType,
} from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';

export class DuckDBLogicalType {
  readonly logical_type: duckdb.LogicalType;
  protected constructor(logical_type: duckdb.LogicalType) {
    this.logical_type = logical_type;
  }
  static create(logical_type: duckdb.LogicalType): DuckDBLogicalType {
    switch (duckdb.get_type_id(logical_type)) {
      case duckdb.Type.DECIMAL:
        return new DuckDBDecimalLogicalType(logical_type);
      case duckdb.Type.ENUM:
        return new DuckDBEnumLogicalType(logical_type);
      case duckdb.Type.LIST:
        return new DuckDBListLogicalType(logical_type);
      case duckdb.Type.STRUCT:
        return new DuckDBStructLogicalType(logical_type);
      case duckdb.Type.MAP:
        return new DuckDBMapLogicalType(logical_type);
      case duckdb.Type.ARRAY:
        return new DuckDBArrayLogicalType(logical_type);
      case duckdb.Type.UNION:
        return new DuckDBUnionLogicalType(logical_type);
      default:
        return new DuckDBLogicalType(logical_type);
    }
  }
  public static createDecimal(
    width: number,
    scale: number
  ): DuckDBDecimalLogicalType {
    return new DuckDBDecimalLogicalType(
      duckdb.create_decimal_type(width, scale)
    );
  }
  public static createEnum(
    member_names: readonly string[]
  ): DuckDBEnumLogicalType {
    return new DuckDBEnumLogicalType(duckdb.create_enum_type(member_names));
  }
  public static createList(
    valueType: DuckDBLogicalType
  ): DuckDBListLogicalType {
    return new DuckDBListLogicalType(
      duckdb.create_list_type(valueType.logical_type)
    );
  }
  public static createStruct(
    entryNames: readonly string[],
    entryLogicalTypes: readonly DuckDBLogicalType[],    
  ): DuckDBStructLogicalType {
    const length = entryNames.length;
    if (length !== entryLogicalTypes.length) {
      throw new Error(`Could not create struct: \
        entryNames length (${entryNames.length}) does not match entryLogicalTypes length (${entryLogicalTypes.length})`);
    }
    const member_types: duckdb.LogicalType[] = [];
    const member_names: string[] = [];
    for (let i = 0; i < length; i++) {
      member_types.push(entryLogicalTypes[i].logical_type);
      member_names.push(entryNames[i]);
    }
    return new DuckDBStructLogicalType(
      duckdb.create_struct_type(member_types, member_names)
    );
  }
  public static createMap(
    keyType: DuckDBLogicalType,
    valueType: DuckDBLogicalType
  ): DuckDBMapLogicalType {
    return new DuckDBMapLogicalType(
      duckdb.create_map_type(keyType.logical_type, valueType.logical_type)
    );
  }
  public static createArray(
    valueType: DuckDBLogicalType,
    length: number
  ): DuckDBArrayLogicalType {
    return new DuckDBArrayLogicalType(
      duckdb.create_array_type(valueType.logical_type, length)
    );
  }
  public static createUnion(
    memberTags: readonly string[],
    memberLogicalTypes: readonly DuckDBLogicalType[], 
  ): DuckDBUnionLogicalType {
    const length = memberTags.length;
    if (length !== memberLogicalTypes.length) {
      throw new Error(`Could not create union: \
        memberTags length (${memberTags.length}) does not match memberLogicalTypes length (${memberLogicalTypes.length})`);
    }
    const member_types: duckdb.LogicalType[] = [];
    const member_names: string[] = [];
    for (let i = 0; i < length; i++) {
      member_types.push(memberLogicalTypes[i].logical_type);
      member_names.push(memberTags[i]);
    }
    return new DuckDBUnionLogicalType(
      duckdb.create_union_type(member_types, member_names)
    );
  }
  public get typeId(): DuckDBTypeId {
    return duckdb.get_type_id(this.logical_type) as number as DuckDBTypeId;
  }
  public get alias(): string | undefined {
    return duckdb.logical_type_get_alias(this.logical_type) || undefined;
  }
  public set alias(newAlias: string | null | undefined) {
    duckdb.logical_type_set_alias(this.logical_type, newAlias || '');
  }
  public asType(): DuckDBType {
    const alias = this.alias;
    switch (this.typeId) {
      case DuckDBTypeId.BOOLEAN:
        return  DuckDBBooleanType.create(alias);
      case DuckDBTypeId.TINYINT:
        return DuckDBTinyIntType.create(alias);
      case DuckDBTypeId.SMALLINT:
        return DuckDBSmallIntType.create(alias);
      case DuckDBTypeId.INTEGER:
        return DuckDBIntegerType.create(alias);
      case DuckDBTypeId.BIGINT:
        return DuckDBBigIntType.create(alias);
      case DuckDBTypeId.UTINYINT:
        return DuckDBUTinyIntType.create(alias);
      case DuckDBTypeId.USMALLINT:
        return DuckDBUSmallIntType.create(alias);
      case DuckDBTypeId.UINTEGER:
        return DuckDBUIntegerType.create(alias);
      case DuckDBTypeId.UBIGINT:
        return DuckDBUBigIntType.create(alias);
      case DuckDBTypeId.FLOAT:
        return DuckDBFloatType.create(alias);
      case DuckDBTypeId.DOUBLE:
        return DuckDBDoubleType.create(alias);
      case DuckDBTypeId.TIMESTAMP:
        return DuckDBTimestampType.create(alias);
      case DuckDBTypeId.DATE:
        return DuckDBDateType.create(alias);
      case DuckDBTypeId.TIME:
        return DuckDBTimeType.create(alias);
      case DuckDBTypeId.INTERVAL:
        return DuckDBIntervalType.create(alias);
      case DuckDBTypeId.HUGEINT:
        return DuckDBHugeIntType.create(alias);
      case DuckDBTypeId.UHUGEINT:
        return DuckDBUHugeIntType.create(alias);
      case DuckDBTypeId.VARCHAR:
        return DuckDBVarCharType.create(alias);
      case DuckDBTypeId.BLOB:
        return DuckDBBlobType.create(alias);
      case DuckDBTypeId.DECIMAL:
        throw new Error('Expected override');
      case DuckDBTypeId.TIMESTAMP_S:
        return DuckDBTimestampSecondsType.create(alias);
      case DuckDBTypeId.TIMESTAMP_MS:
        return DuckDBTimestampMillisecondsType.create(alias);
      case DuckDBTypeId.TIMESTAMP_NS:
        return DuckDBTimestampNanosecondsType.create(alias);
      case DuckDBTypeId.ENUM:
        throw new Error('Expected override');
      case DuckDBTypeId.LIST:
        throw new Error('Expected override');
      case DuckDBTypeId.STRUCT:
        throw new Error('Expected override');
      case DuckDBTypeId.MAP:
        throw new Error('Expected override');
      case DuckDBTypeId.ARRAY:
        throw new Error('Expected override');
      case DuckDBTypeId.UUID:
        return DuckDBUUIDType.create(alias);
      case DuckDBTypeId.UNION:
        throw new Error('Expected override');
      case DuckDBTypeId.BIT:
        return DuckDBBitType.create(alias);
      case DuckDBTypeId.TIME_TZ:
        return DuckDBTimeTZType.create(alias);
      case DuckDBTypeId.TIMESTAMP_TZ:
        return DuckDBTimestampTZType.create(alias);
      case DuckDBTypeId.ANY:
        return DuckDBAnyType.create(alias);
      case DuckDBTypeId.VARINT:
        return DuckDBVarIntType.create(alias);
      case DuckDBTypeId.SQLNULL:
        return DuckDBSQLNullType.create(alias);
      default:
        throw new Error(`Unexpected type id: ${this.typeId}`);
    }
  }
}

export class DuckDBDecimalLogicalType extends DuckDBLogicalType {
  public get width(): number {
    return duckdb.decimal_width(this.logical_type);
  }
  public get scale(): number {
    return duckdb.decimal_scale(this.logical_type);
  }
  public get internalTypeId(): DuckDBTypeId {
    return duckdb.decimal_internal_type(
      this.logical_type
    ) as number as DuckDBTypeId;
  }
  public override asType(): DuckDBDecimalType {
    return new DuckDBDecimalType(this.width, this.scale, this.alias);
  }
}

export class DuckDBEnumLogicalType extends DuckDBLogicalType {
  public get valueCount(): number {
    return duckdb.enum_dictionary_size(this.logical_type);
  }
  public value(index: number): string {
    return duckdb.enum_dictionary_value(this.logical_type, index);
  }
  public values(): readonly string[] {
    const values: string[] = [];
    const count = this.valueCount;
    for (let i = 0; i < count; i++) {
      values.push(this.value(i));
    }
    return values;
  }
  public get internalTypeId(): DuckDBTypeId {
    return duckdb.enum_internal_type(
      this.logical_type
    ) as number as DuckDBTypeId;
  }
  public override asType(): DuckDBEnumType {
    return new DuckDBEnumType(this.values(), this.internalTypeId, this.alias);
  }
}

export class DuckDBListLogicalType extends DuckDBLogicalType {
  public get valueType(): DuckDBLogicalType {
    return DuckDBLogicalType.create(
      duckdb.list_type_child_type(this.logical_type)
    );
  }
  public override asType(): DuckDBListType {
    return new DuckDBListType(this.valueType.asType(), this.alias);
  }
}

export class DuckDBStructLogicalType extends DuckDBLogicalType {
  public get entryCount(): number {
    return duckdb.struct_type_child_count(this.logical_type);
  }
  public entryName(index: number): string {
    return duckdb.struct_type_child_name(this.logical_type, index);
  }
  public entryLogicalType(index: number): DuckDBLogicalType {
    return DuckDBLogicalType.create(
      duckdb.struct_type_child_type(this.logical_type, index)
    );
  }
  public entryType(index: number): DuckDBType {
    return this.entryLogicalType(index).asType();
  }
  public entryNames(): string[] {
    const names: string[] = [];
    const count = this.entryCount;
    for (let i = 0; i < count; i++) {
      names.push(this.entryName(i));
    }
    return names;
  }
  public entryLogicalTypes(): DuckDBLogicalType[] {
    const valueTypes: DuckDBLogicalType[] = [];
    const count = this.entryCount;
    for (let i = 0; i < count; i++) {
      valueTypes.push(this.entryLogicalType(i));
    }
    return valueTypes;
  }
  public entryTypes(): DuckDBType[] {
    const valueTypes: DuckDBType[] = [];
    const count = this.entryCount;
    for (let i = 0; i < count; i++) {
      valueTypes.push(this.entryType(i));
    }
    return valueTypes;
  }
  public override asType(): DuckDBStructType {
    return new DuckDBStructType(this.entryNames(), this.entryTypes(), this.alias);
  }
}

export class DuckDBMapLogicalType extends DuckDBLogicalType {
  public get keyType(): DuckDBLogicalType {
    return DuckDBLogicalType.create(
      duckdb.map_type_key_type(this.logical_type)
    );
  }
  public get valueType(): DuckDBLogicalType {
    return DuckDBLogicalType.create(
      duckdb.map_type_value_type(this.logical_type)
    );
  }
  public override asType(): DuckDBMapType {
    return new DuckDBMapType(this.keyType.asType(), this.valueType.asType(), this.alias);
  }
}

export class DuckDBArrayLogicalType extends DuckDBLogicalType {
  public get valueType(): DuckDBLogicalType {
    return DuckDBLogicalType.create(
      duckdb.array_type_child_type(this.logical_type)
    );
  }
  public get length(): number {
    return duckdb.array_type_array_size(this.logical_type);
  }
  public override asType(): DuckDBArrayType {
    return new DuckDBArrayType(this.valueType.asType(), this.length, this.alias);
  }
}

export class DuckDBUnionLogicalType extends DuckDBLogicalType {
  public get memberCount(): number {
    return duckdb.union_type_member_count(this.logical_type);
  }
  public memberTag(index: number): string {
    return duckdb.union_type_member_name(this.logical_type, index);
  }
  public memberLogicalType(index: number): DuckDBLogicalType {
    return DuckDBLogicalType.create(
      duckdb.union_type_member_type(this.logical_type, index)
    );
  }
  public memberType(index: number): DuckDBType {
    return this.memberLogicalType(index).asType();
  }
  public memberTags(): string[] {
    const tags: string[] = [];
    const count = this.memberCount;
    for (let i = 0; i < count; i++) {
      tags.push(this.memberTag(i));
    }
    return tags;
  }
  public memberLogicalTypes(): DuckDBLogicalType[] {
    const valueTypes: DuckDBLogicalType[] = [];
    const count = this.memberCount;
    for (let i = 0; i < count; i++) {
      valueTypes.push(this.memberLogicalType(i));
    }
    return valueTypes;
  }
  public memberTypes(): DuckDBType[] {
    const valueTypes: DuckDBType[] = [];
    const count = this.memberCount;
    for (let i = 0; i < count; i++) {
      valueTypes.push(this.memberType(i));
    }
    return valueTypes;
  }
  public override asType(): DuckDBUnionType {
    return new DuckDBUnionType(this.memberTags(), this.memberTypes(), this.alias);
  }
}
