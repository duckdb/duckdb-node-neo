import duckdb from '@duckdb/node-bindings';

export interface ExpectedSimpleLogicalType {
  typeId: Exclude<duckdb.Type,
    | duckdb.Type.ARRAY
    | duckdb.Type.DECIMAL
    | duckdb.Type.ENUM
    | duckdb.Type.LIST
    | duckdb.Type.MAP
    | duckdb.Type.STRUCT
    | duckdb.Type.UNION
  >;
}

export interface ExpectedArrayLogicalType {
  typeId: duckdb.Type.ARRAY;
  valueType: ExpectedLogicalType;
  size: number;
}

export interface ExpectedDecimalLogicalType {
  typeId: duckdb.Type.DECIMAL;
  width: number;
  scale: number;
  internalType: duckdb.Type;
}

export interface ExpectedEnumLogicalType {
  typeId: duckdb.Type.ENUM;
  values: string[];
  internalType: duckdb.Type;
}

export interface ExpectedListLogicalType {
  typeId: duckdb.Type.LIST;
  valueType: ExpectedLogicalType;
}

export interface ExpectedMapLogicalType {
  typeId: duckdb.Type.MAP;
  keyType: ExpectedLogicalType;
  valueType: ExpectedLogicalType;
}

export interface ExpectedStructEntry {
  name: string;
  type: ExpectedLogicalType;
}

export interface ExpectedStructLogicalType {
  typeId: duckdb.Type.STRUCT;
  entries: ExpectedStructEntry[];
}

export interface ExpectedUnionAlternative {
  tag: string;
  type: ExpectedLogicalType;
}

export interface ExpectedUnionLogicalType {
  typeId: duckdb.Type.UNION;
  alternatives: ExpectedUnionAlternative[];
}

export type ExpectedLogicalType =
  | ExpectedSimpleLogicalType
  | ExpectedArrayLogicalType
  | ExpectedDecimalLogicalType
  | ExpectedEnumLogicalType
  | ExpectedListLogicalType
  | ExpectedMapLogicalType
  | ExpectedStructLogicalType
  | ExpectedUnionLogicalType
  ;
