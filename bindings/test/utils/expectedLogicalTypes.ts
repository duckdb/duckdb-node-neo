import duckdb from '@duckdb/node-bindings';
import {
  ExpectedArrayLogicalType,
  ExpectedDecimalLogicalType,
  ExpectedEnumLogicalType,
  ExpectedListLogicalType,
  ExpectedLogicalType,
  ExpectedMapLogicalType,
  ExpectedSimpleLogicalType,
  ExpectedStructEntry,
  ExpectedStructLogicalType,
  ExpectedUnionAlternative,
  ExpectedUnionLogicalType,
} from './ExpectedLogicalType';

export const BOOLEAN: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.BOOLEAN,
};

export const TINYINT: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.TINYINT,
};
export const SMALLINT: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.SMALLINT,
};
export const INTEGER: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.INTEGER,
};
export const BIGINT: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.BIGINT,
};

export const UTINYINT: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.UTINYINT,
};
export const USMALLINT: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.USMALLINT,
};
export const UINTEGER: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.UINTEGER,
};
export const UBIGINT: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.UBIGINT,
};

export const FLOAT: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.FLOAT,
};
export const DOUBLE: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.DOUBLE,
};

export const TIMESTAMP: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.TIMESTAMP,
};
export const DATE: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.DATE,
};
export const TIME: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.TIME,
};
export const INTERVAL: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.INTERVAL,
};

export const HUGEINT: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.HUGEINT,
};
export const UHUGEINT: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.UHUGEINT,
};

export const VARCHAR: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.VARCHAR,
};
export const BLOB: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.BLOB,
};

export const TIMESTAMP_S: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.TIMESTAMP_S,
};
export const TIMESTAMP_MS: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.TIMESTAMP_MS,
};
export const TIMESTAMP_NS: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.TIMESTAMP_NS,
};

export const UUID: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.UUID,
};

export const BIT: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.BIT,
};

export const TIME_TZ: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.TIME_TZ,
};
export const TIMESTAMP_TZ: ExpectedSimpleLogicalType = {
  typeId: duckdb.Type.TIMESTAMP_TZ,
};

export function ARRAY(
  valueType: ExpectedLogicalType,
  size: number
): ExpectedArrayLogicalType {
  return {
    typeId: duckdb.Type.ARRAY,
    valueType,
    size,
  };
}

export function DECIMAL(
  width: number,
  scale: number,
  internalType: duckdb.Type
): ExpectedDecimalLogicalType {
  return {
    typeId: duckdb.Type.DECIMAL,
    width,
    scale,
    internalType,
  };
}

export function ENUM(
  values: string[],
  internalType: duckdb.Type
): ExpectedEnumLogicalType {
  return {
    typeId: duckdb.Type.ENUM,
    values,
    internalType,
  };
}

export function LIST(valueType: ExpectedLogicalType): ExpectedListLogicalType {
  return {
    typeId: duckdb.Type.LIST,
    valueType,
  };
}

export function MAP(
  keyType: ExpectedLogicalType,
  valueType: ExpectedLogicalType
): ExpectedMapLogicalType {
  return {
    typeId: duckdb.Type.MAP,
    keyType,
    valueType,
  };
}

export function ENTRY(
  name: string,
  type: ExpectedLogicalType
): ExpectedStructEntry {
  return {
    name,
    type,
  };
}

export function STRUCT(
  ...entries: ExpectedStructEntry[]
): ExpectedStructLogicalType {
  return {
    typeId: duckdb.Type.STRUCT,
    entries,
  };
}

export function ALT(
  tag: string,
  type: ExpectedLogicalType
): ExpectedUnionAlternative {
  return {
    tag,
    type,
  };
}

export function UNION(
  ...alternatives: ExpectedUnionAlternative[]
): ExpectedUnionLogicalType {
  return {
    typeId: duckdb.Type.UNION,
    alternatives,
  };
}
