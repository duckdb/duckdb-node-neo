import type { DuckDBType } from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';
import type { Json } from './Json';

/**
 * The JSON-compatible type produced by `JsonDuckDBValueConverter` for each
 * DuckDB type id.
 *
 * Values are non-NULL results only: SQL NULL is handled by
 * `createDuckDBValueConverter`, which yields `null` for any type. Callers
 * representing a nullable value should use `JsonTypeForTypeId[Id] | null`.
 *
 * This differs from `JSTypeForTypeId` wherever a DuckDB value has no lossless
 * JSON representation: 64-bit and wider integers, temporal types, DECIMAL,
 * BLOB, BIT, and GEOMETRY are rendered as strings rather than as `bigint`,
 * `Date`, or `Uint8Array`, and FLOAT and DOUBLE widen to `number | string` so
 * that Infinity and NaN survive as strings.
 *
 * Type ids that cannot appear in a result (`INVALID`, `ANY`,
 * `STRING_LITERAL`, `INTEGER_LITERAL`) map to `never`, since their
 * converters throw.
 *
 * Nested types bottom out at `Json`, because `DuckDBListType`,
 * `DuckDBArrayType`, `DuckDBStructType`, `DuckDBMapType`, and
 * `DuckDBUnionType` do not carry their child types at the type level.
 *
 * This interface is exhaustive over `DuckDBTypeId`, and is the annotation
 * source for the converter map in `JsonDuckDBValueConverter`; adding a type id
 * without adding it here is a compile error.
 */
export interface JsonTypeForTypeId {
  [DuckDBTypeId.INVALID]: never;
  [DuckDBTypeId.BOOLEAN]: boolean;
  [DuckDBTypeId.TINYINT]: number;
  [DuckDBTypeId.SMALLINT]: number;
  [DuckDBTypeId.INTEGER]: number;
  [DuckDBTypeId.BIGINT]: string;
  [DuckDBTypeId.UTINYINT]: number;
  [DuckDBTypeId.USMALLINT]: number;
  [DuckDBTypeId.UINTEGER]: number;
  [DuckDBTypeId.UBIGINT]: string;
  [DuckDBTypeId.FLOAT]: number | string;
  [DuckDBTypeId.DOUBLE]: number | string;
  [DuckDBTypeId.TIMESTAMP]: string;
  [DuckDBTypeId.DATE]: string;
  [DuckDBTypeId.TIME]: string;
  [DuckDBTypeId.INTERVAL]: { months: number; days: number; micros: string };
  [DuckDBTypeId.HUGEINT]: string;
  [DuckDBTypeId.UHUGEINT]: string;
  [DuckDBTypeId.VARCHAR]: string;
  [DuckDBTypeId.BLOB]: string;
  [DuckDBTypeId.DECIMAL]: string;
  [DuckDBTypeId.TIMESTAMP_S]: string;
  [DuckDBTypeId.TIMESTAMP_MS]: string;
  [DuckDBTypeId.TIMESTAMP_NS]: string;
  [DuckDBTypeId.ENUM]: string;
  [DuckDBTypeId.LIST]: (Json | null)[];
  [DuckDBTypeId.STRUCT]: { [key: string]: Json | null };
  [DuckDBTypeId.MAP]: { key: Json | null; value: Json | null }[];
  [DuckDBTypeId.ARRAY]: (Json | null)[];
  [DuckDBTypeId.UUID]: string;
  [DuckDBTypeId.UNION]: { tag: string; value: Json | null };
  [DuckDBTypeId.BIT]: string;
  [DuckDBTypeId.TIME_TZ]: string;
  [DuckDBTypeId.TIMESTAMP_TZ]: string;
  [DuckDBTypeId.ANY]: never;
  [DuckDBTypeId.BIGNUM]: string;
  [DuckDBTypeId.SQLNULL]: null;
  [DuckDBTypeId.STRING_LITERAL]: never;
  [DuckDBTypeId.INTEGER_LITERAL]: never;
  [DuckDBTypeId.TIME_NS]: string;
  [DuckDBTypeId.GEOMETRY]: string;
  [DuckDBTypeId.VARIANT]: Json | null;
}

/**
 * The JSON-compatible type produced by `JsonDuckDBValueConverter` for a given
 * `DuckDBType`.
 *
 * Excludes NULL; use `JsonTypeFor<T> | null` for a nullable value.
 */
export type JsonTypeFor<T extends DuckDBType> = JsonTypeForTypeId[T['typeId']];
