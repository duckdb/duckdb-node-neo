import type { DuckDBType } from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';
import type { JS } from './JS';

/**
 * The JS type produced by `JSDuckDBValueConverter` for each DuckDB type id.
 *
 * Values are non-NULL results only: SQL NULL is handled by
 * `createDuckDBValueConverter`, which yields `null` for any type. Callers
 * representing a nullable value should use `JSTypeForTypeId[Id] | null`.
 *
 * Type ids that cannot appear in a result (`INVALID`, `ANY`,
 * `STRING_LITERAL`, `INTEGER_LITERAL`) map to `never`, since their
 * converters throw.
 *
 * Nested types bottom out at `JS`, because `DuckDBListType`,
 * `DuckDBArrayType`, `DuckDBStructType`, `DuckDBMapType`, and
 * `DuckDBUnionType` do not carry their child types at the type level.
 *
 * This interface is exhaustive over `DuckDBTypeId`, and is the annotation
 * source for the converter map in `JSDuckDBValueConverter`; adding a type id
 * without adding it here is a compile error.
 */
export interface JSTypeForTypeId {
  [DuckDBTypeId.INVALID]: never;
  [DuckDBTypeId.BOOLEAN]: boolean;
  [DuckDBTypeId.TINYINT]: number;
  [DuckDBTypeId.SMALLINT]: number;
  [DuckDBTypeId.INTEGER]: number;
  [DuckDBTypeId.BIGINT]: bigint;
  [DuckDBTypeId.UTINYINT]: number;
  [DuckDBTypeId.USMALLINT]: number;
  [DuckDBTypeId.UINTEGER]: number;
  [DuckDBTypeId.UBIGINT]: bigint;
  [DuckDBTypeId.FLOAT]: number;
  [DuckDBTypeId.DOUBLE]: number;
  [DuckDBTypeId.TIMESTAMP]: Date;
  [DuckDBTypeId.DATE]: Date;
  [DuckDBTypeId.TIME]: bigint;
  [DuckDBTypeId.INTERVAL]: { months: number; days: number; micros: bigint };
  [DuckDBTypeId.HUGEINT]: bigint;
  [DuckDBTypeId.UHUGEINT]: bigint;
  [DuckDBTypeId.VARCHAR]: string;
  [DuckDBTypeId.BLOB]: Uint8Array;
  [DuckDBTypeId.DECIMAL]: number;
  [DuckDBTypeId.TIMESTAMP_S]: Date;
  [DuckDBTypeId.TIMESTAMP_MS]: Date;
  [DuckDBTypeId.TIMESTAMP_NS]: Date;
  [DuckDBTypeId.ENUM]: string;
  [DuckDBTypeId.LIST]: (JS | null)[];
  [DuckDBTypeId.STRUCT]: { [key: string]: JS | null };
  [DuckDBTypeId.MAP]: { key: JS | null; value: JS | null }[];
  [DuckDBTypeId.ARRAY]: (JS | null)[];
  [DuckDBTypeId.UUID]: string;
  [DuckDBTypeId.UNION]: { tag: string; value: JS | null };
  [DuckDBTypeId.BIT]: Uint8Array;
  [DuckDBTypeId.TIME_TZ]: { micros: bigint; offset: number };
  [DuckDBTypeId.TIMESTAMP_TZ]: Date;
  [DuckDBTypeId.ANY]: never;
  [DuckDBTypeId.BIGNUM]: bigint;
  [DuckDBTypeId.SQLNULL]: null;
  [DuckDBTypeId.STRING_LITERAL]: never;
  [DuckDBTypeId.INTEGER_LITERAL]: never;
  [DuckDBTypeId.TIME_NS]: bigint;
  [DuckDBTypeId.GEOMETRY]: Uint8Array;
  [DuckDBTypeId.VARIANT]: JS | null;
}

/**
 * The JS type produced by `JSDuckDBValueConverter` for a given `DuckDBType`.
 *
 * Excludes NULL; use `JSTypeFor<T> | null` for a nullable value.
 */
export type JSTypeFor<T extends DuckDBType> = JSTypeForTypeId[T['typeId']];
