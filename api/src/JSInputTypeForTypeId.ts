import type { DuckDBType } from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';
import type { JS } from './JS';
import type { DuckDBVariantValue } from './values';

/**
 * The JS type accepted by `jsToDuckDBValue` for each DuckDB type id: the write
 * counterpart of `JSTypeForTypeId`.
 *
 * Everything `JSTypeForTypeId` produces, this table accepts — see the
 * round-trip assertion in `test/js-writes.test.ts` — but not the reverse.
 * Writes are deliberately wider where a second spelling is unambiguous: the
 * 64-bit and wider integers take `number` as well as `bigint`, and DECIMAL
 * takes either a double or a raw scaled `bigint`. VARIANT is the one
 * exception in the other direction; see below.
 *
 * NULL is not in the table: `createToDuckDBValueConverter` maps `null` to
 * `null` for every type, so a nullable value is `JSInputTypeForTypeId[Id] |
 * null`.
 *
 * Type ids that cannot be written map to `never`: `INVALID`, `ANY`,
 * `STRING_LITERAL` and `INTEGER_LITERAL`, none of which can appear in a result
 * either.
 *
 * VARIANT is the one place the write side is *narrower* than the read side. A
 * VARIANT holds its own type, so writing one means saying what that type is.
 * A `DuckDBVariantValue` carries it — either explicitly, as
 * `variantValue(structValue({k: 'n'}), STRUCT({k: VARCHAR}))`, or inferred by
 * `typeForValue` from a wrapped inner value — and a bare boolean, number,
 * bigint or string is unambiguous enough to infer on its own. A bare `Date`,
 * `Uint8Array`, array or object is not: each maps to several DuckDB types, so
 * `typeForValue` yields ANY and the write fails. Wrap those to say which.
 *
 * Reading a VARIANT discards that type, which is why the round-trip
 * containment the other ids satisfy does not hold here: the read table gives
 * `JS | null`, and JS alone is not enough to write back.
 *
 * This interface is exhaustive over `DuckDBTypeId`, and is the annotation
 * source for the converter map in `jsToDuckDBValue`; adding a type id without
 * adding it here is a compile error.
 */
export interface JSInputTypeForTypeId {
  [DuckDBTypeId.INVALID]: never;
  [DuckDBTypeId.BOOLEAN]: boolean;
  [DuckDBTypeId.TINYINT]: number;
  [DuckDBTypeId.SMALLINT]: number;
  [DuckDBTypeId.INTEGER]: number;
  [DuckDBTypeId.BIGINT]: bigint | number;
  [DuckDBTypeId.UTINYINT]: number;
  [DuckDBTypeId.USMALLINT]: number;
  [DuckDBTypeId.UINTEGER]: number;
  [DuckDBTypeId.UBIGINT]: bigint | number;
  [DuckDBTypeId.FLOAT]: number;
  [DuckDBTypeId.DOUBLE]: number;
  [DuckDBTypeId.TIMESTAMP]: Date;
  [DuckDBTypeId.DATE]: Date;
  [DuckDBTypeId.TIME]: bigint;
  [DuckDBTypeId.INTERVAL]: { months: number; days: number; micros: bigint };
  [DuckDBTypeId.HUGEINT]: bigint | number;
  [DuckDBTypeId.UHUGEINT]: bigint | number;
  [DuckDBTypeId.VARCHAR]: string;
  [DuckDBTypeId.BLOB]: Uint8Array;
  [DuckDBTypeId.DECIMAL]: number | bigint;
  [DuckDBTypeId.TIMESTAMP_S]: Date;
  [DuckDBTypeId.TIMESTAMP_MS]: Date;
  [DuckDBTypeId.TIMESTAMP_NS]: Date;
  [DuckDBTypeId.ENUM]: string;
  [DuckDBTypeId.LIST]: readonly (JS | null)[];
  [DuckDBTypeId.STRUCT]: Readonly<Record<string, JS | null>>;
  [DuckDBTypeId.MAP]: readonly { key: JS | null; value: JS | null }[];
  [DuckDBTypeId.ARRAY]: readonly (JS | null)[];
  [DuckDBTypeId.UUID]: string;
  [DuckDBTypeId.UNION]: { tag: string; value: JS | null };
  [DuckDBTypeId.BIT]: Uint8Array;
  [DuckDBTypeId.TIME_TZ]: { micros: bigint; offset: number };
  [DuckDBTypeId.TIMESTAMP_TZ]: Date;
  [DuckDBTypeId.ANY]: never;
  [DuckDBTypeId.BIGNUM]: bigint | number;
  [DuckDBTypeId.SQLNULL]: null;
  [DuckDBTypeId.STRING_LITERAL]: never;
  [DuckDBTypeId.INTEGER_LITERAL]: never;
  [DuckDBTypeId.TIME_NS]: bigint;
  [DuckDBTypeId.GEOMETRY]: Uint8Array;
  [DuckDBTypeId.VARIANT]:
    | DuckDBVariantValue
    | boolean
    | number
    | bigint
    | string;
}

/**
 * The JS type accepted by `jsToDuckDBValue` for a given `DuckDBType`.
 *
 * Excludes NULL; use `JSInputTypeFor<T> | null` for a nullable value.
 */
export type JSInputTypeFor<T extends DuckDBType> =
  JSInputTypeForTypeId[T['typeId']];
