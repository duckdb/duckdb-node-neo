import { DuckDBType } from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';
import { DuckDBValue } from './values';

/**
 * Converts a `T` into the `DuckDBValue` representation for a DuckDB type: the
 * inverse of `DuckDBValueConverter<T>`, which converts the other way.
 *
 * `T` is whatever representation a given converter accepts — see
 * `JSToDuckDBValueConverter` for the JS one. `DuckDBValue` is itself a
 * TypeScript representation, plain JS for the scalars and the value wrapper
 * classes, which hold nothing but data, for the rest. So a conversion of this
 * shape touches no C API, which is what lets it survive the move to the V2 C
 * API, where constructing a `duckdb_value` requires a connection or context.
 * Nothing here constructs one.
 */
export type ToDuckDBValueConverter<T> = (
  value: T,
  type: DuckDBType,
  converter: ToDuckDBValueConverter<T>
) => DuckDBValue;

/**
 * A converter for a single DuckDB type id, whose accepted input `In` is
 * narrower than the `Nested` the top-level converter it recurses through
 * accepts.
 *
 * The write-side counterpart of `DuckDBValueConverterFor`, and narrowing for
 * the same reason: in a per-type-id map only the accepted input varies by id,
 * while the `converter` argument is always the map's top-level converter.
 */
export type ToDuckDBValueConverterFor<In, Nested> = (
  value: In,
  type: DuckDBType,
  converter: ToDuckDBValueConverter<Nested>
) => DuckDBValue;

/**
 * Assembles a per-type-id converter map into a single converter.
 *
 * `InputTable` is the type table the map was annotated from — one entry per
 * type id — so each entry may accept a type narrower than `T`. That narrowing
 * is contravariant, so unlike the read direction it cannot be checked through
 * to the dispatch: the type id is only known at runtime, and the compiler
 * cannot relate `type.typeId` to the static type of `value`. The one unchecked
 * cast that closes the gap lives here rather than at each call site — callers
 * get a checked signature, and the mismatch surfaces as the converter's own
 * `Expected ...` error.
 */
export function createToDuckDBValueConverter<
  T,
  InputTable extends Record<DuckDBTypeId, unknown>,
>(convertersByTypeId: {
  [Id in DuckDBTypeId]: ToDuckDBValueConverterFor<InputTable[Id], T>;
}): ToDuckDBValueConverter<T> {
  return (value, type, converter) => {
    if (value == null) {
      return null;
    }
    const converterForTypeId = convertersByTypeId[type.typeId] as
      | ToDuckDBValueConverter<T>
      | undefined;
    if (!converterForTypeId) {
      throw new Error(`No converter for typeId: ${type.typeId}`);
    }
    return converterForTypeId(value, type, converter);
  };
}
