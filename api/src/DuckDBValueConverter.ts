import { DuckDBType } from './DuckDBType';
import { DuckDBValue } from './values';

export type DuckDBValueConverter<T> = (
  value: DuckDBValue,
  type: DuckDBType,
  converter: DuckDBValueConverter<T>
) => T | null;

/**
 * A converter for a single DuckDB type id, whose output type is narrower than
 * the output type of the top-level converter it recurses through.
 *
 * `DuckDBValueConverter<T>` ties both the return type and the recursion hook
 * to the same `T`. In a per-type-id converter map, only the return type varies
 * by id: the `converter` argument is always the map's top-level converter, so
 * `Nested` stays fixed while `Out` narrows.
 */
export type DuckDBValueConverterFor<Out, Nested> = (
  value: DuckDBValue,
  type: DuckDBType,
  converter: DuckDBValueConverter<Nested>
) => Out | null;
