import type duckdb from '@duckdb/node-bindings';
import type { DuckDBType } from '../DuckDBType';
import type { DuckDBValue } from '../values';
import type { DuckDBVector } from './DuckDBVector';

export type CreateDuckDBVectorFn = (
  vector: duckdb.Vector,
  itemCount: number,
  knownType?: DuckDBType
) => DuckDBVector<DuckDBValue>;

/**
 * @internal
 * Holds the concrete {@link DuckDBVector.create} implementation, assigned by
 * ./createVector when the vectors module graph loads.
 *
 * This indirection keeps the base class free of static imports of its
 * subclasses. A direct import would form a module-initialization cycle (each
 * subclass statically imports the base to `extend` it), which throws at load
 * time ("Cannot access 'DuckDBVector' before initialization") regardless of
 * when `create()` is actually called. This module is intentionally not
 * re-exported from the package barrel.
 */
export const vectorRegistry: { create?: CreateDuckDBVectorFn } = {};
