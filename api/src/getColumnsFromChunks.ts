import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValue } from './values';

export function getColumnsFromChunks(
  chunks: readonly DuckDBDataChunk[],
): DuckDBValue[][] {
  const columns: DuckDBValue[][] = [];
  for (const chunk of chunks) {
    chunk.appendToColumns(columns);
  }
  return columns;
}
