import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValue } from './values';

export function getColumnsFromChunks(
  chunks: readonly DuckDBDataChunk[],
  columnCount: number
): DuckDBValue[][] {
  const columns: DuckDBValue[][] = Array.from({ length: columnCount }, () => []);
  for (const chunk of chunks) {
    chunk.appendToColumns(columns);
  }
  return columns;
}
