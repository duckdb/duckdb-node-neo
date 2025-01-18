import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValue } from './values';

export function getRowsFromChunks(
  chunks: readonly DuckDBDataChunk[]
): DuckDBValue[][] {
  const rows: DuckDBValue[][] = [];
  for (const chunk of chunks) {
    chunk.visitRows((row) => rows.push(row));
  }
  return rows;
}
