import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValue } from './values';

export function getColumnsFromChunks(chunks: readonly DuckDBDataChunk[]): DuckDBValue[][] {
  const columns: DuckDBValue[][] = [];
  if (chunks.length === 0) {
    return columns;
  }
  chunks[0].visitColumns(column => columns.push(column));
  for (let chunkIndex = 1; chunkIndex < chunks.length; chunkIndex++) {
    for (let columnIndex = 0; columnIndex < columns.length; columnIndex++) {
      chunks[chunkIndex].visitColumnValues(columnIndex, value => columns[columnIndex].push(value));
    }
  }
  return columns;
}
