import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValue } from './values';

export function getColumnsFromChunks(
  chunks: readonly DuckDBDataChunk[]
): DuckDBValue[][] {
  if (chunks.length === 0) {
    return [];
  }
  const columnCount = chunks[0].columnCount;
  let totalRowCount = 0;
  for (const chunk of chunks) {
    totalRowCount += chunk.rowCount;
  }
  const columns: DuckDBValue[][] = new Array(columnCount);
  for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
    columns[columnIndex] = new Array(totalRowCount);
  }
  let rowOffset = 0;
  for (const chunk of chunks) {
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      chunk.getColumnVector(columnIndex).appendTo(columns[columnIndex], rowOffset);
    }
    rowOffset += chunk.rowCount;
  }
  return columns;
}
