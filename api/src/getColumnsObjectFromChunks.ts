import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValue } from './values';

export function getColumnsObjectFromChunks(
  chunks: readonly DuckDBDataChunk[],
  columnNames: readonly string[],
): Record<string, DuckDBValue[]> {
  const columnsObject: Record<string, DuckDBValue[]> = {};
  for (const columnName of columnNames) {
    columnsObject[columnName] = [];
  }
  if (chunks.length === 0) {
    return columnsObject;
  }
  const columnCount = chunks[0].columnCount;
  for (let chunkIndex = 0; chunkIndex < chunks.length; chunkIndex++) {
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      chunks[chunkIndex].visitColumnValues(columnIndex, (value) =>
        columnsObject[columnNames[columnIndex]].push(value)
      );
    }
  }
  return columnsObject;
}
