import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValue } from './values';

export function getColumnsObjectFromChunks(
  chunks: readonly DuckDBDataChunk[],
  columnNames: readonly string[]
): Record<string, DuckDBValue[]> {
  const columnsObject: Record<string, DuckDBValue[]> = {};
  const columnCount = columnNames.length;
  let totalRowCount = 0;
  for (const chunk of chunks) {
    totalRowCount += chunk.rowCount;
  }
  // Pre-size one array per column (names are deduplicated upstream, so unique).
  const columns: DuckDBValue[][] = new Array(columnCount);
  for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
    const column: DuckDBValue[] = new Array(totalRowCount);
    columns[columnIndex] = column;
    columnsObject[columnNames[columnIndex]] = column;
  }
  let rowOffset = 0;
  for (const chunk of chunks) {
    if (chunk.columnCount !== columnCount) {
      throw new Error(
        `Provided number of column names (${columnCount}) does not match column count (${chunk.columnCount})`
      );
    }
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      chunk.getColumnVector(columnIndex).appendTo(columns[columnIndex], rowOffset);
    }
    rowOffset += chunk.rowCount;
  }
  return columnsObject;
}
