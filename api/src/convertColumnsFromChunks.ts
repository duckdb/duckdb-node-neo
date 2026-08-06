import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValueConverter } from './DuckDBValueConverter';

export function convertColumnsFromChunks<T>(
  chunks: readonly DuckDBDataChunk[],
  converter: DuckDBValueConverter<T>
): (T | null)[][] {
  if (chunks.length === 0) {
    return [];
  }
  const columnCount = chunks[0].columnCount;
  let totalRowCount = 0;
  for (const chunk of chunks) {
    totalRowCount += chunk.rowCount;
  }
  const convertedColumns: (T | null)[][] = new Array(columnCount);
  for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
    convertedColumns[columnIndex] = new Array(totalRowCount);
  }
  let rowOffset = 0;
  for (const chunk of chunks) {
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      chunk
        .getColumnVector(columnIndex)
        .convertTo(converter, convertedColumns[columnIndex], rowOffset);
    }
    rowOffset += chunk.rowCount;
  }
  return convertedColumns;
}
