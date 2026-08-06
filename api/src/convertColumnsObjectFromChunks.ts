import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValueConverter } from './DuckDBValueConverter';

export function convertColumnsObjectFromChunks<T>(
  chunks: readonly DuckDBDataChunk[],
  columnNames: readonly string[],
  converter: DuckDBValueConverter<T>
): Record<string, (T | null)[]> {
  const convertedColumnsObject: Record<string, (T | null)[]> = {};
  const columnCount = columnNames.length;
  let totalRowCount = 0;
  for (const chunk of chunks) {
    totalRowCount += chunk.rowCount;
  }
  const convertedColumns: (T | null)[][] = new Array(columnCount);
  for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
    const column: (T | null)[] = new Array(totalRowCount);
    convertedColumns[columnIndex] = column;
    convertedColumnsObject[columnNames[columnIndex]] = column;
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
  return convertedColumnsObject;
}
