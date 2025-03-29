import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValueConverter } from './DuckDBValueConverter';

export function convertRowObjectsFromChunks<T>(
  chunks: readonly DuckDBDataChunk[],
  columnNames: readonly string[],
  converter: DuckDBValueConverter<T>
): Record<string, T | null>[] {
  const rowObjects: Record<string, T | null>[] = [];
  for (const chunk of chunks) {
    const rowCount = chunk.rowCount;
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      const rowObject: Record<string, T | null> = {};
      chunk.visitRowValues(rowIndex, (value, _rowIndex, columnIndex, type) => {
        rowObject[columnNames[columnIndex]] = converter(value, type, converter);
      });
      rowObjects.push(rowObject);
    }
  }
  return rowObjects;
}
