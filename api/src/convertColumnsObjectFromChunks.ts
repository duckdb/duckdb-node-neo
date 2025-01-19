import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValueConverter } from './DuckDBValueConverter';

export function convertColumnsObjectFromChunks<T>(
  chunks: readonly DuckDBDataChunk[],
  columnNames: readonly string[],
  converter: DuckDBValueConverter<T>
): Record<string, T[]> {
  const convertedColumnsObject: Record<string, T[]> = {};
  for (const columnName of columnNames) {
    convertedColumnsObject[columnName] = [];
  }
  if (chunks.length === 0) {
    return convertedColumnsObject;
  }
  const columnCount = chunks[0].columnCount;
  for (let chunkIndex = 0; chunkIndex < chunks.length; chunkIndex++) {
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      chunks[chunkIndex].visitColumnValues(
        columnIndex,
        (value, _rowIndex, _columnIndex, type) =>
          convertedColumnsObject[columnNames[columnIndex]].push(
            converter.convertValue(value, type)
          )
      );
    }
  }
  return convertedColumnsObject;
}
