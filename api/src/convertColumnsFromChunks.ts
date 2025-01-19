import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValueConverter } from './DuckDBValueConverter';

export function convertColumnsFromChunks<T>(
  chunks: readonly DuckDBDataChunk[],
  converter: DuckDBValueConverter<T>
): T[][] {
  if (chunks.length === 0) {
    return [];
  }
  const convertedColumns = chunks[0].convertColumns(converter);
  for (let chunkIndex = 1; chunkIndex < chunks.length; chunkIndex++) {
    for (
      let columnIndex = 0;
      columnIndex < convertedColumns.length;
      columnIndex++
    ) {
      const chunk = chunks[chunkIndex];
      chunk.visitColumnValues(
        columnIndex,
        (value, _rowIndex, _columnIndex, type) =>
          convertedColumns[columnIndex].push(
            converter.convertValue(value, type)
          )
      );
    }
  }
  return convertedColumns;
}
