import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValue } from './values';

export function getRowObjectsFromChunks(
  chunks: readonly DuckDBDataChunk[],
  columnNames: readonly string[]
): Record<string, DuckDBValue>[] {
  const rowObjects: Record<string, DuckDBValue>[] = [];
  for (const chunk of chunks) {
    const rowCount = chunk.rowCount;
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      const rowObject: Record<string, DuckDBValue> = {};
      chunk.visitRowValues(rowIndex, (value, _, columnIndex) => {
        rowObject[columnNames[columnIndex]] = value;
      });
      rowObjects.push(rowObject);
    }
  }
  return rowObjects;
}
