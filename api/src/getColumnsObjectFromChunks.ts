import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValue } from './values';

export function getColumnsObjectFromChunks(
  chunks: readonly DuckDBDataChunk[],
  columnNames: readonly string[],
): Record<string, DuckDBValue[]> {
  const columnsObject: Record<string, DuckDBValue[]> = {};
  for (const chunk of chunks) {
    chunk.appendToColumnsObject(columnNames, columnsObject);
  }
  return columnsObject;
}
