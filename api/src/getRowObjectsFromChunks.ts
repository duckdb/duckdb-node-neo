import { compileRowObjectBuilderSafe } from './compileRowObjectBuilder';
import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBValue } from './values';

export function getRowObjectsFromChunks(
  chunks: readonly DuckDBDataChunk[],
  columnNames: readonly string[]
): Record<string, DuckDBValue>[] {
  const rowObjects: Record<string, DuckDBValue>[] = [];
  if (chunks.length === 0) {
    return rowObjects;
  }
  // Column types are stable across chunks, so compile the builder once from the first
  // chunk's flat-read flags, then reuse it across all chunks.
  const builder = compileRowObjectBuilderSafe(
    columnNames,
    chunks[0].getFlatFlags()
  );
  for (const chunk of chunks) {
    chunk.appendToRowObjectsWithBuilder(builder, rowObjects);
  }
  return rowObjects;
}
