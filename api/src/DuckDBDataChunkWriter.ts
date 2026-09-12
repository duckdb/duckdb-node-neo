import duckdb from '@duckdb/node-bindings';
import type { DuckDBAppender } from './DuckDBAppender';
import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBType } from './DuckDBType';
import { ToDuckDBValueConverter } from './ToDuckDBValueConverter';
import { DuckDBValue } from './values';

/** Receives each data chunk a `DuckDBDataChunkWriter` fills. */
export type DuckDBDataChunkSink = (chunk: DuckDBDataChunk) => void;

/**
 * `converter` is optional only when the rows are already `DuckDBValue`s, which
 * is what `T` defaults to. Supply one to write some other representation —
 * `JSToDuckDBValueConverter` for plain JS — and `T` follows from it.
 */
export type DuckDBDataChunkWriterOptions<T> = {
  /**
   * How many rows each emitted data chunk holds — its `rowCount`. Defaults to
   * the DuckDB vector size, which is also the most a data chunk can hold.
   */
  readonly rowsPerDataChunk?: number;
} & ([DuckDBValue] extends [T]
  ? { readonly converter?: ToDuckDBValueConverter<T> }
  : { readonly converter: ToDuckDBValueConverter<T> });

/**
 * Accumulates rows and emits them as filled data chunks, one per
 * `rowsPerDataChunk` rows, passing each to `sink`.
 *
 * Rows are `DuckDBValue`s by default. Supply a converter to write some other
 * representation; the row type follows from it.
 *
 *     const writer = DuckDBDataChunkWriter.forAppender(appender, {
 *       converter: JSToDuckDBValueConverter,
 *     });
 *     for (const row of rows) {
 *       writer.appendRow(row);
 *     }
 *     writer.flush();
 *
 * `flush` emits whatever is buffered, so call it when done: the last chunk is
 * usually a partial one. Rows already grouped into chunk-sized batches need no
 * writer — fill a `DuckDBDataChunk` and pass it to the destination directly.
 */
export class DuckDBDataChunkWriter<T = DuckDBValue> {
  private readonly types: readonly DuckDBType[];
  private readonly sink: DuckDBDataChunkSink;
  private readonly rowsPerDataChunk: number;
  private readonly converter?: ToDuckDBValueConverter<T>;
  private rows: (T | null)[][] = [];

  public constructor(
    types: readonly DuckDBType[],
    sink: DuckDBDataChunkSink,
    options: DuckDBDataChunkWriterOptions<T> = {} as DuckDBDataChunkWriterOptions<T>
  ) {
    this.types = types;
    this.sink = sink;
    const maxRowCount = duckdb.vector_size();
    this.rowsPerDataChunk = options.rowsPerDataChunk ?? maxRowCount;
    // Checked here rather than left to the first flush, which is where the
    // chunk itself would refuse it — a whole buffer's worth of rows later.
    if (this.rowsPerDataChunk < 1 || this.rowsPerDataChunk > maxRowCount) {
      throw new Error(
        `rowsPerDataChunk must be between 1 and ${maxRowCount}, got ${this.rowsPerDataChunk}`
      );
    }
    this.converter = options.converter;
  }

  /**
   * A writer that appends each filled data chunk to `appender`, using the
   * appender's own column types.
   *
   * Equivalent to constructing one with those types and a sink that calls
   * `appendDataChunk`, but with nothing to keep in step by hand.
   */
  public static forAppender<T = DuckDBValue>(
    appender: DuckDBAppender,
    options: DuckDBDataChunkWriterOptions<T> = {} as DuckDBDataChunkWriterOptions<T>
  ): DuckDBDataChunkWriter<T> {
    const types: DuckDBType[] = [];
    const columnCount = appender.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      types.push(appender.columnType(columnIndex));
    }
    return new DuckDBDataChunkWriter<T>(
      types,
      (chunk) => appender.appendDataChunk(chunk),
      options
    );
  }

  public get bufferedRowCount(): number {
    return this.rows.length;
  }

  /**
   * Buffers one row, emitting a data chunk once `rowsPerDataChunk` are held.
   *
   * The row is copied, so the same array can be reused across calls.
   */
  public appendRow(values: readonly (T | null)[]): void {
    if (values.length !== this.types.length) {
      throw new Error(
        `Provided number of values (${values.length}) does not match number of types (${this.types.length})`
      );
    }
    this.rows.push(values.slice());
    if (this.rows.length >= this.rowsPerDataChunk) {
      this.flush();
    }
  }

  /**
   * Emits the buffered rows as one data chunk. A no-op when none are
   * buffered, so it is safe to call more than once.
   *
   * The buffer is emptied either way: if converting the rows fails, or the
   * sink rejects the chunk, those rows are discarded along with the error.
   * Appending afterwards starts a fresh chunk.
   */
  public flush(): void {
    if (this.rows.length === 0) {
      return;
    }
    // Taken before anything that can throw. Left in place, a batch that failed
    // to convert or that the sink rejected would still be buffered, and later
    // appends would grow it past `rowsPerDataChunk` and eventually past what a
    // data chunk can hold at all — by which point the writer can no longer
    // flush and the original error is long gone.
    const rows = this.rows;
    this.rows = [];
    const chunk = DuckDBDataChunk.create(this.types);
    if (this.converter) {
      chunk.setRowsConverted(rows, this.converter);
    } else {
      // Without a converter the options type has already constrained `T` to
      // `DuckDBValue`, which is the only thing that makes this cast sound.
      chunk.setRows(rows as readonly (readonly DuckDBValue[])[]);
    }
    this.sink(chunk);
  }
}
