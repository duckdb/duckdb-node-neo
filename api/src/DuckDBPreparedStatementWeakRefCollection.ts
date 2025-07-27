import { DuckDBPreparedStatement } from './DuckDBPreparedStatement';
import { DuckDBPreparedStatementCollection } from './DuckDBPreparedStatementCollection';

export class DuckDBPreparedStatementWeakRefCollection
  implements DuckDBPreparedStatementCollection
{
  preparedStatements: WeakRef<DuckDBPreparedStatement>[] = [];
  lastPruneTime: number = 0;
  public add(prepared: DuckDBPreparedStatement) {
    const now = performance.now();
    if (now - this.lastPruneTime > 1000) {
      this.lastPruneTime = now;
      this.prune();
    }
    this.preparedStatements.push(new WeakRef(prepared));
  }
  public destroySync() {
    for (const preparedRef of this.preparedStatements) {
      const prepared = preparedRef.deref();
      if (prepared) {
        prepared.destroySync();
      }
    }
    this.preparedStatements = [];
  }
  private prune() {
    this.preparedStatements = this.preparedStatements.filter(
      (ref) => !!ref.deref()
    );
  }
}
