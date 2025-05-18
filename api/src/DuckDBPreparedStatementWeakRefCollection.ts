import { DuckDBPreparedStatement } from './DuckDBPreparedStatement';
import { DuckDBPreparedStatementCollection } from './DuckDBPreparedStatementCollection';

export class DuckDBPreparedStatementWeakRefCollection
  implements DuckDBPreparedStatementCollection
{
  preparedStatements: WeakRef<DuckDBPreparedStatement>[] = [];
  public add(prepared: DuckDBPreparedStatement) {
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
}
