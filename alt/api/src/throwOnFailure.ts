import duckdb from '@duckdb/node-bindings';

export function throwOnFailure(state: duckdb.State, message: string, getError?: () => string, dispose?: () => void) {
  if (state !== duckdb.State.Success) {
    try {
      throw new Error(getError ? `${message}: ${getError()}` : message);
    } finally {
      if (dispose) {
        dispose();
      }
    }
  }
}
