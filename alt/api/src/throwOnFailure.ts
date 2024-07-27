import duckdb from 'duckdb';

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
