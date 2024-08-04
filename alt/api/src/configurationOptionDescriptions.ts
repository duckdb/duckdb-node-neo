import duckdb from '@jraymakers/duckdb-node-bindings';

export function configurationOptionDescriptions(): Readonly<
  Record<string, string>
> {
  const descriptions: Record<string, string> = {};
  const count = duckdb.config_count();
  for (let i = 0; i < count; i++) {
    const { name, description } = duckdb.get_config_flag(i);
    descriptions[name] = description;
  }
  return descriptions;
}
