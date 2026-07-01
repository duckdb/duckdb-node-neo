// Compiles a fixed-shape object-literal builder for STRUCT entries, the same technique
// the row-object builder uses: a static literal keeps V8 in fast-properties mode and
// avoids the dynamic-key `entries[name] = ...` stores of the old per-field loop.
//
// Builders are cached per unique set of entry names (struct shapes are stable), so the
// `new Function` compile happens once per shape rather than once per chunk/vector.
//
// Kept dependency-free (no vector imports) to avoid an import cycle with the vectors.

type ItemReader = { getItem(itemIndex: number): unknown };

export type StructValueEntriesBuilder = (
  entryVectors: readonly ItemReader[],
  itemIndex: number
) => Record<string, unknown>;

const builderCache = new Map<string, StructValueEntriesBuilder>();

function compile(entryNames: readonly string[]): StructValueEntriesBuilder {
  try {
    const body =
      'return {' +
      entryNames
        .map((name, i) => `${JSON.stringify(name)}: v[${i}].getItem(r)`)
        .join(',') +
      '};';
    // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
    return new Function('v', 'r', body) as StructValueEntriesBuilder;
  } catch {
    // CSP / no-eval environment: fall back to a dynamic-key build.
    return (entryVectors, itemIndex) => {
      const entries: Record<string, unknown> = {};
      for (let i = 0; i < entryNames.length; i++) {
        entries[entryNames[i]] = entryVectors[i].getItem(itemIndex);
      }
      return entries;
    };
  }
}

export function getStructValueEntriesBuilder(
  entryNames: readonly string[]
): StructValueEntriesBuilder {
  const key = JSON.stringify(entryNames);
  let builder = builderCache.get(key);
  if (!builder) {
    builder = compile(entryNames);
    builderCache.set(key, builder);
  }
  return builder;
}
