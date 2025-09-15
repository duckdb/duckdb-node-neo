export function quotedString(input: string): string {
  return `'${input.replaceAll(`'`, `''`)}'`;
}

export function quotedIdentifier(input: string): string {
  return `"${input.replaceAll(`"`, `""`)}"`;
}
