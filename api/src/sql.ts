export function quotedString(input: string): string {
  return `'${input.replace(`'`, `''`)}'`;
}

export function quotedIdentifier(input: string): string {
  return `"${input.replace(`"`, `""`)}"`;
}
