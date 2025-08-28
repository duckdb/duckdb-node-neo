export type JS =
  | null
  | boolean
  | number
  | bigint
  | string
  | Uint8Array
  | Date
  | JS[]
  | { [key: string]: JS };
