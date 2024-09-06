import {
  ExpectedArrayVector,
  ExpectedDataVector,
  ExpectedListEntry,
  ExpectedListVector,
  ExpectedMapVector,
  ExpectedStructVector,
  ExpectedUnionVector,
  ExpectedVector,
} from './ExpectedVector';

export function array(
  itemCount: number,
  validity: boolean[],
  child: ExpectedVector
): ExpectedArrayVector {
  return {
    kind: 'array',
    itemCount,
    validity,
    child,
  };
}

export function data(
  itemBytes: number,
  validity: boolean[],
  values: any[]
): ExpectedDataVector {
  return {
    kind: 'data',
    itemBytes,
    validity,
    values,
  };
}

export function list(
  validity: boolean[],
  entries: (ExpectedListEntry | null)[],
  childItemCount: number,
  child: ExpectedVector
): ExpectedListVector {
  return {
    kind: 'list',
    validity,
    entries,
    childItemCount,
    child,
  };
}

export function map(
  validity: boolean[],
  entries: (ExpectedListEntry | null)[],
  keys: ExpectedVector,
  values: ExpectedVector
): ExpectedMapVector {
  return {
    kind: 'map',
    validity,
    entries,
    keys,
    values,
  };
}

export function struct(
  itemCount: number,
  validity: boolean[],
  children: ExpectedVector[]
): ExpectedStructVector {
  return {
    kind: 'struct',
    itemCount,
    validity,
    children,
  };
}

export function union(children: ExpectedVector[]): ExpectedUnionVector {
  return {
    kind: 'union',
    children,
  };
}
