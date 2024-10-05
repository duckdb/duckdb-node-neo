export interface ExpectedArrayVector {
  kind: 'array';
  itemCount: number;
  validity: boolean[];
  child: ExpectedVector;
}

export interface ExpectedDataVector {
  kind: 'data';
  validity: boolean[];
  itemBytes: number;
  values: any[];
}

export type ExpectedListEntry = [bigint, bigint] | null;

export interface ExpectedListVector {
  kind: 'list';
  validity: boolean[];
  entries: (ExpectedListEntry | null)[];
  childItemCount: number;
  child: ExpectedVector;
}

export interface ExpectedMapVector {
  kind: 'map';
  validity: boolean[];
  entries: (ExpectedListEntry | null)[];
  keys: ExpectedVector;
  values: ExpectedVector;
}

export interface ExpectedStructVector {
  kind: 'struct';
  itemCount: number;
  validity: boolean[];
  children: ExpectedVector[];
}

export interface ExpectedUnionVector {
  kind: 'union';
  children: ExpectedVector[];
}

export type ExpectedVector =
  | ExpectedArrayVector
  | ExpectedDataVector
  | ExpectedListVector
  | ExpectedMapVector
  | ExpectedStructVector
  | ExpectedUnionVector
  ;
