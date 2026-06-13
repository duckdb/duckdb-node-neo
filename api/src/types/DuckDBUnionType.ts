import { BaseDuckDBType } from './BaseDuckDBType';
import type { DuckDBType } from './DuckDBType';
import { DuckDBLogicalType } from '../DuckDBLogicalType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { Json } from '../Json';
import { quotedIdentifier } from '../sql';

export class DuckDBUnionType extends BaseDuckDBType<DuckDBTypeId.UNION> {
  public readonly memberTags: readonly string[];
  public readonly tagMemberIndexes: Readonly<Record<string, number>>;
  public readonly memberTypes: readonly DuckDBType[];
  public constructor(
    memberTags: readonly string[],
    memberTypes: readonly DuckDBType[],
    alias?: string
  ) {
    super(DuckDBTypeId.UNION, alias);
    if (memberTags.length !== memberTypes.length) {
      throw new Error(`Could not create DuckDBUnionType: \
        tags length (${memberTags.length}) does not match valueTypes length (${memberTypes.length})`);
    }
    this.memberTags = memberTags;
    const tagMemberIndexes: Record<string, number> = {};
    for (let i = 0; i < memberTags.length; i++) {
      tagMemberIndexes[memberTags[i]] = i;
    }
    this.tagMemberIndexes = tagMemberIndexes;
    this.memberTypes = memberTypes;
  }
  public memberIndexForTag(tag: string): number {
    return this.tagMemberIndexes[tag];
  }
  public memberTypeForTag(tag: string): DuckDBType {
    return this.memberTypes[this.tagMemberIndexes[tag]];
  }
  public get memberCount() {
    return this.memberTags.length;
  }
  public toString(): string {
    const parts: string[] = [];
    for (let i = 0; i < this.memberTags.length; i++) {
      parts.push(
        `${quotedIdentifier(this.memberTags[i])} ${this.memberTypes[i]}`
      );
    }
    return `UNION(${parts.join(', ')})`;
  }
  public override toLogicalType(): DuckDBLogicalType {
    const logicalType = DuckDBLogicalType.createUnion(
      this.memberTags,
      this.memberTypes.map((t) => t.toLogicalType())
    );
    if (this.alias) {
      logicalType.alias = this.alias;
    }
    return logicalType;
  }
  public override toJson(): Json {
    return {
      typeId: this.typeId,
      memberTags: [...this.memberTags],
      memberTypes: this.memberTypes.map(t => t.toJson()),
      ...(this.alias ? { alias: this.alias } : {}),
    };
  }
}
export function UNION(
  members: Record<string, DuckDBType>,
  alias?: string
): DuckDBUnionType {
  const memberTags = Object.keys(members);
  const memberTypes = Object.values(members);
  return new DuckDBUnionType(memberTags, memberTypes, alias);
}
