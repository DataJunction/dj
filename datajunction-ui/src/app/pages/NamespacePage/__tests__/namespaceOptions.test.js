import { describe, it, expect } from 'vitest';
import {
  buildNamespaceOptions,
  findHierarchyNode,
  filterNamespaceGroups,
} from '../namespaceOptions';

const sample = [
  { namespace: 'ads', git: { __typename: 'GitRootConfig' } },
  { namespace: 'member', git: null },
  { namespace: 'member.cds', git: { __typename: 'GitRootConfig' } },
  { namespace: 'member.scratch', git: null },
  { namespace: 'member.cds.branchx', git: { __typename: 'GitBranchConfig' } },
  { namespace: 'ae', git: null },
];

describe('buildNamespaceOptions', () => {
  it('groups git roots (any depth) and non-root top-levels, dedups, sorts', () => {
    const groups = buildNamespaceOptions(sample);
    expect(groups.map(g => g.label)).toEqual(['Git-backed', 'Top-level']);
    expect(groups[0].options.map(o => o.value)).toEqual(['ads', 'member.cds']);
    expect(groups[0].options.every(o => o.isGitRoot)).toBe(true);
    expect(groups[1].options.map(o => o.value)).toEqual(['ae', 'member']);
    expect(groups[1].options.every(o => o.isGitRoot)).toBe(false);
  });

  it('omits an empty group', () => {
    const groups = buildNamespaceOptions([{ namespace: 'ae', git: null }]);
    expect(groups.map(g => g.label)).toEqual(['Top-level']);
  });
});

const hierarchy = [
  {
    namespace: 'member',
    path: 'member',
    children: [
      { namespace: 'cds', path: 'member.cds', children: [] },
      { namespace: 'scratch', path: 'member.scratch', children: [] },
    ],
  },
  { namespace: 'ae', path: 'ae', children: [] },
];

describe('findHierarchyNode', () => {
  it('finds a top-level node by path', () => {
    expect(findHierarchyNode(hierarchy, 'ae')?.path).toBe('ae');
  });
  it('finds a nested node by full dotted path', () => {
    expect(findHierarchyNode(hierarchy, 'member.cds')?.path).toBe('member.cds');
  });
  it('returns null for unknown or empty path', () => {
    expect(findHierarchyNode(hierarchy, 'nope')).toBeNull();
    expect(findHierarchyNode(hierarchy, '')).toBeNull();
  });
});

describe('filterNamespaceGroups', () => {
  const groups = [
    {
      label: 'Git-backed',
      options: [
        { value: 'ads', isGitRoot: true },
        { value: 'member.cds', isGitRoot: true },
      ],
    },
    {
      label: 'Top-level namespaces',
      options: [
        { value: 'ae', isGitRoot: false },
        { value: 'member', isGitRoot: false },
      ],
    },
  ];
  it('returns groups unchanged for empty text', () => {
    expect(filterNamespaceGroups(groups, '')).toEqual(groups);
  });
  it('narrows options case-insensitively and drops empty groups', () => {
    const r = filterNamespaceGroups(groups, 'MEM');
    expect(r.map(g => g.label)).toEqual(['Git-backed', 'Top-level namespaces']);
    expect(r[0].options.map(o => o.value)).toEqual(['member.cds']);
    expect(r[1].options.map(o => o.value)).toEqual(['member']);
  });
  it('drops a group with no matches', () => {
    const r = filterNamespaceGroups(groups, 'ae');
    expect(r.map(g => g.label)).toEqual(['Top-level namespaces']);
    expect(r[0].options.map(o => o.value)).toEqual(['ae']);
  });
});
