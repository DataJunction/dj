import { describe, it, expect } from 'vitest';
import {
  buildNamespaceOptions,
  findHierarchyNode,
  immediateChildren,
  buildJumpTree,
  searchNamespaces,
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

describe('searchNamespaces', () => {
  it('matches nested namespaces that the curated list excludes', () => {
    // member.scratch is a deep non-git namespace — absent from buildNamespaceOptions,
    // but search must find it.
    expect(searchNamespaces(sample, 'scratch')).toEqual(['member.scratch']);
  });

  it('matches across depth, sorted by full path', () => {
    expect(searchNamespaces(sample, 'member')).toEqual([
      'member',
      'member.cds',
      'member.cds.branchx',
      'member.scratch',
    ]);
  });

  it('returns nothing for empty/whitespace filter', () => {
    expect(searchNamespaces(sample, '')).toEqual([]);
    expect(searchNamespaces(sample, '   ')).toEqual([]);
  });
});

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

const jumpHierarchy = [
  { namespace: 'default', path: 'default', children: [] },
  { namespace: 'finance', path: 'finance', children: [] },
  {
    namespace: 'growth',
    path: 'growth',
    children: [
      { namespace: 'experiments', path: 'growth.experiments', children: [] },
      {
        namespace: 'metrics',
        path: 'growth.metrics',
        children: [
          { namespace: 'dau', path: 'growth.metrics.dau', children: [] },
        ],
      },
    ],
  },
  { namespace: 'marketing', path: 'marketing', children: [] },
];

describe('immediateChildren', () => {
  it('returns direct children only', () => {
    expect(immediateChildren(jumpHierarchy, 'growth').map(c => c.path)).toEqual(
      ['growth.experiments', 'growth.metrics'],
    );
  });
  it('returns [] for a leaf or missing path', () => {
    expect(immediateChildren(jumpHierarchy, 'growth.experiments')).toEqual([]);
    expect(immediateChildren(jumpHierarchy, 'nope')).toEqual([]);
  });
});

describe('buildJumpTree', () => {
  it('shows siblings at each level, current collapsed, no current children', () => {
    const rows = buildJumpTree(jumpHierarchy, 'growth');
    expect(rows.map(r => r.path)).toEqual([
      'default',
      'finance',
      'growth',
      'marketing',
    ]);
    const growth = rows.find(r => r.path === 'growth');
    expect(growth.isCurrent).toBe(true);
    expect(growth.depth).toBe(0);
  });
  it('descends along the current path, excluding current children', () => {
    const rows = buildJumpTree(jumpHierarchy, 'growth.metrics');
    expect(rows.map(r => r.path)).toEqual([
      'default',
      'finance',
      'growth',
      'marketing', // level 0 siblings
      'growth.experiments',
      'growth.metrics', // level 1 siblings under ancestor growth
    ]);
    expect(rows.find(r => r.path === 'growth').isAncestor).toBe(true);
    expect(rows.find(r => r.path === 'growth.metrics').isCurrent).toBe(true);
    // growth.metrics.dau (a child of current) is NOT present
    expect(rows.find(r => r.path === 'growth.metrics.dau')).toBeUndefined();
  });
  it('returns [] when no currentPath', () => {
    expect(buildJumpTree(jumpHierarchy, '')).toEqual([]);
  });
});
