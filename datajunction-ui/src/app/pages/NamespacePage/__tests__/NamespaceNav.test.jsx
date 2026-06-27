import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import NamespaceNav from '../NamespaceNav';

const namespaces = [
  { namespace: 'ads', numNodes: 42, git: { __typename: 'GitRootConfig' } },
  { namespace: 'member', numNodes: 7, git: null },
  {
    namespace: 'member.cds',
    numNodes: 156,
    git: { __typename: 'GitRootConfig' },
  },
];

function renderNav(props = {}) {
  return render(
    <NamespaceNav
      namespaces={namespaces}
      hierarchy={[]}
      currentNamespace={undefined}
      gitRoots={new Set(['ads', 'member.cds'])}
      onSelect={vi.fn()}
      {...props}
    />,
  );
}

beforeEach(() => localStorage.clear());

describe('NamespaceNav', () => {
  it('by default shows no Pinned section; Git-backed open, Top-level collapsed', () => {
    renderNav();
    expect(screen.queryByText('Pinned')).not.toBeInTheDocument();
    expect(screen.getByText('ads')).toBeInTheDocument();
    expect(screen.getByText('member.cds')).toBeInTheDocument();
    expect(screen.getByText('Top-level')).toBeInTheDocument();
    expect(screen.queryByText('member')).not.toBeInTheDocument();
  });

  it('expands Top-level when its heading is clicked', () => {
    renderNav();
    fireEvent.click(screen.getByText('Top-level'));
    expect(screen.getByText('member')).toBeInTheDocument();
  });

  it('selecting a row calls onSelect', () => {
    const onSelect = vi.fn();
    renderNav({ onSelect });
    fireEvent.click(screen.getByText('ads'));
    expect(onSelect).toHaveBeenCalledWith('ads');
  });

  it('pinning a row surfaces a Pinned section', () => {
    renderNav();
    fireEvent.click(screen.getByLabelText('Pin ads'));
    expect(screen.getByText('Pinned')).toBeInTheDocument();
    expect(screen.getAllByLabelText('Unpin ads').length).toBeGreaterThan(0);
  });

  it('filtering hides the Pinned section and narrows the list', () => {
    renderNav();
    fireEvent.click(screen.getByLabelText('Pin ads'));
    fireEvent.change(screen.getByPlaceholderText('Filter namespaces…'), {
      target: { value: 'cds' },
    });
    expect(screen.queryByText('Pinned')).not.toBeInTheDocument();
    expect(screen.getByText('member.cds')).toBeInTheDocument();
    expect(screen.queryByText('member')).not.toBeInTheDocument();
  });

  it('filtering surfaces deep nested namespaces (not just roots/top-levels)', () => {
    renderNav({
      namespaces: [
        { namespace: 'default', numNodes: 5, git: null },
        {
          namespace: 'default.fruits.citrus.lemons',
          numNodes: 1,
          git: null,
        },
      ],
    });
    // Unfiltered, the nested namespace is not in the curated list...
    expect(
      screen.queryByText('default.fruits.citrus.lemons'),
    ).not.toBeInTheDocument();
    // ...but filtering finds it.
    fireEvent.change(screen.getByPlaceholderText('Filter namespaces…'), {
      target: { value: 'lemons' },
    });
    expect(
      screen.getByText('default.fruits.citrus.lemons'),
    ).toBeInTheDocument();
  });

  it('rail folder nav shows the current namespace child folders, not its siblings', () => {
    const hierarchy = [
      { namespace: 'default', path: 'default', children: [] },
      {
        namespace: 'growth',
        path: 'growth',
        children: [
          { namespace: 'metrics', path: 'growth.metrics', children: [] },
          {
            namespace: 'experiments',
            path: 'growth.experiments',
            children: [],
          },
        ],
      },
      { namespace: 'marketing', path: 'marketing', children: [] },
    ];
    const folderNamespaces = [
      { namespace: 'default', git: null },
      { namespace: 'growth', git: null },
      { namespace: 'growth.metrics', git: null },
      { namespace: 'growth.experiments', git: null },
      { namespace: 'marketing', git: null },
    ];
    const onSelect = vi.fn();
    render(
      <NamespaceNav
        namespaces={folderNamespaces}
        hierarchy={hierarchy}
        currentNamespace="growth"
        gitRoots={new Set()}
        onSelect={onSelect}
      />,
    );
    // The rail lists the current namespace's child folders for drilling in.
    expect(screen.getByText('Folders')).toBeInTheDocument();
    expect(screen.getByText('metrics')).toBeInTheDocument();
    expect(screen.getByText('experiments')).toBeInTheDocument();
    // Siblings of the current namespace are NOT shown (no all-namespaces explosion).
    expect(screen.queryByText('default')).not.toBeInTheDocument();
    expect(screen.queryByText('marketing')).not.toBeInTheDocument();
    // Clicking a child folder drills into it.
    fireEvent.click(screen.getByText('metrics'));
    expect(onSelect).toHaveBeenCalledWith('growth.metrics');
  });
});
