import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import Explorer from '../Explorer';
import DJClientContext from '../../../providers/djclient';

function renderExplorer(props) {
  return render(
    <DJClientContext.Provider value={{ DataJunctionAPI: {} }}>
      <Explorer {...props} />
    </DJClientContext.Provider>,
  );
}

const leaf = { namespace: 'yshang', path: 'users.yshang', children: [] };

describe('Explorer row actions (kebab menu)', () => {
  it('opens a menu offering Pin and Add child for non-git-backed namespaces', () => {
    renderExplorer({
      item: leaf,
      current: 'users.yshang',
      gitRoots: new Set(),
      pinnedSet: new Set(),
      onTogglePin: vi.fn(),
    });
    fireEvent.click(screen.getByLabelText('Actions for users.yshang'));
    expect(screen.getByText('Pin namespace')).toBeInTheDocument();
    expect(screen.getByText('Add child namespace')).toBeInTheDocument();
  });

  it('omits Add child under a git root (git-managed)', () => {
    const item = { namespace: 'cubes', path: 'arc.main.cubes', children: [] };
    renderExplorer({
      item,
      current: 'arc.main.cubes',
      gitRoots: new Set(['arc']),
      pinnedSet: new Set(),
      onTogglePin: vi.fn(),
    });
    fireEvent.click(screen.getByLabelText('Actions for arc.main.cubes'));
    expect(screen.getByText('Pin namespace')).toBeInTheDocument();
    expect(screen.queryByText('Add child namespace')).not.toBeInTheDocument();
  });

  it('fires onTogglePin with the full path from the menu', () => {
    const onTogglePin = vi.fn();
    renderExplorer({
      item: leaf,
      current: 'users.yshang',
      gitRoots: new Set(),
      pinnedSet: new Set(),
      onTogglePin,
    });
    fireEvent.click(screen.getByLabelText('Actions for users.yshang'));
    fireEvent.click(screen.getByText('Pin namespace'));
    expect(onTogglePin).toHaveBeenCalledWith('users.yshang');
  });

  it('reflects pinned state in the menu', () => {
    renderExplorer({
      item: leaf,
      current: 'users.yshang',
      gitRoots: new Set(),
      pinnedSet: new Set(['users.yshang']),
      onTogglePin: vi.fn(),
    });
    fireEvent.click(screen.getByLabelText('Actions for users.yshang'));
    expect(screen.getByText('Unpin namespace')).toBeInTheDocument();
  });

  it('offers only Add child when onTogglePin is not provided', () => {
    renderExplorer({
      item: leaf,
      current: 'users.yshang',
      gitRoots: new Set(),
    });
    fireEvent.click(screen.getByLabelText('Actions for users.yshang'));
    expect(screen.getByText('Add child namespace')).toBeInTheDocument();
    expect(screen.queryByText('Pin namespace')).not.toBeInTheDocument();
  });
});
