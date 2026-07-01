import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import FolderTree from '../FolderTree';

// Small hierarchy fixture:
//   g.metrics  (has one child: g.metrics.daily)
//   g.cubes    (no children)
const folders = [
  {
    namespace: 'metrics',
    path: 'g.metrics',
    children: [{ namespace: 'daily', path: 'g.metrics.daily', children: [] }],
  },
  { namespace: 'cubes', path: 'g.cubes', children: [] },
];

describe('FolderTree', () => {
  it('renders the "Folders" heading and top-level folder names', () => {
    render(<FolderTree folders={folders} onSelect={vi.fn()} />);
    expect(screen.getByText('Folders')).toBeInTheDocument();
    expect(screen.getByText('metrics')).toBeInTheDocument();
    expect(screen.getByText('cubes')).toBeInTheDocument();
  });

  it('shows an Expand button for a folder with children; clicking it reveals the nested child, clicking again (Collapse) hides it', () => {
    render(<FolderTree folders={folders} onSelect={vi.fn()} />);

    // The nested child should not be visible initially (collapsed by default).
    expect(screen.queryByText('daily')).not.toBeInTheDocument();

    // Expand button is present for 'metrics' (it has children).
    const expandBtn = screen.getByLabelText('Expand');
    expect(expandBtn).toBeInTheDocument();

    // Click to expand — nested child appears.
    fireEvent.click(expandBtn);
    expect(screen.getByText('daily')).toBeInTheDocument();

    // The button label flips to 'Collapse'.
    const collapseBtn = screen.getByLabelText('Collapse');
    expect(collapseBtn).toBeInTheDocument();

    // Click to collapse — nested child disappears.
    fireEvent.click(collapseBtn);
    expect(screen.queryByText('daily')).not.toBeInTheDocument();
  });

  it('clicking a folder name calls onSelect with that node path', () => {
    const onSelect = vi.fn();
    render(<FolderTree folders={folders} onSelect={onSelect} />);

    fireEvent.click(screen.getByText('cubes'));
    expect(onSelect).toHaveBeenCalledWith('g.cubes');

    fireEvent.click(screen.getByText('metrics'));
    expect(onSelect).toHaveBeenCalledWith('g.metrics');
  });

  it('renders nothing for empty folders', () => {
    const { container } = render(
      <FolderTree folders={[]} onSelect={vi.fn()} />,
    );
    expect(container.firstChild).toBeNull();
  });
});
