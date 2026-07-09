import * as React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import GitMenu from '../GitMenu';

describe('<GitMenu />', () => {
  it('renders the repo @ branch identity header', () => {
    render(
      <GitMenu
        repoPath="corp/ads-dj"
        branch="main"
        viewInGitUrl="https://git/x"
        onOpenSettings={() => {}}
      />,
    );
    expect(screen.getByText('corp/ads-dj')).toBeInTheDocument();
    expect(screen.getByText('main')).toBeInTheDocument();
  });

  it('renders only the items whose handlers/urls are provided', () => {
    render(
      <GitMenu repoPath="corp/ads-dj" branch="main" viewInGitUrl={null} />,
    );
    expect(screen.queryByText('Git settings')).not.toBeInTheDocument();
    expect(screen.queryByText('View in git')).not.toBeInTheDocument();
    expect(screen.queryByText('+ New Branch')).not.toBeInTheDocument();
    expect(screen.queryByText('Delete branch')).not.toBeInTheDocument();
  });

  it('renders Git settings only when onOpenSettings is provided', () => {
    const onOpenSettings = vi.fn();
    render(
      <GitMenu
        repoPath="corp/ads-dj"
        branch="main"
        viewInGitUrl={null}
        onOpenSettings={onOpenSettings}
      />,
    );
    fireEvent.click(screen.getByText('Git settings'));
    expect(onOpenSettings).toHaveBeenCalledTimes(1);
  });

  it('renders New Branch, View in git and Delete when provided and fires handlers', () => {
    const onNewBranch = vi.fn();
    const onDelete = vi.fn();
    const onOpenSettings = vi.fn();
    render(
      <GitMenu
        repoPath="corp/ads-dj"
        branch="feature"
        viewInGitUrl="https://git/x"
        onOpenSettings={onOpenSettings}
        onNewBranch={onNewBranch}
        onDelete={onDelete}
      />,
    );
    expect(screen.getByText('View in git')).toBeInTheDocument();
    fireEvent.click(screen.getByText('+ New Branch'));
    fireEvent.click(screen.getByText('Delete branch'));
    fireEvent.click(screen.getByText('Git settings'));
    expect(onNewBranch).toHaveBeenCalledTimes(1);
    expect(onDelete).toHaveBeenCalledTimes(1);
    expect(onOpenSettings).toHaveBeenCalledTimes(1);
  });
});
