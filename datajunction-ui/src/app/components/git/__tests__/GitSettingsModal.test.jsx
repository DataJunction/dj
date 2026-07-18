import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { GitSettingsModal } from '../GitSettingsModal';

const open = props =>
  render(
    <GitSettingsModal
      isOpen
      namespace="magnesium.tech"
      onClose={vi.fn()}
      onSave={vi.fn().mockResolvedValue({})}
      onRemove={vi.fn().mockResolvedValue({})}
      currentConfig={null}
      {...props}
    />,
  );

it('has no read-only / git_only checkbox', () => {
  open();
  expect(
    screen.queryByText(/editable \(ui edits allowed\)/i),
  ).not.toBeInTheDocument();
  expect(screen.queryByText(/git is source of truth/i)).not.toBeInTheDocument();
});

it('shows repo + branch fields for "tracks a branch" and saves a flat payload', async () => {
  const onSave = vi.fn().mockResolvedValue({});
  open({ onSave });
  fireEvent.click(screen.getByText(/git repo/i));
  fireEvent.click(screen.getByText(/tracks a branch/i));
  fireEvent.change(screen.getByLabelText(/repository/i), {
    target: { value: 'corp/tech-realtime-guardrails' },
  });
  fireEvent.change(screen.getByLabelText(/^branch$/i), {
    target: { value: 'main' },
  });
  fireEvent.click(screen.getByRole('button', { name: /save/i }));
  await screen.findByText(/success|saved/i).catch(() => {});
  expect(onSave).toHaveBeenCalledWith({
    github_repo_path: 'corp/tech-realtime-guardrails',
    git_branch: 'main',
  });
});

it('preselects flat mode from an existing flat config', () => {
  open({
    currentConfig: { github_repo_path: 'corp/repo', git_branch: 'main' },
  });
  expect(screen.getByDisplayValue('corp/repo')).toBeInTheDocument();
  expect(screen.getByDisplayValue('main')).toBeInTheDocument();
});

it('warns before connecting a namespace that already has nodes', () => {
  render(
    <GitSettingsModal
      isOpen
      namespace="some.sandbox"
      nodeCount={4}
      onClose={vi.fn()}
      onSave={vi.fn().mockResolvedValue({})}
      onRemove={vi.fn()}
      currentConfig={null}
    />,
  );
  fireEvent.click(screen.getByText(/git repo/i));
  expect(
    screen.getByText(/4 nodes here aren't in the repo/i),
  ).toBeInTheDocument();
  expect(screen.getByText(/replaced by the repo/i)).toBeInTheDocument();
});

it('selecting "Edited in DataJunction" saves via onRemove, not onSave', async () => {
  const onSave = vi.fn().mockResolvedValue({});
  const onRemove = vi.fn().mockResolvedValue({});
  open({
    onSave,
    onRemove,
    currentConfig: { github_repo_path: 'corp/repo', git_branch: 'main' },
  });
  fireEvent.click(screen.getByText(/edited in datajunction/i));
  fireEvent.click(screen.getByRole('button', { name: /save/i }));
  await screen.findByText(/success|saved/i).catch(() => {});
  expect(onRemove).toHaveBeenCalled();
  expect(onSave).not.toHaveBeenCalled();
});
