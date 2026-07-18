import { describe, it, expect } from 'vitest';
import { detectShape, buildGitConfigPayload } from '../gitShape';

describe('detectShape', () => {
  it('returns not-git when nothing is set', () => {
    expect(detectShape({})).toBe('not-git');
    expect(detectShape({ github_repo_path: null, git_branch: null })).toBe(
      'not-git',
    );
  });
  it('returns branch when parent_namespace is set', () => {
    expect(
      detectShape({
        parent_namespace: 'growth.metrics',
        git_branch: 'feature-x',
      }),
    ).toBe('branch');
  });
  it('returns flat when repo + git_branch, no parent', () => {
    expect(
      detectShape({ github_repo_path: 'corp/repo', git_branch: 'main' }),
    ).toBe('flat');
  });
  it('returns root when repo but no git_branch, no parent', () => {
    expect(
      detectShape({
        github_repo_path: 'corp/repo',
        git_branch: null,
        default_branch: 'main',
      }),
    ).toBe('root');
  });
});

describe('buildGitConfigPayload', () => {
  it('flat sends repo + git_branch only', () => {
    expect(
      buildGitConfigPayload({
        source: 'git',
        model: 'flat',
        repository: 'corp/repo',
        branch: 'main',
      }),
    ).toEqual({ github_repo_path: 'corp/repo', git_branch: 'main' });
  });
  it('root sends repo + default_branch only', () => {
    expect(
      buildGitConfigPayload({
        source: 'git',
        model: 'root',
        repository: 'corp/repo',
        defaultBranch: 'main',
      }),
    ).toEqual({ github_repo_path: 'corp/repo', default_branch: 'main' });
  });
  it('dj source clears git config', () => {
    expect(buildGitConfigPayload({ source: 'dj' })).toBeNull();
  });
});
