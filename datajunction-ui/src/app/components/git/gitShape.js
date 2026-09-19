// Classify a namespace's git config (from GET /namespaces/{ns}/git) into one of
// four shapes. Order matters: a branch (has a parent) is checked before repo
// ownership, since a branch inherits repo fields.
export function detectShape(config = {}) {
  if (config.parent_namespace) return 'branch';
  if (config.github_repo_path) {
    return config.git_branch ? 'flat' : 'root';
  }
  return 'not-git';
}

// Build the PATCH /namespaces/{ns}/git body from the modal form. Returns null
// to signal "clear git config" (the caller issues DELETE instead of PATCH).
export function buildGitConfigPayload({
  source,
  model,
  repository,
  branch,
  defaultBranch,
}) {
  if (source !== 'git') return null;
  if (model === 'flat') {
    return {
      github_repo_path: repository?.trim() || null,
      git_branch: branch?.trim() || null,
    };
  }
  return {
    github_repo_path: repository?.trim() || null,
    default_branch: defaultBranch?.trim() || null,
  };
}
