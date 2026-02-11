import React, { useState, useEffect } from 'react';

/**
 * Modal for configuring git settings for a namespace.
 * Supports two modes:
 * - Git Root: Configure repository and path only (no branch, no git_only flag)
 * - Branch Namespace: Auto-calculates parent from namespace name, user sets branch and git_only
 *   (e.g., "demo.main" automatically has parent "demo")
 *   git_only determines if the branch is read-only (deployed from git) or editable (UI changes allowed)
 */
export function GitSettingsModal({
  isOpen,
  onClose,
  onSave,
  onRemove,
  currentConfig,
  namespace,
}) {
  const [mode, setMode] = useState('root'); // 'root' or 'branch'
  const [repoPath, setRepoPath] = useState('');
  const [branch, setBranch] = useState('');
  const [path, setPath] = useState('');
  const [defaultBranch, setDefaultBranch] = useState('main');
  const [gitOnly, setGitOnly] = useState(true);
  const [saving, setSaving] = useState(false);
  const [removing, setRemoving] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);
  const [wasRemoved, setWasRemoved] = useState(false);
  const [parentConfig, setParentConfig] = useState(null);

  // Auto-calculate parent namespace from current namespace
  // e.g., "demo.main" -> "demo", "demo.metrics.feature1" -> "demo.metrics"
  const parentNamespace = namespace?.includes('.')
    ? namespace.substring(0, namespace.lastIndexOf('.'))
    : '';

  useEffect(() => {
    if (currentConfig) {
      // Determine mode based on whether parent_namespace is set
      const isBranchMode = !!currentConfig.parent_namespace;
      setMode(isBranchMode ? 'branch' : 'root');

      setRepoPath(currentConfig.github_repo_path || '');
      setBranch(currentConfig.git_branch || '');
      setPath(currentConfig.git_path || 'nodes/');
      setDefaultBranch(currentConfig.default_branch || 'main');

      // git_only is only relevant for branch namespaces
      // Default to true (read-only) for new branch configs, use existing value if set
      if (isBranchMode) {
        setGitOnly(
          currentConfig.git_only !== undefined ? currentConfig.git_only : true,
        );
      }
    } else {
      // New configuration - default to git root mode and nodes/ path
      setMode('root');
      setPath('nodes/');
      // Default git_only to true for when user switches to branch mode
      setGitOnly(true);
    }
    // Don't reset success here - it gets reset when modal closes
    // Otherwise the success banner disappears when currentConfig updates after save
    setError(null);
    setWasRemoved(false);
    setParentConfig(null);
  }, [currentConfig]);

  const handleSubmit = async e => {
    e.preventDefault();
    setError(null);
    setSuccess(false);
    setWasRemoved(false);

    // Client-side validation
    if (mode === 'branch') {
      if (!parentNamespace) {
        setError(
          'Cannot configure as branch namespace: namespace has no parent (no dots in name)',
        );
        return;
      }
      if (!branch.trim()) {
        setError('Git branch is required for branch mode');
        return;
      }
    } else {
      // Git root mode
      if (!repoPath.trim()) {
        setError('Repository is required');
        return;
      }
    }

    setSaving(true);

    try {
      const config =
        mode === 'branch'
          ? {
              // Branch mode: only send branch, parent, and git_only
              git_branch: branch.trim() || null,
              parent_namespace: parentNamespace || null,
              git_only: gitOnly,
            }
          : {
              // Git root mode: only send repo, path, and default_branch (no git_branch, no git_only)
              github_repo_path: repoPath.trim() || null,
              git_path: path.trim() || null,
              default_branch: defaultBranch.trim() || null,
            };

      const result = await onSave(config);
      if (result?._error) {
        setError(result.message);
      } else {
        setSuccess(true);
        // Keep modal open so user can see success message
        // User can close manually via Close button or X
      }
    } catch (err) {
      setError(err.message || 'Failed to save git settings');
    } finally {
      setSaving(false);
    }
  };

  // Fetch parent config when parent namespace changes (only if modal is open)
  useEffect(() => {
    if (isOpen && mode === 'branch' && parentNamespace) {
      // Fetch parent's git config to show inherited values
      fetch(`/api/namespaces/${parentNamespace}/git`)
        .then(res => (res.ok ? res.json() : null))
        .then(data => setParentConfig(data))
        .catch(() => setParentConfig(null));
    } else {
      setParentConfig(null);
    }
  }, [isOpen, mode, parentNamespace]);

  const handleRemove = async () => {
    if (
      !window.confirm(
        'Remove git configuration? This will disconnect this namespace from git but will not delete any files.',
      )
    ) {
      return;
    }

    setError(null);
    setSuccess(false);
    setWasRemoved(false);
    setRemoving(true);

    try {
      const config = {
        github_repo_path: null,
        git_branch: null,
        git_path: null,
        git_only: false,
      };

      const result = await onRemove(config);
      if (result?._error) {
        setError(result.message);
      } else {
        setSuccess(true);
        setWasRemoved(true);
        // Close modal after successful removal
        setTimeout(() => {
          onClose();
        }, 1500);
      }
    } catch (err) {
      setError(err.message || 'Failed to remove git settings');
    } finally {
      setRemoving(false);
    }
  };

  const handleClose = () => {
    setError(null);
    setSuccess(false);
    setWasRemoved(false);
    onClose();
  };

  if (!isOpen) return null;

  return (
    <div className="modal-overlay" onClick={handleClose}>
      <div className="modal-content" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <h3>Git Configuration</h3>
          <button
            className="btn-close-modal"
            onClick={handleClose}
            title="Close"
          >
            ×
          </button>
        </div>

        <form onSubmit={handleSubmit}>
          <div className="modal-body">
            <p
              style={{
                color: '#64748b',
                fontSize: '13px',
                marginBottom: '16px',
              }}
            >
              Configure git integration for <strong>{namespace}</strong> to
              enable branch management and sync changes to GitHub.
            </p>

            {error && (
              <div
                style={{
                  padding: '12px',
                  backgroundColor: '#fef2f2',
                  border: '1px solid #fecaca',
                  borderRadius: '6px',
                  color: '#dc2626',
                  fontSize: '13px',
                  marginBottom: '16px',
                }}
              >
                {error}
              </div>
            )}

            {/* Mode Toggle */}
            <div style={{ marginBottom: '20px' }}>
              <div
                style={{
                  display: 'flex',
                  gap: '12px',
                  padding: '4px',
                  backgroundColor: '#f1f5f9',
                  borderRadius: '8px',
                }}
              >
                <button
                  type="button"
                  onClick={() => setMode('root')}
                  disabled={saving}
                  style={{
                    flex: 1,
                    padding: '10px 16px',
                    fontSize: '13px',
                    fontWeight: 500,
                    border: 'none',
                    borderRadius: '6px',
                    backgroundColor:
                      mode === 'root' ? '#ffffff' : 'transparent',
                    color: mode === 'root' ? '#0f172a' : '#64748b',
                    cursor: saving ? 'not-allowed' : 'pointer',
                    boxShadow:
                      mode === 'root' ? '0 1px 3px rgba(0,0,0,0.1)' : 'none',
                    transition: 'all 0.15s',
                  }}
                >
                  Git Root
                </button>
                <button
                  type="button"
                  onClick={() => setMode('branch')}
                  disabled={saving}
                  style={{
                    flex: 1,
                    padding: '10px 16px',
                    fontSize: '13px',
                    fontWeight: 500,
                    border: 'none',
                    borderRadius: '6px',
                    backgroundColor:
                      mode === 'branch' ? '#ffffff' : 'transparent',
                    color: mode === 'branch' ? '#0f172a' : '#64748b',
                    cursor: saving ? 'not-allowed' : 'pointer',
                    boxShadow:
                      mode === 'branch' ? '0 1px 3px rgba(0,0,0,0.1)' : 'none',
                    transition: 'all 0.15s',
                  }}
                >
                  Branch Namespace
                </button>
              </div>
              <span
                style={{
                  display: 'block',
                  marginTop: '8px',
                  fontSize: '12px',
                  color: '#64748b',
                }}
              >
                {mode === 'root'
                  ? 'Configure repository for this namespace (recommended for most users)'
                  : `Link to parent "${
                      parentNamespace || '(none)'
                    }" and inherit repository configuration`}
              </span>
            </div>

            {/* Conditional Fields based on Mode */}
            {mode === 'root' ? (
              <>
                {/* Git Root Mode - Repository and Path only */}
                <div className="form-group">
                  <label htmlFor="git-repo-path">Repository *</label>
                  <input
                    id="git-repo-path"
                    type="text"
                    placeholder="owner/repo"
                    value={repoPath}
                    onChange={e => setRepoPath(e.target.value)}
                    disabled={saving}
                    required
                  />
                  <span className="form-hint">
                    GitHub repository path (e.g., "myorg/dj-definitions")
                  </span>
                </div>

                <div className="form-group">
                  <label htmlFor="git-path">Path</label>
                  <input
                    id="git-path"
                    type="text"
                    placeholder="nodes/"
                    value={path}
                    onChange={e => setPath(e.target.value)}
                    disabled={saving}
                  />
                  <span className="form-hint">
                    Subdirectory within the repo for node YAML files
                  </span>
                </div>

                <div className="form-group">
                  <label htmlFor="default-branch">Default Branch</label>
                  <input
                    id="default-branch"
                    type="text"
                    placeholder="main"
                    value={defaultBranch}
                    onChange={e => setDefaultBranch(e.target.value)}
                    disabled={saving}
                  />
                  <span className="form-hint">
                    Default branch to use when creating new branches (e.g.,
                    "main")
                  </span>
                </div>
              </>
            ) : (
              <>
                {/* Branch Mode - Show parent and branch field */}
                {!parentNamespace ? (
                  <div
                    style={{
                      marginBottom: '16px',
                      padding: '12px',
                      backgroundColor: '#fef2f2',
                      borderRadius: '6px',
                      border: '1px solid #fecaca',
                      color: '#dc2626',
                      fontSize: '13px',
                    }}
                  >
                    Cannot configure as branch namespace: namespace "{namespace}
                    " has no parent. Branch namespaces must have a dot in their
                    name (e.g., "demo.main").
                  </div>
                ) : (
                  <>
                    {/* Display parent namespace as read-only info */}
                    <div
                      style={{
                        marginBottom: '16px',
                        padding: '12px',
                        backgroundColor: '#f8fafc',
                        borderRadius: '6px',
                        border: '1px solid #e2e8f0',
                      }}
                    >
                      <div
                        style={{
                          fontSize: '12px',
                          fontWeight: 600,
                          color: '#475569',
                          marginBottom: '8px',
                        }}
                      >
                        Parent Namespace
                      </div>
                      <div
                        style={{
                          fontSize: '14px',
                          fontWeight: 500,
                          color: '#0f172a',
                          fontFamily: 'monospace',
                        }}
                      >
                        {parentNamespace}
                      </div>
                      <div
                        style={{
                          fontSize: '12px',
                          color: '#64748b',
                          marginTop: '6px',
                        }}
                      >
                        Repository configuration will be inherited from this
                        parent
                      </div>
                    </div>

                    {/* Show inherited config from parent */}
                    {parentConfig && (
                      <div
                        style={{
                          marginBottom: '16px',
                          padding: '12px',
                          backgroundColor: '#f0f9ff',
                          borderRadius: '6px',
                          border: '1px solid #bae6fd',
                        }}
                      >
                        <div
                          style={{
                            fontSize: '12px',
                            fontWeight: 600,
                            color: '#0369a1',
                            marginBottom: '8px',
                          }}
                        >
                          Inherited Configuration
                        </div>
                        <div style={{ fontSize: '12px', color: '#64748b' }}>
                          <div style={{ marginBottom: '4px' }}>
                            <strong>Repository:</strong>{' '}
                            {parentConfig.github_repo_path ||
                              '(not configured)'}
                          </div>
                          <div>
                            <strong>Path:</strong>{' '}
                            {parentConfig.git_path || '(root)'}
                          </div>
                        </div>
                      </div>
                    )}
                  </>
                )}

                <div className="form-group">
                  <label htmlFor="git-branch">Branch *</label>
                  <input
                    id="git-branch"
                    type="text"
                    placeholder="feature-x, dev, etc."
                    value={branch}
                    onChange={e => setBranch(e.target.value)}
                    disabled={saving}
                    required
                  />
                  <span className="form-hint">
                    Git branch name for this namespace
                  </span>
                </div>

                {/* Git-only checkbox - only for branch namespaces */}
                <div
                  style={{
                    marginTop: '16px',
                    padding: '12px',
                    backgroundColor: gitOnly ? '#fef3c7' : '#f0fdf4',
                    borderRadius: '6px',
                    border: `1px solid ${gitOnly ? '#fcd34d' : '#86efac'}`,
                  }}
                >
                  <label
                    style={{
                      display: 'flex',
                      alignItems: 'flex-start',
                      gap: '10px',
                      cursor: 'pointer',
                      margin: 0,
                      textTransform: 'none',
                      letterSpacing: 'normal',
                      fontSize: '14px',
                      fontWeight: 'normal',
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={gitOnly}
                      onChange={e => setGitOnly(e.target.checked)}
                      disabled={saving}
                      style={{ marginTop: '3px' }}
                    />
                    <span>
                      <span
                        style={{
                          fontWeight: 600,
                          color: gitOnly ? '#92400e' : '#166534',
                          textTransform: 'none',
                        }}
                      >
                        {gitOnly
                          ? 'Read-only (Git is source of truth)'
                          : 'Editable (UI edits allowed)'}
                      </span>
                      <span
                        style={{
                          display: 'block',
                          marginTop: '4px',
                          fontSize: '12px',
                          color: '#64748b',
                          fontWeight: 'normal',
                          textTransform: 'none',
                        }}
                      >
                        {gitOnly
                          ? 'Changes must be made via git and deployed through CI/CD. UI editing is disabled.'
                          : 'Users can edit nodes in the UI. Changes can be synced to git.'}
                      </span>
                    </span>
                  </label>
                </div>
              </>
            )}

            {success && (
              <div
                style={{
                  marginTop: '16px',
                  padding: '12px',
                  backgroundColor: '#f0fdf4',
                  border: '1px solid #86efac',
                  borderRadius: '6px',
                  color: '#166534',
                  fontSize: '13px',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px',
                }}
              >
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                >
                  <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" />
                  <polyline points="22 4 12 14.01 9 11.01" />
                </svg>
                {wasRemoved
                  ? 'Git configuration removed successfully!'
                  : 'Git configuration saved successfully!'}
              </div>
            )}
          </div>

          <div
            className="modal-actions"
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
            }}
          >
            {/* Left side: Remove button (only show if git is configured) */}
            <div>
              {currentConfig?.github_repo_path && !success && (
                <button
                  type="button"
                  onClick={handleRemove}
                  disabled={saving || removing}
                  style={{
                    padding: '8px 16px',
                    fontSize: '13px',
                    fontWeight: 500,
                    border: '1px solid #fca5a5',
                    borderRadius: '6px',
                    backgroundColor: removing ? '#fee2e2' : '#ffffff',
                    color: removing ? '#991b1b' : '#dc2626',
                    cursor: saving || removing ? 'not-allowed' : 'pointer',
                    opacity: saving || removing ? 0.6 : 1,
                  }}
                >
                  {removing ? 'Removing...' : 'Reset'}
                </button>
              )}
            </div>

            {/* Right side: Save/Cancel buttons */}
            <div style={{ display: 'flex', gap: '8px' }}>
              <button
                type="button"
                className="btn-secondary"
                onClick={handleClose}
                disabled={saving || removing}
              >
                {success ? 'Close' : 'Cancel'}
              </button>
              {!success && (
                <button
                  type="submit"
                  className="btn-primary"
                  disabled={saving || removing}
                  style={
                    saving
                      ? {
                          opacity: 0.7,
                          cursor: 'wait',
                          backgroundColor: '#9ca3af',
                        }
                      : {}
                  }
                >
                  {saving ? 'Saving...' : 'Save Settings'}
                </button>
              )}
            </div>
          </div>
        </form>
      </div>
    </div>
  );
}

export default GitSettingsModal;
