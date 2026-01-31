import React, { useState, useEffect } from 'react';

/**
 * Modal for configuring git settings for a namespace.
 */
export function GitSettingsModal({
  isOpen,
  onClose,
  onSave,
  onRemove,
  currentConfig,
  namespace,
}) {
  const [repoPath, setRepoPath] = useState('');
  const [branch, setBranch] = useState('');
  const [path, setPath] = useState('');
  const [gitOnly, setGitOnly] = useState(true);
  const [saving, setSaving] = useState(false);
  const [removing, setRemoving] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);
  const [wasRemoved, setWasRemoved] = useState(false);

  useEffect(() => {
    if (currentConfig) {
      setRepoPath(currentConfig.github_repo_path || '');
      setBranch(currentConfig.git_branch || '');
      setPath(currentConfig.git_path || 'nodes/');
      // If git is already configured (has repo path), use the existing git_only value
      // Otherwise, default to read-only (true) for new git configurations
      const hasExistingGitConfig = !!currentConfig.github_repo_path;
      setGitOnly(hasExistingGitConfig ? currentConfig.git_only : true);
    } else {
      // New configuration - default to read-only and nodes/ path
      setPath('nodes/');
      setGitOnly(true);
    }
    // Don't reset success here - it gets reset when modal closes
    // Otherwise the success banner disappears when currentConfig updates after save
    setError(null);
    setWasRemoved(false);
  }, [currentConfig]);

  const handleSubmit = async e => {
    e.preventDefault();
    setError(null);
    setSuccess(false);
    setWasRemoved(false);
    setSaving(true);

    try {
      const config = {
        github_repo_path: repoPath.trim() || null,
        git_branch: branch.trim() || null,
        git_path: path.trim() || null,
        git_only: gitOnly,
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

            <div className="form-group">
              <label htmlFor="git-repo-path">Repository</label>
              <input
                id="git-repo-path"
                type="text"
                placeholder="owner/repo"
                value={repoPath}
                onChange={e => setRepoPath(e.target.value)}
                disabled={saving}
              />
              <span className="form-hint">
                GitHub repository path (e.g., "myorg/dj-definitions")
              </span>
            </div>

            <div className="form-group">
              <label htmlFor="git-branch">Branch</label>
              <input
                id="git-branch"
                type="text"
                placeholder="main"
                value={branch}
                onChange={e => setBranch(e.target.value)}
                disabled={saving}
              />
              <span className="form-hint">
                Git branch for this namespace (e.g., "main" or "production")
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
                required
              />
              <span className="form-hint">
                Subdirectory within the repo for node YAML files
              </span>
            </div>

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
