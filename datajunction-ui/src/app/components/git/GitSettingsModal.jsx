import React, { useState, useEffect } from 'react';

/**
 * Modal for configuring git settings for a namespace.
 */
export function GitSettingsModal({
  isOpen,
  onClose,
  onSave,
  currentConfig,
  namespace,
}) {
  const [repoPath, setRepoPath] = useState('');
  const [branch, setBranch] = useState('');
  const [path, setPath] = useState('');
  const [gitOnly, setGitOnly] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);

  useEffect(() => {
    if (currentConfig) {
      setRepoPath(currentConfig.github_repo_path || '');
      setBranch(currentConfig.git_branch || '');
      setPath(currentConfig.git_path || '');
      // If git is already configured (has repo path), use the existing git_only value
      // Otherwise, default to read-only (true) for new git configurations
      const hasExistingGitConfig = !!currentConfig.github_repo_path;
      setGitOnly(hasExistingGitConfig ? currentConfig.git_only : true);
    } else {
      // New configuration - default to read-only
      setGitOnly(true);
    }
    setSuccess(false);
    setError(null);
  }, [currentConfig]);

  const handleSubmit = async e => {
    e.preventDefault();
    setError(null);
    setSuccess(false);
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
        // Keep modal open briefly to show success, then close
        setTimeout(() => {
          onClose();
          setSuccess(false);
        }, 1500);
      }
    } catch (err) {
      setError(err.message || 'Failed to save git settings');
    } finally {
      setSaving(false);
    }
  };

  const handleClose = () => {
    setError(null);
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
              <label htmlFor="git-path">Path (optional)</label>
              <input
                id="git-path"
                type="text"
                placeholder="definitions/"
                value={path}
                onChange={e => setPath(e.target.value)}
                disabled={saving}
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
                Git configuration saved successfully!
              </div>
            )}
          </div>

          <div className="modal-actions">
            <button
              type="button"
              className="btn-secondary"
              onClick={handleClose}
              disabled={saving}
            >
              Cancel
            </button>
            <button type="submit" className="btn-primary" disabled={saving}>
              {saving ? 'Saving...' : 'Save Settings'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

export default GitSettingsModal;
