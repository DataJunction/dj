import React, { useState, useEffect } from 'react';
import { detectShape, buildGitConfigPayload } from './gitShape';
import SplitFilter from '../SplitFilter';

/**
 * Modal for configuring git settings for a namespace.
 * Intent-based: user picks "Edited in DataJunction" or "Git repo",
 * then (if git) picks "Tracks a branch" (flat) or "Feature branches + PRs" (root).
 * No git_only checkbox — git-backed namespaces are always read-only here.
 */
export function GitSettingsModal({
  isOpen,
  onClose,
  onSave,
  onRemove,
  currentConfig,
  namespace,
  nodeCount = 0,
}) {
  const [source, setSource] = useState('dj'); // 'dj' | 'git'
  const [model, setModel] = useState('flat'); // 'flat' | 'root'
  const [repository, setRepository] = useState('');
  const [branch, setBranch] = useState('main');
  const [defaultBranch, setDefaultBranch] = useState('main');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);

  useEffect(() => {
    if (!isOpen) return;
    const shape = detectShape(currentConfig || {});
    setSource(shape === 'not-git' ? 'dj' : 'git');
    setModel(shape === 'root' ? 'root' : 'flat');
    setRepository(currentConfig?.github_repo_path || '');
    setBranch(currentConfig?.git_branch || 'main');
    setDefaultBranch(currentConfig?.default_branch || 'main');
    setError(null);
  }, [currentConfig, isOpen]);

  const handleSubmit = async e => {
    e.preventDefault();
    setError(null);
    setSuccess(false);
    if (source === 'git' && !repository.trim()) {
      setError('Repository is required');
      return;
    }
    setSaving(true);
    try {
      const payload = buildGitConfigPayload({
        source,
        model,
        repository,
        branch,
        defaultBranch,
      });
      const result =
        payload === null ? await onRemove() : await onSave(payload);
      if (result?._error) setError(result.message);
      else setSuccess(true);
    } catch (err) {
      setError(err.message || 'Failed to save git settings');
    } finally {
      setSaving(false);
    }
  };

  const handleClose = () => {
    setError(null);
    setSuccess(false);
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
          <div
            className="modal-body"
            style={{
              fontFamily:
                'system-ui, -apple-system, "Segoe UI", Roboto, sans-serif',
              textTransform: 'none',
            }}
          >
            <p
              style={{
                color: '#64748b',
                fontSize: '13px',
                marginBottom: '16px',
              }}
            >
              Configure git integration for <strong>{namespace}</strong>.
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

            {/* Q1: Source of definitions */}
            <div
              style={{
                display: 'flex',
                textTransform: 'none',
                marginBottom: '16px',
              }}
            >
              <SplitFilter
                label="Source of definitions"
                value={source}
                onChange={v => v && setSource(v)}
                options={[
                  { value: 'dj', label: 'Edited in DataJunction' },
                  { value: 'git', label: 'Git repo' },
                ]}
              />
            </div>

            {source === 'git' && (
              <>
                {detectShape(currentConfig || {}) === 'not-git' &&
                  nodeCount > 0 && (
                    <div className="git-repo-wins-warning" role="alert">
                      ⚠️ {nodeCount} nodes here aren't in the repo and will be
                      replaced by the repo's contents on the next deploy. Git
                      becomes the source of truth for this namespace.
                    </div>
                  )}

                {/* Q2: Git model */}
                <div
                  style={{
                    display: 'flex',
                    textTransform: 'none',
                    marginBottom: '16px',
                  }}
                >
                  <SplitFilter
                    label="Git model"
                    value={model}
                    onChange={v => v && setModel(v)}
                    options={[
                      { value: 'flat', label: 'Tracks a branch' },
                      { value: 'root', label: 'Feature branches + PRs' },
                    ]}
                  />
                </div>

                <div className="form-group">
                  <label htmlFor="git-repo-path">Repository</label>
                  <input
                    id="git-repo-path"
                    type="text"
                    placeholder="owner/repo"
                    value={repository}
                    onChange={e => setRepository(e.target.value)}
                    disabled={saving}
                  />
                </div>

                {model === 'flat' ? (
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
                  </div>
                ) : (
                  <div className="form-group">
                    <label htmlFor="git-default-branch">Default branch</label>
                    <input
                      id="git-default-branch"
                      type="text"
                      placeholder="main"
                      value={defaultBranch}
                      onChange={e => setDefaultBranch(e.target.value)}
                      disabled={saving}
                    />
                  </div>
                )}

                <p className="git-readonly-note">
                  🔒 Git-backed namespaces are read-only here; edit via git +
                  CI.
                </p>
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
                Git configuration saved successfully!
              </div>
            )}
          </div>

          <div
            className="modal-actions"
            style={{ display: 'flex', justifyContent: 'flex-end', gap: '8px' }}
          >
            <button
              type="button"
              className="btn-secondary"
              onClick={handleClose}
              disabled={saving}
            >
              {success ? 'Close' : 'Cancel'}
            </button>
            {!success && (
              <button
                type="submit"
                className="btn-primary"
                disabled={saving}
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
                {saving ? 'Saving...' : 'Save'}
              </button>
            )}
          </div>
        </form>
      </div>
    </div>
  );
}

export default GitSettingsModal;
