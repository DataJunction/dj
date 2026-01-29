import React, { useState } from 'react';

/**
 * Modal for creating a pull request from a branch namespace.
 */
export function CreatePRModal({
  isOpen,
  onClose,
  onCreate,
  namespace,
  gitBranch,
  parentBranch,
  repoPath,
}) {
  const [title, setTitle] = useState('');
  const [body, setBody] = useState('');
  const [progress, setProgress] = useState(null); // null | 'syncing' | 'creating'
  const [error, setError] = useState(null);
  const [result, setResult] = useState(null);

  const handleSubmit = async e => {
    e.preventDefault();
    if (!title.trim()) return;

    setError(null);
    setProgress('syncing');

    try {
      const res = await onCreate(
        title.trim(),
        body.trim() || null,
        setProgress,
      );
      if (res?._error) {
        setError(res.message);
      } else {
        setResult(res);
      }
    } catch (err) {
      setError(err.message || 'Failed to create pull request');
    } finally {
      setProgress(null);
    }
  };

  const getProgressText = () => {
    if (progress === 'syncing') return 'Syncing to git...';
    if (progress === 'creating') return 'Creating PR...';
    return 'Create PR';
  };

  const handleClose = () => {
    setTitle('');
    setBody('');
    setError(null);
    setResult(null);
    setProgress(null);
    onClose();
  };

  const isWorking = progress !== null;

  if (!isOpen) return null;

  return (
    <div className="modal-overlay" onClick={handleClose}>
      <div
        className="modal-content"
        onClick={e => e.stopPropagation()}
        style={{ maxWidth: '560px' }}
      >
        <div className="modal-header">
          <h3>Create Pull Request</h3>
          <button
            className="btn-close-modal"
            onClick={handleClose}
            title="Close"
          >
            ×
          </button>
        </div>

        {result ? (
          <div className="modal-body">
            <div
              style={{
                textAlign: 'center',
                padding: '16px 0',
              }}
            >
              <span
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  width: '48px',
                  height: '48px',
                  borderRadius: '50%',
                  backgroundColor: '#dcfce7',
                  color: '#16a34a',
                  fontSize: '24px',
                  marginBottom: '12px',
                }}
              >
                ✓
              </span>
              <h4 style={{ margin: '0 0 8px 0' }}>
                Pull Request #{result.pr_number} Created!
              </h4>
            </div>

            <div
              style={{
                backgroundColor: '#f8fafc',
                borderRadius: '6px',
                padding: '12px',
                fontSize: '13px',
              }}
            >
              <div style={{ marginBottom: '8px' }}>
                <strong>Title:</strong> {title}
              </div>
              <div style={{ marginBottom: '8px' }}>
                <strong>Branch:</strong> {result.head_branch} →{' '}
                {result.base_branch}
              </div>
              <div>
                <strong>URL:</strong>{' '}
                <a
                  href={result.pr_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{ color: '#3b82f6' }}
                >
                  {result.pr_url}
                </a>
              </div>
            </div>

            <div className="modal-actions">
              <button className="btn-secondary" onClick={handleClose}>
                Close
              </button>
              <a
                href={result.pr_url}
                target="_blank"
                rel="noopener noreferrer"
                className="btn-primary"
                style={{ textDecoration: 'none' }}
              >
                View on GitHub
              </a>
            </div>
          </div>
        ) : (
          <form onSubmit={handleSubmit}>
            <div className="modal-body">
              {/* Branch flow indicator */}
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  gap: '12px',
                  padding: '12px',
                  backgroundColor: '#f8fafc',
                  borderRadius: '6px',
                  marginBottom: '16px',
                  fontSize: '13px',
                }}
              >
                <span
                  style={{
                    padding: '4px 8px',
                    backgroundColor: '#dbeafe',
                    borderRadius: '4px',
                    fontFamily: 'monospace',
                  }}
                >
                  {gitBranch}
                </span>
                <span style={{ color: '#64748b' }}>→</span>
                <span
                  style={{
                    padding: '4px 8px',
                    backgroundColor: '#dcfce7',
                    borderRadius: '4px',
                    fontFamily: 'monospace',
                  }}
                >
                  {parentBranch}
                </span>
              </div>

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
                <label htmlFor="pr-title">Title *</label>
                <input
                  id="pr-title"
                  type="text"
                  placeholder="Add new metrics for revenue tracking"
                  value={title}
                  onChange={e => setTitle(e.target.value)}
                  disabled={isWorking}
                  autoFocus
                />
              </div>

              <div className="form-group">
                <label htmlFor="pr-body">Description</label>
                <textarea
                  id="pr-body"
                  placeholder="Describe the changes in this pull request..."
                  value={body}
                  onChange={e => setBody(e.target.value)}
                  disabled={isWorking}
                  rows={4}
                  style={{
                    width: '100%',
                    padding: '8px 12px',
                    border: '1px solid #e2e8f0',
                    borderRadius: '6px',
                    fontSize: '14px',
                    fontFamily: 'inherit',
                    resize: 'vertical',
                  }}
                />
              </div>
            </div>

            <div className="modal-actions">
              <button
                type="button"
                className="btn-secondary"
                onClick={handleClose}
                disabled={isWorking}
              >
                Cancel
              </button>
              <button
                type="submit"
                className="btn-primary"
                disabled={isWorking || !title.trim()}
              >
                {getProgressText()}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}

export default CreatePRModal;
