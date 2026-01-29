import React, { useState } from 'react';

/**
 * Modal for syncing namespace to git.
 */
export function SyncToGitModal({
  isOpen,
  onClose,
  onSync,
  namespace,
  gitBranch,
  repoPath,
}) {
  const [commitMessage, setCommitMessage] = useState('');
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState(null);
  const [result, setResult] = useState(null);

  const handleSubmit = async e => {
    e.preventDefault();
    setError(null);
    setSyncing(true);

    try {
      const res = await onSync(commitMessage.trim() || null);
      if (res?._error) {
        setError(res.message);
      } else {
        setResult(res);
      }
    } catch (err) {
      setError(err.message || 'Failed to sync to git');
    } finally {
      setSyncing(false);
    }
  };

  const handleClose = () => {
    setCommitMessage('');
    setError(null);
    setResult(null);
    onClose();
  };

  if (!isOpen) return null;

  return (
    <div className="modal-overlay" onClick={handleClose}>
      <div className="modal-content" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <h3>Sync to Git</h3>
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
                Synced {result.files_synced} file
                {result.files_synced !== 1 ? 's' : ''}!
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
                <strong>Commit:</strong>{' '}
                <a
                  href={result.commit_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{
                    fontFamily: 'monospace',
                    fontSize: '12px',
                    color: '#3b82f6',
                  }}
                >
                  {result.commit_sha?.slice(0, 7)}
                </a>
              </div>
              <div>
                <strong>Branch:</strong> {gitBranch}
              </div>
            </div>

            <div className="modal-actions">
              <button className="btn-secondary" onClick={handleClose}>
                Close
              </button>
              <a
                href={result.commit_url}
                target="_blank"
                rel="noopener noreferrer"
                className="btn-primary"
                style={{ textDecoration: 'none' }}
              >
                View Commit
              </a>
            </div>
          </div>
        ) : (
          <form onSubmit={handleSubmit}>
            <div className="modal-body">
              <p
                style={{
                  color: '#64748b',
                  fontSize: '13px',
                  marginBottom: '16px',
                }}
              >
                Sync all nodes in <strong>{namespace}</strong> to git as YAML
                files.
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

              <div
                style={{
                  backgroundColor: '#f8fafc',
                  borderRadius: '6px',
                  padding: '12px',
                  fontSize: '13px',
                  marginBottom: '16px',
                }}
              >
                <div style={{ marginBottom: '4px' }}>
                  <strong>Repository:</strong> {repoPath}
                </div>
                <div>
                  <strong>Branch:</strong> {gitBranch}
                </div>
              </div>

              <div className="form-group">
                <label htmlFor="commit-message">
                  Commit Message (optional)
                </label>
                <input
                  id="commit-message"
                  type="text"
                  placeholder={`Sync ${namespace}`}
                  value={commitMessage}
                  onChange={e => setCommitMessage(e.target.value)}
                  disabled={syncing}
                />
                <span className="form-hint">
                  Leave blank to use default message
                </span>
              </div>
            </div>

            <div className="modal-actions">
              <button
                type="button"
                className="btn-secondary"
                onClick={handleClose}
                disabled={syncing}
              >
                Cancel
              </button>
              <button type="submit" className="btn-primary" disabled={syncing}>
                {syncing ? 'Syncing...' : 'Sync Now'}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}

export default SyncToGitModal;
