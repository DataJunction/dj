import React, { useState } from 'react';

/**
 * Modal for creating a new branch namespace.
 */
export function CreateBranchModal({
  isOpen,
  onClose,
  onCreate,
  namespace,
  gitBranch,
}) {
  const [branchName, setBranchName] = useState('');
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState(null);
  const [result, setResult] = useState(null);

  // Convert branch name to expected namespace suffix for preview
  const previewNamespace = branchName
    ? `${namespace.split('.').slice(0, -1).join('.') || namespace}.${branchName
        .replace(/-/g, '_')
        .replace(/\//g, '_')}`
    : '';

  const handleSubmit = async e => {
    e.preventDefault();
    if (!branchName.trim()) return;

    setError(null);
    setCreating(true);

    try {
      const res = await onCreate(branchName.trim());
      if (res?._error) {
        setError(res.message);
      } else {
        setResult(res);
      }
    } catch (err) {
      setError(err.message || 'Failed to create branch');
    } finally {
      setCreating(false);
    }
  };

  const handleClose = () => {
    setBranchName('');
    setError(null);
    setResult(null);
    onClose();
  };

  if (!isOpen) return null;

  return (
    <div className="modal-overlay" onClick={handleClose}>
      <div className="modal-content" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <h3>Create Branch</h3>
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
              <h4 style={{ margin: '0 0 8px 0' }}>Branch Created!</h4>
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
                <strong>Namespace:</strong>{' '}
                <a
                  href={`/namespaces/${result.branch.namespace}`}
                  style={{ color: '#3b82f6' }}
                >
                  {result.branch.namespace}
                </a>
              </div>
              <div style={{ marginBottom: '8px' }}>
                <strong>Git Branch:</strong> {result.branch.git_branch}
              </div>
              <div>
                <strong>Parent:</strong> {result.branch.parent_namespace}
              </div>
              {result.deployment_results?.length > 0 && (
                <div style={{ marginTop: '12px' }}>
                  <strong>Nodes copied:</strong>{' '}
                  {result.deployment_results.length}
                </div>
              )}
            </div>

            <div className="modal-actions">
              <button className="btn-secondary" onClick={handleClose}>
                Close
              </button>
              <a
                href={`/namespaces/${result.branch.namespace}`}
                className="btn-primary"
                style={{ textDecoration: 'none' }}
              >
                Go to Branch
              </a>
            </div>
          </div>
        ) : (
          <form onSubmit={handleSubmit}>
            <div className="modal-body">
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
                <label htmlFor="branch-name">Branch Name</label>
                <input
                  id="branch-name"
                  type="text"
                  placeholder="feature-xyz"
                  value={branchName}
                  onChange={e => setBranchName(e.target.value)}
                  disabled={creating}
                  autoFocus
                />
                <span className="form-hint">
                  Name for the new git branch (e.g., "feature-xyz", "fix-bug")
                </span>
              </div>

              {branchName && (
                <div
                  style={{
                    backgroundColor: '#f0f9ff',
                    border: '1px solid #bae6fd',
                    borderRadius: '6px',
                    padding: '12px',
                    fontSize: '13px',
                    marginTop: '16px',
                  }}
                >
                  <div style={{ fontWeight: 500, marginBottom: '8px' }}>
                    This will:
                  </div>
                  <ul
                    style={{
                      margin: 0,
                      paddingLeft: '20px',
                      color: '#475569',
                    }}
                  >
                    <li>
                      Create git branch "{branchName}" from "{gitBranch}"
                    </li>
                    <li>Create namespace "{previewNamespace}"</li>
                  </ul>
                </div>
              )}
            </div>

            <div className="modal-actions">
              <button
                type="button"
                className="btn-secondary"
                onClick={handleClose}
                disabled={creating}
              >
                Cancel
              </button>
              <button
                type="submit"
                className="btn-primary"
                disabled={creating || !branchName.trim()}
              >
                {creating ? 'Creating...' : 'Create Branch'}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}

export default CreateBranchModal;
