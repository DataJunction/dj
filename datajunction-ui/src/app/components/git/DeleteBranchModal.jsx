import React, { useState } from 'react';

/**
 * Modal for confirming branch deletion.
 */
export function DeleteBranchModal({
  isOpen,
  onClose,
  onDelete,
  namespace,
  gitBranch,
  parentNamespace,
}) {
  const [deleteGitBranch, setDeleteGitBranch] = useState(true);
  const [deleting, setDeleting] = useState(false);
  const [error, setError] = useState(null);

  const handleSubmit = async e => {
    e.preventDefault();
    setError(null);
    setDeleting(true);

    try {
      const result = await onDelete(deleteGitBranch);
      if (result?._error) {
        setError(result.message);
      } else {
        onClose();
        // Redirect to parent namespace
        window.location.href = `/namespaces/${parentNamespace}`;
      }
    } catch (err) {
      setError(err.message || 'Failed to delete branch');
    } finally {
      setDeleting(false);
    }
  };

  const handleClose = () => {
    setDeleteGitBranch(false);
    setError(null);
    onClose();
  };

  if (!isOpen) return null;

  return (
    <div className="modal-overlay" onClick={handleClose}>
      <div className="modal-content" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <h3>Delete Branch</h3>
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
            <div
              style={{
                display: 'flex',
                alignItems: 'flex-start',
                gap: '12px',
                padding: '12px 12px 0 12px',
                backgroundColor: '#fef3c7',
                border: '1px solid #fcd34d',
                borderRadius: '6px',
                marginBottom: '16px',
              }}
            >
              <span
                style={{
                  fontSize: '20px',
                  paddingBottom: '12px',
                  margin: '-5px -1px 0 0',
                }}
              >
                ⚠️
              </span>
              <div style={{ fontSize: '13px' }}>
                Are you sure you want to delete this branch namespace?
              </div>
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

            <div
              style={{
                backgroundColor: '#f8fafc',
                borderRadius: '6px',
                padding: '12px',
                fontSize: '13px',
                marginBottom: '16px',
              }}
            >
              <div style={{ marginBottom: '8px' }}>
                <strong>Namespace:</strong> {namespace}
              </div>
              <div style={{ marginBottom: '8px' }}>
                <strong>Git Branch:</strong> {gitBranch}
              </div>
              <div>
                <strong>Parent:</strong> {parentNamespace}
              </div>
            </div>

            <label
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
                fontSize: '13px',
                cursor: 'pointer',
                fontWeight: 'none',
                textTransform: 'none',
              }}
            >
              <input
                type="checkbox"
                checked={deleteGitBranch}
                onChange={e => setDeleteGitBranch(e.target.checked)}
                disabled={deleting}
              />
              Also delete git branch on GitHub
            </label>
          </div>

          <div className="modal-actions">
            <button
              type="button"
              className="btn-secondary"
              onClick={handleClose}
              disabled={deleting}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="btn-primary"
              disabled={deleting}
              style={{
                backgroundColor: '#dc2626',
                borderColor: '#dc2626',
              }}
            >
              {deleting ? 'Deleting...' : 'Delete Branch'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

export default DeleteBranchModal;
