import React, { useState } from 'react';

/**
 * Modal for creating a new service account.
 * Shows a form initially, then displays credentials after successful creation.
 */
export function CreateServiceAccountModal({ isOpen, onClose, onCreate }) {
  const [name, setName] = useState('');
  const [creating, setCreating] = useState(false);
  const [credentials, setCredentials] = useState(null);

  const handleSubmit = async e => {
    e.preventDefault();
    if (!name.trim()) return;

    setCreating(true);
    try {
      const result = await onCreate(name.trim());
      if (result.client_id) {
        setCredentials(result);
        setName('');
      } else if (result.message) {
        alert(result.message);
      }
    } catch (error) {
      console.error('Error creating service account:', error);
      alert('Failed to create service account');
    } finally {
      setCreating(false);
    }
  };

  const handleClose = () => {
    setName('');
    setCredentials(null);
    onClose();
  };

  const copyToClipboard = text => {
    navigator.clipboard.writeText(text);
  };

  if (!isOpen) return null;

  return (
    <div className="modal-overlay" onClick={handleClose}>
      <div className="modal-content" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <h3>Create Service Account</h3>
          <button
            className="btn-close-modal"
            onClick={handleClose}
            title="Close"
          >
            ×
          </button>
        </div>

        {credentials ? (
          /* Show credentials after creation */
          <div className="modal-body">
            <div className="credentials-success">
              <span className="success-icon">✓</span>
              <h4>Service Account Created!</h4>
            </div>
            <p className="credentials-warning">
              Save these credentials now. The client secret will not be shown
              again.
            </p>
            <div className="credentials-grid">
              <div className="credential-item">
                <label>Name</label>
                <code>{credentials.name}</code>
              </div>
              <div className="credential-item">
                <label>Client ID</label>
                <div className="credential-value">
                  <code>{credentials.client_id}</code>
                  <button
                    className="btn-copy"
                    onClick={() => copyToClipboard(credentials.client_id)}
                    title="Copy"
                  >
                    📋
                  </button>
                </div>
              </div>
              <div className="credential-item">
                <label>Client Secret</label>
                <div className="credential-value">
                  <code>{credentials.client_secret}</code>
                  <button
                    className="btn-copy"
                    onClick={() => copyToClipboard(credentials.client_secret)}
                    title="Copy"
                  >
                    📋
                  </button>
                </div>
              </div>
            </div>
            <div className="modal-actions">
              <button className="btn-primary" onClick={handleClose}>
                Done
              </button>
            </div>
          </div>
        ) : (
          /* Show creation form */
          <form onSubmit={handleSubmit}>
            <div className="modal-body">
              <div className="form-group">
                <label htmlFor="service-account-name">Name</label>
                <input
                  id="service-account-name"
                  type="text"
                  placeholder="e.g., my-pipeline, etl-job, ci-cd"
                  value={name}
                  onChange={e => setName(e.target.value)}
                  disabled={creating}
                  autoFocus
                />
                <span className="form-hint">
                  A descriptive name to identify this service account
                </span>
              </div>
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
                disabled={creating || !name.trim()}
              >
                {creating ? 'Creating...' : 'Create'}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}

export default CreateServiceAccountModal;
