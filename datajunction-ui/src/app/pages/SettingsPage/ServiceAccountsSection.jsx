import React, { useState } from 'react';
import CreateServiceAccountModal from './CreateServiceAccountModal';

/**
 * Displays and manages service accounts.
 */
export function ServiceAccountsSection({ accounts, onCreate, onDelete }) {
  const [showModal, setShowModal] = useState(false);

  const handleDelete = async account => {
    const confirmed = window.confirm(
      `Delete service account "${account.name}"?\n\nThis will revoke all access for this account and cannot be undone.`,
    );
    if (!confirmed) return;

    try {
      await onDelete(account.client_id);
    } catch (error) {
      console.error('Error deleting service account:', error);
      alert('Failed to delete service account');
    }
  };

  const handleCreate = async name => {
    const result = await onCreate(name);
    return result;
  };

  return (
    <section className="settings-section" id="service-accounts">
      <div className="section-title-row">
        <h2 className="settings-section-title">Service Accounts</h2>
        <button className="btn-create" onClick={() => setShowModal(true)}>
          + Create
        </button>
      </div>
      <div className="settings-card">
        <p className="section-description">
          Service accounts allow programmatic access to the DJ API. Create
          accounts for your applications, scripts, or CI/CD pipelines.
        </p>

        {accounts.length > 0 ? (
          <div className="service-accounts-list">
            <table className="service-accounts-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Client ID</th>
                  <th>Created</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {accounts.map(account => (
                  <tr key={account.id}>
                    <td>{account.name}</td>
                    <td>
                      <code className="client-id">{account.client_id}</code>
                    </td>
                    <td className="created-date">
                      {new Date(account.created_at).toLocaleDateString()}
                    </td>
                    <td className="actions-cell">
                      <button
                        className="btn-icon btn-delete-account"
                        onClick={() => handleDelete(account)}
                        title="Delete service account"
                      >
                        ×
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="empty-state">
            No service accounts yet. Create one to enable programmatic API
            access.
          </p>
        )}
      </div>

      <CreateServiceAccountModal
        isOpen={showModal}
        onClose={() => setShowModal(false)}
        onCreate={handleCreate}
      />
    </section>
  );
}

export default ServiceAccountsSection;
