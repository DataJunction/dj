import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import LoadingIcon from '../../icons/LoadingIcon';
import EditIcon from '../../icons/EditIcon';
import '../../../styles/settings.css';

// Available activity types for subscriptions
const ACTIVITY_TYPES = [
  { value: 'create', label: 'Create' },
  { value: 'update', label: 'Update' },
  { value: 'delete', label: 'Delete' },
  { value: 'status_change', label: 'Status Change' },
  { value: 'refresh', label: 'Refresh' },
  { value: 'tag', label: 'Tag' },
];

export function SettingsPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [currentUser, setCurrentUser] = useState(null);
  const [subscriptions, setSubscriptions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [editingIndex, setEditingIndex] = useState(null);
  const [editedActivityTypes, setEditedActivityTypes] = useState([]);
  const [saving, setSaving] = useState(false);

  // Service accounts state
  const [serviceAccounts, setServiceAccounts] = useState([]);
  const [newAccountName, setNewAccountName] = useState('');
  const [creatingAccount, setCreatingAccount] = useState(false);
  const [newAccountCredentials, setNewAccountCredentials] = useState(null);
  const [showCreateModal, setShowCreateModal] = useState(false);

  useEffect(() => {
    async function fetchData() {
      try {
        const user = await djClient.whoami();
        setCurrentUser(user);

        // Fetch notification subscriptions
        const prefs = await djClient.getNotificationPreferences();

        // Fetch node details for subscriptions via GraphQL
        const nodeNames = (prefs || [])
          .filter(p => p.entity_type === 'node')
          .map(p => p.entity_name);

        let nodeInfoMap = {};
        if (nodeNames.length > 0) {
          const nodes = await djClient.getNodesByNames(nodeNames);
          nodeInfoMap = Object.fromEntries(
            nodes.map(n => [
              n.name,
              {
                node_type: n.type?.toLowerCase(),
                display_name: n.current?.displayName,
                status: n.current?.status?.toLowerCase(),
                mode: n.current?.mode?.toLowerCase(),
              },
            ]),
          );
        }

        // Merge node info into subscriptions
        const enrichedPrefs = (prefs || []).map(pref => ({
          ...pref,
          ...(nodeInfoMap[pref.entity_name] || {}),
        }));

        setSubscriptions(enrichedPrefs);

        // Fetch service accounts
        try {
          const accounts = await djClient.listServiceAccounts();
          setServiceAccounts(accounts || []);
        } catch (err) {
          // Service accounts may not be available, ignore error
          console.log('Service accounts not available:', err);
        }
      } catch (error) {
        console.error('Error fetching settings data:', error);
      } finally {
        setLoading(false);
      }
    }
    fetchData();
  }, [djClient]);

  const startEditing = index => {
    setEditingIndex(index);
    setEditedActivityTypes([...subscriptions[index].activity_types]);
  };

  const cancelEditing = () => {
    setEditingIndex(null);
    setEditedActivityTypes([]);
  };

  const toggleActivityType = activityType => {
    if (editedActivityTypes.includes(activityType)) {
      setEditedActivityTypes(
        editedActivityTypes.filter(t => t !== activityType),
      );
    } else {
      setEditedActivityTypes([...editedActivityTypes, activityType]);
    }
  };

  const saveSubscription = async sub => {
    if (editedActivityTypes.length === 0) {
      alert('Please select at least one activity type');
      return;
    }

    setSaving(true);
    try {
      await djClient.subscribeToNotifications({
        entity_type: sub.entity_type,
        entity_name: sub.entity_name,
        activity_types: editedActivityTypes,
        alert_types: sub.alert_types || ['web'],
      });

      // Update local state
      const updatedSubs = [...subscriptions];
      updatedSubs[editingIndex] = {
        ...sub,
        activity_types: editedActivityTypes,
      };
      setSubscriptions(updatedSubs);
      setEditingIndex(null);
      setEditedActivityTypes([]);
    } catch (error) {
      console.error('Error updating subscription:', error);
      alert('Failed to update subscription');
    } finally {
      setSaving(false);
    }
  };

  const handleUnsubscribe = async (sub, index) => {
    const confirmed = window.confirm(
      `Unsubscribe from notifications for "${sub.entity_name}"?`,
    );
    if (!confirmed) return;

    try {
      await djClient.unsubscribeFromNotifications({
        entity_type: sub.entity_type,
        entity_name: sub.entity_name,
      });
      setSubscriptions(subscriptions.filter((_, i) => i !== index));
    } catch (error) {
      console.error('Error unsubscribing:', error);
    }
  };

  // Service account handlers
  const openCreateModal = () => {
    setNewAccountName('');
    setNewAccountCredentials(null);
    setShowCreateModal(true);
  };

  const closeCreateModal = () => {
    setShowCreateModal(false);
    setNewAccountName('');
    // Keep credentials visible if they exist so user can still copy them
  };

  const handleCreateServiceAccount = async e => {
    e.preventDefault();
    if (!newAccountName.trim()) return;

    setCreatingAccount(true);
    try {
      const result = await djClient.createServiceAccount(newAccountName.trim());
      if (result.client_id) {
        setNewAccountCredentials(result);
        setServiceAccounts([...serviceAccounts, result]);
        setNewAccountName('');
      } else if (result.message) {
        alert(result.message);
      }
    } catch (error) {
      console.error('Error creating service account:', error);
      alert('Failed to create service account');
    } finally {
      setCreatingAccount(false);
    }
  };

  const dismissCredentialsAndClose = () => {
    setNewAccountCredentials(null);
    setShowCreateModal(false);
  };

  const handleDeleteServiceAccount = async account => {
    const confirmed = window.confirm(
      `Delete service account "${account.name}"?\n\nThis will revoke all access for this account and cannot be undone.`,
    );
    if (!confirmed) return;

    try {
      await djClient.deleteServiceAccount(account.client_id);
      setServiceAccounts(
        serviceAccounts.filter(a => a.client_id !== account.client_id),
      );
    } catch (error) {
      console.error('Error deleting service account:', error);
      alert('Failed to delete service account');
    }
  };

  const copyToClipboard = text => {
    navigator.clipboard.writeText(text);
  };

  if (loading) {
    return (
      <div className="settings-page">
        <div className="settings-container">
          <LoadingIcon />
        </div>
      </div>
    );
  }

  return (
    <div className="settings-page">
      <div className="settings-container">
        <h1 className="settings-title">Settings</h1>

        {/* Profile Section */}
        <section className="settings-section">
          <h2 className="settings-section-title">Profile</h2>
          <div className="settings-card">
            <div className="profile-info">
              <div className="profile-avatar">
                {currentUser?.name
                  ? currentUser.name
                      .split(' ')
                      .map(n => n[0])
                      .join('')
                      .toUpperCase()
                      .slice(0, 2)
                  : currentUser?.username?.slice(0, 2).toUpperCase() || '?'}
              </div>
              <div className="profile-details">
                <div className="profile-field">
                  <label>Username</label>
                  <span>{currentUser?.username || '-'}</span>
                </div>
                <div className="profile-field">
                  <label>Email</label>
                  <span>{currentUser?.email || '-'}</span>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Notification Subscriptions Section */}
        <section className="settings-section" id="notifications">
          <h2 className="settings-section-title">Notification Subscriptions</h2>
          <div className="settings-card">
            {subscriptions.length === 0 ? (
              <p className="empty-state">
                You're not watching any nodes yet. Visit a node page and click
                "Watch" to subscribe to updates.
              </p>
            ) : (
              <div className="subscriptions-list">
                {subscriptions.map((sub, index) => (
                  <div key={index} className="subscription-item">
                    <div className="subscription-header">
                      <div className="subscription-entity">
                        <a href={`/nodes/${sub.entity_name}`}>
                          {sub.entity_name}
                        </a>
                        {sub.node_type ? (
                          <span
                            className={`node_type__${sub.node_type} badge node_type`}
                          >
                            {sub.node_type.toUpperCase()}
                          </span>
                        ) : (
                          <span className="entity-type-badge">
                            {sub.entity_type}
                          </span>
                        )}
                        {sub.status === 'invalid' && (
                          <span className="status-badge status-invalid">
                            INVALID
                          </span>
                        )}
                      </div>
                      <div className="subscription-actions">
                        {editingIndex === index ? (
                          <>
                            <button
                              className="btn-save"
                              onClick={() => saveSubscription(sub)}
                              disabled={saving}
                            >
                              {saving ? 'Saving...' : 'Save'}
                            </button>
                            <button
                              className="btn-cancel"
                              onClick={cancelEditing}
                              disabled={saving}
                            >
                              Cancel
                            </button>
                          </>
                        ) : (
                          <>
                            <button
                              className="btn-icon btn-edit"
                              onClick={() => startEditing(index)}
                              title="Edit subscription"
                            >
                              <EditIcon />
                            </button>
                            <button
                              className="btn-icon btn-unsubscribe"
                              onClick={() => handleUnsubscribe(sub, index)}
                              title="Unsubscribe"
                            >
                              ×
                            </button>
                          </>
                        )}
                      </div>
                    </div>

                    <div className="subscription-activity-types">
                      <label className="activity-types-label">
                        Activity types:
                      </label>
                      {editingIndex === index ? (
                        <div className="activity-types-checkboxes">
                          {ACTIVITY_TYPES.map(type => (
                            <label key={type.value} className="checkbox-label">
                              <input
                                type="checkbox"
                                checked={editedActivityTypes.includes(
                                  type.value,
                                )}
                                onChange={() => toggleActivityType(type.value)}
                              />
                              {type.label}
                            </label>
                          ))}
                        </div>
                      ) : (
                        <div className="activity-types-badges">
                          {sub.activity_types?.map(type => (
                            <span
                              key={type}
                              className={`activity-badge activity-badge-${type}`}
                            >
                              {type}
                            </span>
                          )) || <span className="text-muted">All</span>}
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </section>

        {/* Service Accounts Section */}
        <section className="settings-section" id="service-accounts">
          <div className="section-title-row">
            <h2 className="settings-section-title">Service Accounts</h2>
            <button className="btn-create" onClick={openCreateModal}>
              + Create
            </button>
          </div>
          <div className="settings-card">
            <p className="section-description">
              Service accounts allow programmatic access to the DJ API. Create
              accounts for your applications, scripts, or CI/CD pipelines.
            </p>

            {/* List of existing service accounts */}
            {serviceAccounts.length > 0 ? (
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
                    {serviceAccounts.map(account => (
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
                            onClick={() => handleDeleteServiceAccount(account)}
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
        </section>
      </div>

      {/* Create Service Account Modal */}
      {showCreateModal && (
        <div className="modal-overlay" onClick={closeCreateModal}>
          <div className="modal-content" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <h3>Create Service Account</h3>
              <button
                className="btn-close-modal"
                onClick={closeCreateModal}
                title="Close"
              >
                ×
              </button>
            </div>

            {newAccountCredentials ? (
              /* Show credentials after creation */
              <div className="modal-body">
                <div className="credentials-success">
                  <span className="success-icon">✓</span>
                  <h4>Service Account Created!</h4>
                </div>
                <p className="credentials-warning">
                  Save these credentials now. The client secret will not be
                  shown again.
                </p>
                <div className="credentials-grid">
                  <div className="credential-item">
                    <label>Name</label>
                    <code>{newAccountCredentials.name}</code>
                  </div>
                  <div className="credential-item">
                    <label>Client ID</label>
                    <div className="credential-value">
                      <code>{newAccountCredentials.client_id}</code>
                      <button
                        className="btn-copy"
                        onClick={() =>
                          copyToClipboard(newAccountCredentials.client_id)
                        }
                        title="Copy"
                      >
                        📋
                      </button>
                    </div>
                  </div>
                  <div className="credential-item">
                    <label>Client Secret</label>
                    <div className="credential-value">
                      <code>{newAccountCredentials.client_secret}</code>
                      <button
                        className="btn-copy"
                        onClick={() =>
                          copyToClipboard(newAccountCredentials.client_secret)
                        }
                        title="Copy"
                      >
                        📋
                      </button>
                    </div>
                  </div>
                </div>
                <div className="modal-actions">
                  <button
                    className="btn-primary"
                    onClick={dismissCredentialsAndClose}
                  >
                    Done
                  </button>
                </div>
              </div>
            ) : (
              /* Show creation form */
              <form onSubmit={handleCreateServiceAccount}>
                <div className="modal-body">
                  <div className="form-group">
                    <label htmlFor="service-account-name">Name</label>
                    <input
                      id="service-account-name"
                      type="text"
                      placeholder="e.g., my-pipeline, etl-job, ci-cd"
                      value={newAccountName}
                      onChange={e => setNewAccountName(e.target.value)}
                      disabled={creatingAccount}
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
                    onClick={closeCreateModal}
                    disabled={creatingAccount}
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    className="btn-primary"
                    disabled={creatingAccount || !newAccountName.trim()}
                  >
                    {creatingAccount ? 'Creating...' : 'Create'}
                  </button>
                </div>
              </form>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
