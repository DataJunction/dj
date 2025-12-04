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
      </div>
    </div>
  );
}
