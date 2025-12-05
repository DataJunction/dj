import React, { useState } from 'react';
import EditIcon from '../../icons/EditIcon';

// Available activity types for subscriptions
const ACTIVITY_TYPES = [
  { value: 'create', label: 'Create' },
  { value: 'update', label: 'Update' },
  { value: 'delete', label: 'Delete' },
  { value: 'status_change', label: 'Status Change' },
  { value: 'refresh', label: 'Refresh' },
  { value: 'tag', label: 'Tag' },
];

/**
 * Displays and manages notification subscriptions.
 */
export function NotificationSubscriptionsSection({
  subscriptions,
  onUpdate,
  onUnsubscribe,
}) {
  const [editingIndex, setEditingIndex] = useState(null);
  const [editedActivityTypes, setEditedActivityTypes] = useState([]);
  const [saving, setSaving] = useState(false);

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
      await onUpdate(sub, editedActivityTypes);
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
      await onUnsubscribe(sub);
    } catch (error) {
      console.error('Error unsubscribing:', error);
    }
  };

  return (
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
                    <a href={`/nodes/${sub.entity_name}`}>{sub.entity_name}</a>
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
                            checked={editedActivityTypes.includes(type.value)}
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
  );
}

export default NotificationSubscriptionsSection;
