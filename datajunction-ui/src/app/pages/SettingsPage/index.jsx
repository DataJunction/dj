import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import LoadingIcon from '../../icons/LoadingIcon';
import ProfileSection from './ProfileSection';
import NotificationSubscriptionsSection from './NotificationSubscriptionsSection';
import ServiceAccountsSection from './ServiceAccountsSection';
import '../../../styles/settings.css';

/**
 * Main Settings page that orchestrates all settings sections.
 */
export function SettingsPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [currentUser, setCurrentUser] = useState(null);
  const [subscriptions, setSubscriptions] = useState([]);
  const [serviceAccounts, setServiceAccounts] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchData() {
      try {
        // Fetch user profile
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

  // Subscription handlers
  const handleUpdateSubscription = async (sub, activityTypes) => {
    await djClient.subscribeToNotifications({
      entity_type: sub.entity_type,
      entity_name: sub.entity_name,
      activity_types: activityTypes,
      alert_types: sub.alert_types || ['web'],
    });

    // Update local state
    setSubscriptions(
      subscriptions.map(s =>
        s.entity_name === sub.entity_name
          ? { ...s, activity_types: activityTypes }
          : s,
      ),
    );
  };

  const handleUnsubscribe = async sub => {
    await djClient.unsubscribeFromNotifications({
      entity_type: sub.entity_type,
      entity_name: sub.entity_name,
    });
    setSubscriptions(
      subscriptions.filter(s => s.entity_name !== sub.entity_name),
    );
  };

  // Service account handlers
  const handleCreateServiceAccount = async name => {
    const result = await djClient.createServiceAccount(name);
    if (result.client_id) {
      setServiceAccounts([...serviceAccounts, result]);
    }
    return result;
  };

  const handleDeleteServiceAccount = async clientId => {
    await djClient.deleteServiceAccount(clientId);
    setServiceAccounts(serviceAccounts.filter(a => a.client_id !== clientId));
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

        <ProfileSection user={currentUser} />

        <NotificationSubscriptionsSection
          subscriptions={subscriptions}
          onUpdate={handleUpdateSubscription}
          onUnsubscribe={handleUnsubscribe}
        />

        <ServiceAccountsSection
          accounts={serviceAccounts}
          onCreate={handleCreateServiceAccount}
          onDelete={handleDeleteServiceAccount}
        />
      </div>
    </div>
  );
}
