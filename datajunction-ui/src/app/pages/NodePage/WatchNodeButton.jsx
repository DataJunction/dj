import { useContext, useState, useRef, useEffect } from 'react';
import DJClientContext from '../../providers/djclient';
import EyeIcon from '../../icons/EyeIcon';

const EVENT_TYPES = [
  { id: 'delete', label: 'On Delete' },
  { id: 'update', label: 'On Update' },
];

export default function WatchButton({ node }) {
  // All hooks must be called before any early returns
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [selectedEvents, setSelectedEvents] = useState([]);
  const [loading, setLoading] = useState(false);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const dropdownRef = useRef();

  // Load existing preferences
  useEffect(() => {
    if (!node || !node.name || !node.type) {
      return;
    }

    const loadPreferences = async () => {
      try {
        const preferences = await djClient.getNotificationPreferences({
          entity_name: node.name,
        });

        const matched = preferences.find(item =>
          item.alert_types.includes('web'),
        );

        if (matched) {
          setSelectedEvents(matched.activity_types);
        }
      } catch (err) {
        console.error('Failed to load notification preferences', err);
      }
    };

    loadPreferences();
  }, [djClient, node?.name, node?.type]);

  // Close dropdown on outside click
  useEffect(() => {
    const handleClickOutside = event => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setDropdownOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Early return after all hooks are called
  if (!node || !node.name || !node.type) {
    return null;
  }

  const toggleEvent = async eventId => {
    const isSelected = selectedEvents.includes(eventId);

    try {
      setLoading(true);
      let updatedEvents;

      if (!isSelected) {
        updatedEvents = Array.from(new Set([...selectedEvents, eventId]));
      } else {
        updatedEvents = selectedEvents.filter(e => e !== eventId);
      }

      if (updatedEvents.length === 0) {
        await djClient.unsubscribeFromNotifications({
          entity_type: 'node',
          entity_name: node.name,
        });
      } else {
        await djClient.subscribeToNotifications({
          entity_type: 'node',
          entity_name: node.name,
          activity_types: updatedEvents,
          alert_types: ['web'],
        });
      }

      setSelectedEvents(updatedEvents);
    } catch (err) {
      console.error('Failed to update preference', err);
    } finally {
      setLoading(false);
    }
  };

  const isWatching = selectedEvents.length > 0;

  return (
    <div
      ref={dropdownRef}
      style={{
        position: 'relative',
        display: 'inline-block',
      }}
    >
      <button
        className="action-btn"
        onClick={() => setDropdownOpen(prev => !prev)}
        disabled={loading}
        style={{
          backgroundColor: isWatching ? '#e3f2fd' : undefined,
          borderColor: isWatching ? '#1976d2' : undefined,
          color: isWatching ? '#1976d2' : undefined,
        }}
      >
        <EyeIcon />
        {isWatching ? `Watching (${selectedEvents.length})` : 'Watch'}
        <span
          style={{
            fontSize: '8px',
            marginLeft: '4px',
            opacity: 0.7,
          }}
        >
          {dropdownOpen ? '▲' : '▼'}
        </span>
      </button>

      {dropdownOpen && (
        <div
          style={{
            position: 'absolute',
            top: '100%',
            right: 0,
            marginTop: '4px',
            padding: '12px',
            backgroundColor: 'white',
            border: '1px solid #ddd',
            borderRadius: '8px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
            zIndex: 1000,
            minWidth: '180px',
          }}
        >
          <div
            style={{
              fontSize: '10px',
              fontWeight: '600',
              color: '#666',
              textTransform: 'uppercase',
              letterSpacing: '0.5px',
              marginBottom: '8px',
            }}
          >
            Notify me
          </div>
          {EVENT_TYPES.map(event => {
            const isSelected = selectedEvents.includes(event.id);
            return (
              <label
                key={event.id}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px',
                  fontSize: '12px',
                  color: '#444',
                  marginBottom: '8px',
                  cursor: loading ? 'wait' : 'pointer',
                  opacity: loading ? 0.6 : 1,
                }}
              >
                <input
                  type="checkbox"
                  checked={isSelected}
                  onChange={() => toggleEvent(event.id)}
                  disabled={loading}
                  style={{ accentColor: '#1976d2' }}
                />
                {event.label}
              </label>
            );
          })}
        </div>
      )}
    </div>
  );
}
