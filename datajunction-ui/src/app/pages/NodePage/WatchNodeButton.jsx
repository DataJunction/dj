import { useContext, useState, useRef, useEffect } from 'react';
import DJClientContext from '../../providers/djclient';
import EyeIcon from '../../icons/EyeIcon';
import ExpandedIcon from '../../icons/ExpandedIcon';
import CollapsedIcon from '../../icons/CollapsedIcon';

const EVENT_TYPES = ['delete', 'update'];

export default function WatchButton({ node, buttonStyle }) {
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

  const toggleEvent = async event => {
    const isSelected = selectedEvents.includes(event);

    try {
      setLoading(true);
      let updatedEvents;

      if (!isSelected) {
        updatedEvents = Array.from(new Set([...selectedEvents, event]));
      } else {
        updatedEvents = selectedEvents.filter(e => e !== event);
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

  const handleWatchClick = async () => {
    try {
      setLoading(true);
      if (selectedEvents.length === 0) {
        await djClient.subscribeToNotifications({
          entity_type: 'node',
          entity_name: node.name,
          activity_types: EVENT_TYPES,
          alert_types: ['web'],
        });
        setSelectedEvents(EVENT_TYPES);
      } else {
        await djClient.unsubscribeFromNotifications({
          entity_type: 'node',
          entity_name: node.name,
        });
        setSelectedEvents([]);
      }
    } catch (err) {
      console.error('Watch toggle failed', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div
      className="btn-group"
      ref={dropdownRef}
      style={{
        position: 'relative',
        display: 'inline-flex',
        verticalAlign: 'middle',
      }}
    >
      <button
        onClick={handleWatchClick}
        disabled={loading}
        style={{
          ...buttonStyle,
          borderTopRightRadius: 0,
          borderBottomRightRadius: 0,
          borderRight: 'none',
        }}
      >
        <EyeIcon />
        Watch
        {selectedEvents.length > 0 && (
          <span
            style={{
              backgroundColor: '#e2e6ed',
              color: '#333',
              padding: '2px 5px',
              borderRadius: '999px',
              fontSize: '10px',
              fontWeight: 500,
              lineHeight: 1,
            }}
          >
            {selectedEvents.length}
          </span>
        )}
      </button>

      <button
        onClick={() => setDropdownOpen(prev => !prev)}
        disabled={loading}
        style={{
          ...buttonStyle,
          borderTopLeftRadius: 0,
          borderBottomLeftRadius: 0,
          padding: '0 6px',
        }}
        aria-label="Toggle dropdown"
      >
        {dropdownOpen ? <ExpandedIcon /> : <CollapsedIcon />}
      </button>

      {dropdownOpen && (
        <ul
          className="p-2"
          style={{
            display: 'block',
            minWidth: '220px',
            position: 'absolute',
            top: '100%',
            right: 0,
            zIndex: 9999,
            backgroundColor: '#fff',
            border: '1px solid #ccc',
            borderRadius: '0.5rem',
            marginTop: '0.25rem',
            boxShadow: '0px 6px 16px rgba(0, 0, 0, 0.1)',
            padding: '0.5rem 0',
          }}
        >
          {EVENT_TYPES.map(event => {
            const isSelected = selectedEvents.includes(event);
            return (
              <li
                key={event}
                onClick={() => toggleEvent(event)}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  padding: '0.5rem 1rem',
                  cursor: 'pointer',
                  backgroundColor: isSelected ? '#f0f4f8' : 'transparent',
                  fontWeight: isSelected ? '600' : '400',
                  fontSize: '0.9rem',
                  color: '#333',
                  borderLeft: isSelected
                    ? '4px solid #7983ff'
                    : '4px solid transparent',
                  transition: 'background 0.2s',
                }}
              >
                <input
                  className="form-check-input"
                  type="checkbox"
                  checked={isSelected}
                  readOnly
                  style={{ marginRight: '0.75rem' }}
                />
                {event}
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
