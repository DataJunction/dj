import * as React from 'react';
import { Link } from 'react-router-dom';
import LoadingIcon from '../icons/LoadingIcon';

/**
 * Reusable card component for dashboard sections
 *
 * @param {Object} props
 * @param {string} props.title - Section title
 * @param {string} [props.actionLink] - Optional link URL for "View All" or other action
 * @param {string} [props.actionText] - Text for action link (defaults to "View All →")
 * @param {boolean} [props.loading] - Show loading spinner
 * @param {React.ReactNode} [props.emptyState] - Content to show when empty (if loading is false and no children)
 * @param {React.ReactNode} props.children - Card content
 * @param {Object} [props.cardStyle] - Additional styles for the card wrapper
 * @param {Object} [props.contentStyle] - Additional styles for the content area
 * @param {boolean} [props.showHeader] - Whether to show the header (defaults to true)
 */
export function DashboardCard({
  title,
  actionLink,
  actionText = 'View All →',
  loading = false,
  emptyState = null,
  children,
  cardStyle = {},
  contentStyle = {},
  showHeader = true,
}) {
  // Check if children has actual content (not false, null, undefined)
  const hasContent =
    React.Children.toArray(children).filter(
      child => child !== false && child !== null && child !== undefined,
    ).length > 0;
  const showEmptyState = !loading && !hasContent && emptyState;

  return (
    <section>
      {showHeader && (
        <div className="section-title-row">
          <h2 className="settings-section-title">{title}</h2>
          {actionLink && (
            <Link to={actionLink} style={{ fontSize: '13px' }}>
              {actionText}
            </Link>
          )}
        </div>
      )}
      <div
        className="settings-card"
        style={{
          padding: '0.75rem',
          ...cardStyle,
          ...contentStyle,
        }}
      >
        {loading ? (
          <div style={{ textAlign: 'center', padding: '2rem' }}>
            <LoadingIcon />
          </div>
        ) : showEmptyState ? (
          emptyState
        ) : (
          children
        )}
      </div>
    </section>
  );
}

/**
 * Empty state component for dashboard cards
 */
export function DashboardCardEmpty({ icon, message, action }) {
  return (
    <div
      style={{
        padding: '2rem',
        textAlign: 'center',
        color: '#666',
        fontSize: '12px',
      }}
    >
      {icon && (
        <div style={{ fontSize: '24px', marginBottom: '0.5rem' }}>{icon}</div>
      )}
      <p>{message}</p>
      {action}
    </div>
  );
}

export default DashboardCard;
