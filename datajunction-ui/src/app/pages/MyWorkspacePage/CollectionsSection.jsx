import * as React from 'react';
import { useContext, useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import DJClientContext from '../../providers/djclient';
import DashboardCard from '../../components/DashboardCard';

// Collections Section (includes featured + my collections)
export function CollectionsSection({ collections, loading, currentUser }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [allCollections, setAllCollections] = useState([]);
  const [allLoading, setAllLoading] = useState(true);

  useEffect(() => {
    const fetchAll = async () => {
      try {
        const response = await djClient.listAllCollections();
        console.log('All collections response:', response);
        const all = response?.data?.listCollections || [];
        setAllCollections(all);
      } catch (error) {
        console.error('Error fetching all collections:', error);
        // Fall back to user's collections if fetching all fails
        setAllCollections(collections);
      }
      setAllLoading(false);
    };
    fetchAll();
  }, [djClient, collections]);

  // Sort: user's collections first, then others
  // If allCollections is empty, fall back to the collections prop
  const collectionsToUse =
    allCollections.length > 0 ? allCollections : collections;
  const myCollections = collectionsToUse.filter(
    c => c.createdBy?.username === currentUser?.username || !c.createdBy,
  );
  const otherCollections = collectionsToUse.filter(
    c => c.createdBy && c.createdBy?.username !== currentUser?.username,
  );
  const allToShow = [...myCollections, ...otherCollections].slice(0, 8);

  const collectionsGrid = allToShow.map(collection => {
    const createdByUsername = collection.createdBy?.username;
    const isOwner = createdByUsername === currentUser?.username;
    const ownerDisplay = isOwner
      ? 'you'
      : createdByUsername?.split('@')[0] || 'unknown';

    return (
      <a
        key={collection.name}
        href={`/collections/${collection.name}`}
        style={{
          display: 'flex',
          flexDirection: 'column',
          padding: '1rem',
          border: '1px solid var(--border-color, #e0e0e0)',
          borderRadius: '8px',
          textDecoration: 'none',
          color: 'inherit',
          transition: 'all 0.15s ease',
          backgroundColor: 'var(--card-bg, #fff)',
          cursor: 'pointer',
        }}
        onMouseEnter={e => {
          e.currentTarget.style.borderColor = 'var(--primary-color, #007bff)';
          e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.1)';
          e.currentTarget.style.transform = 'translateY(-2px)';
        }}
        onMouseLeave={e => {
          e.currentTarget.style.borderColor = 'var(--border-color, #e0e0e0)';
          e.currentTarget.style.boxShadow = 'none';
          e.currentTarget.style.transform = 'translateY(0)';
        }}
      >
        <div
          style={{
            fontWeight: '600',
            fontSize: '14px',
            marginBottom: '0.5rem',
            lineHeight: '1.3',
          }}
        >
          {collection.name}
        </div>
        {collection.description && (
          <div
            style={{
              fontSize: '12px',
              color: '#666',
              lineHeight: '1.4',
              marginBottom: '0.75rem',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              display: '-webkit-box',
              WebkitLineClamp: 2,
              WebkitBoxOrient: 'vertical',
              flex: 1,
            }}
          >
            {collection.description}
          </div>
        )}
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            fontSize: '11px',
            color: '#888',
            marginTop: 'auto',
          }}
        >
          <span>
            {collection.nodeCount}{' '}
            {collection.nodeCount === 1 ? 'node' : 'nodes'}
          </span>
          <span
            style={{
              color: isOwner ? 'var(--primary-color, #4a90d9)' : '#888',
            }}
          >
            by {ownerDisplay}
          </span>
        </div>
      </a>
    );
  });

  return (
    <DashboardCard
      title="Collections"
      actionLink="/collections"
      actionText="+ Create"
      loading={loading || allLoading}
      cardStyle={{ padding: '0.75rem', minHeight: '200px' }}
      emptyState={
        <div style={{ padding: '1rem', textAlign: 'center' }}>
          <div
            style={{ fontSize: '48px', marginBottom: '0.5rem', opacity: 0.3 }}
          >
            📁
          </div>
          <p style={{ fontSize: '13px', color: '#666', marginBottom: '1rem' }}>
            No collections yet
          </p>
          <p
            style={{
              fontSize: '11px',
              color: '#999',
              marginBottom: '1rem',
              lineHeight: '1.4',
            }}
          >
            Group related metrics and dimensions together for easier discovery
          </p>
          <Link
            to="/collections"
            style={{
              display: 'inline-block',
              padding: '6px 12px',
              fontSize: '11px',
              backgroundColor: 'var(--primary-color, #4a90d9)',
              color: '#fff',
              borderRadius: '6px',
              textDecoration: 'none',
              fontWeight: '500',
            }}
          >
            + Create Collection
          </Link>
        </div>
      }
    >
      {allToShow.length > 0 && (
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))',
            gap: '0.75rem',
          }}
        >
          {collectionsGrid}
        </div>
      )}
    </DashboardCard>
  );
}
