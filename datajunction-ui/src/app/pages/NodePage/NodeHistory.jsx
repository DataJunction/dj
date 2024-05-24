import { useEffect, useState } from 'react';
import * as React from 'react';
import DiffIcon from '../../icons/DiffIcon';
import { labelize } from '../../../utils/form';
import CommitIcon from '../../icons/CommitIcon';

export default function NodeHistory({ node, djClient }) {
  const [history, setHistory] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      if (node) {
        const data = await djClient.history('node', node.name);
        setHistory(data);
      }
    };
    fetchData().catch(console.error);
  }, [djClient, node]);

  const eventData = event => {
    const standard = (
      <>
        <a href={'#'} className={'highlight-svg'} title="Browse Details">
          <CommitIcon /> Details
        </a>
      </>
    );

    if (event.activity_type === 'update' && event.entity_type === 'node') {
      return (
        <>
          <a href={`/nodes/${event.node}/revisions/${event.details.version}`}>
            <span className={`badge version`}>{event.details.version}</span>
          </a>
          <a
            href={`/nodes/${event.node}/revisions/${event.details.version}`}
            className={'highlight-svg'}
            title="View Diff"
          >
            <DiffIcon /> Diff
          </a>
        </>
      );
    }
    return '';
  };

  const eventDescription = event => {
    if (event.activity_type === 'create' && event.entity_type === 'node') {
      return (
        <div className="history-left">
          <b style={{ textTransform: 'capitalize' }}>{event.activity_type}</b>{' '}
          {event.entity_type}{' '}
          <b>
            <a href={'/nodes/' + event.entity_name}>{event.entity_name}</a>
          </b>
        </div>
      );
    }
    if (event.activity_type === 'create' && event.entity_type === 'link') {
      return (
        <div className="history-left">
          <b style={{ textTransform: 'capitalize' }}>{event.activity_type}</b>{' '}
          {event.entity_type} from{' '}
          <b>
            <a href={'/nodes/' + event.entity_name}>{event.entity_name}</a>
          </b>{' '}
          to{' '}
          <b>
            <a href={'/nodes/' + event.details.dimension}>
              {event.details.dimension}
            </a>
          </b>
        </div>
      );
    }
    if (event.activity_type === 'refresh') {
      return (
        <div className="history-left">
          <b style={{ textTransform: 'capitalize' }}>{event.activity_type}</b>{' '}
          {event.entity_type}{' '}
          <b>
            <a href={'/nodes/' + event.entity_name}>{event.entity_name}</a>
          </b>
        </div>
      );
    }
    if (event.activity_type === 'update' && event.entity_type === 'node') {
      return (
        <div className="history-left">
          <b style={{ textTransform: 'capitalize' }}>{event.activity_type}</b>{' '}
          {event.entity_type}{' '}
          <b>
            <a href={'/nodes/' + event.entity_name}>{event.entity_name}</a>
          </b>
        </div>
      );
    }
    if (event.activity_type === 'tag' && event.entity_type === 'node') {
      return (
        <div className="history-left">
          Add tag{event.details.tags.length > 1 ? 's' : ''}{' '}
          {event.details.tags.map(tag => (
            <span className={'badge version'}>
              <a href={`/tags/${tag}`}>{tag}</a>
            </span>
          ))}
        </div>
      );
    }
    if (event.activity_type === 'create' && event.entity_type === 'partition') {
      return (
        <div className="history-left">
          Set <b>{event.details.partition.type_} partition</b> on{' '}
          <a href={'/nodes/' + event.node}>{event.details.column}</a>
        </div>
      );
    }

    if (
      event.activity_type === 'set_attribute' &&
      event.entity_type === 'column_attribute'
    ) {
      return (
        <div className="history-left">
          <b>Set column attributes</b> on{' '}
          <b>
            <a href={'/nodes/' + event.node}>{event.node}</a>
          </b>
        </div>
      );
    }

    if (event.entity_type === 'materialization') {
      return (
        <div className="history-left">
          <span style={{ textTransform: 'capitalize' }}>
            {event.activity_type}
          </span>{' '}
          <b>{event.entity_type}</b>{' '}
          <a href={`/nodes/${event.node}/materializations`}>
            {event.entity_name}
          </a>
        </div>
      );
    }

    if (
      event.activity_type === 'status_change' &&
      event.entity_type === 'node'
    ) {
      return (
        <div className="history-left">
          <b style={{ textTransform: 'capitalize' }}>
            {labelize(event.activity_type)}
          </b>{' '}
          on <a href={`/nodes/${event.node}`}>{event.node}</a> from{' '}
          <b>{event.pre.status}</b> to <b>{event.post.status}</b>
        </div>
      );
    }

    if (
      event.activity_type === 'create' &&
      event.entity_type === 'availability'
    ) {
      return (
        <div className="history-left">
          Materialized table{' '}
          <code>
            {event.post.catalog}.{event.post.schema_}.{event.post.table}
          </code>{' '}
          with partitions between {event.post.min_temporal_partition} and{' '}
          {event.post.max_temporal_partition} available for{' '}
          <a href={'/nodes/' + event.node}>{event.node}</a>.
        </div>
      );
    }

    if (event.activity_type === 'create' && event.entity_type === 'backfill') {
      return (
        <div className="history-left">
          Backfill created for materialization {event.details.materialization}{' '}
          for partition {event.details.partition.column_name} from{' '}
          {event.details.partition.range[0]} to{' '}
          {event.details.partition.range[1]}
        </div>
      );
    }
  };

  const removeTagNodeEventsWithoutTags = event =>
    event.activity_type !== 'tag' ||
    (event.activity_type === 'tag' &&
      event.entity_type === 'node' &&
      event.details.tags.length > 0);

  return (
    <ul className="history-border" role="list" aria-label="Activity">
      {history.filter(removeTagNodeEventsWithoutTags).map(event => (
        <li key={`history-row-${event.id}`} className="history">
          {eventDescription(event)}
          <div className={'history-small'}>
            done by <a href="#">{event.user ? event.user : 'unknown'}</a> on{' '}
            {new Date(Date.parse(event.created_at)).toLocaleString()}
          </div>
          <div className="history-right">{eventData(event)}</div>
        </li>
      ))}
    </ul>
  );
}
