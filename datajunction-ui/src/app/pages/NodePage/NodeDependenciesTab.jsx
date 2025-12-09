import { useEffect, useState } from 'react';
import * as React from 'react';
import { labelize } from '../../../utils/form';
import LoadingIcon from '../../icons/LoadingIcon';

export default function NodeDependenciesTab({ node, djClient }) {
  const [upstreams, setUpstreams] = useState(null);
  const [downstreams, setDownstreams] = useState(null);
  const [dimensions, setDimensions] = useState(null);

  useEffect(() => {
    if (node) {
      // Reset state when node changes
      setUpstreams(null);
      setDownstreams(null);
      setDimensions(null);

      // Load all three in parallel, display each as it loads
      djClient.upstreamsGQL(node.name).then(setUpstreams).catch(console.error);
      djClient
        .downstreamsGQL(node.name)
        .then(setDownstreams)
        .catch(console.error);
      djClient
        .nodeDimensions(node.name)
        .then(setDimensions)
        .catch(console.error);
    }
  }, [djClient, node]);

  return (
    <div>
      <h2>Upstreams</h2>
      {upstreams !== null ? (
        <NodeList nodes={upstreams} />
      ) : (
        <span style={{ display: 'block' }}>
          <LoadingIcon centered={false} />
        </span>
      )}
      <h2>Downstreams</h2>
      {downstreams !== null ? (
        <NodeList nodes={downstreams} />
      ) : (
        <span style={{ display: 'block' }}>
          <LoadingIcon centered={false} />
        </span>
      )}
      <h2>Dimensions</h2>
      {dimensions !== null ? (
        <NodeDimensionsList rawDimensions={dimensions} />
      ) : (
        <span style={{ display: 'block' }}>
          <LoadingIcon centered={false} />
        </span>
      )}
    </div>
  );
}

export function NodeDimensionsList({ rawDimensions }) {
  const dimensions = Object.entries(
    rawDimensions.reduce((group, dimension) => {
      group[dimension.node_name + dimension.path] =
        group[dimension.node_name + dimension.path] ?? [];
      group[dimension.node_name + dimension.path].push(dimension);
      return group;
    }, {}),
  );
  return (
    <div style={{ padding: '0.5rem' }}>
      {dimensions.map(grouping => {
        const dimensionsInGroup = grouping[1];
        const role = dimensionsInGroup[0].path
          .map(pathItem => pathItem.split('.').slice(-1))
          .join(' → ');
        const fullPath = dimensionsInGroup[0].path.join(' → ');
        const groupHeader = (
          <span
            style={{
              fontWeight: 'normal',
              marginBottom: '15px',
              marginTop: '15px',
            }}
          >
            <a href={`/nodes/${dimensionsInGroup[0].node_name}`}>
              <b>{dimensionsInGroup[0].node_display_name}</b>
            </a>{' '}
            with role{' '}
            <span className="HighlightPath">
              <b>{role}</b>
            </span>{' '}
            via <span className="HighlightPath">{fullPath}</span>
          </span>
        );
        const dimensionGroupOptions = dimensionsInGroup.map(dim => {
          return {
            value: dim.name,
            label:
              labelize(dim.name.split('.').slice(-1)[0]) +
              (dim.properties.includes('primary_key') ? ' (PK)' : ''),
          };
        });
        return (
          <details key={grouping[0]}>
            <summary style={{ marginBottom: '10px' }}>{groupHeader}</summary>
            <div className="dimensionsList">
              {dimensionGroupOptions.map(dimension => {
                return (
                  <div key={dimension.value}>
                    {dimension.label.split('[').slice(0)[0]} ⇢{' '}
                    <code className="DimensionAttribute">
                      {dimension.value}
                    </code>
                  </div>
                );
              })}
            </div>
          </details>
        );
      })}
    </div>
  );
}

export function NodeList({ nodes }) {
  return nodes && nodes.length > 0 ? (
    <ul className="backfills">
      {nodes?.map(node => {
        // GraphQL returns uppercase types (e.g., "SOURCE"), CSS classes expect lowercase
        const nodeType = node.type?.toLowerCase() || '';
        return (
          <li
            className="backfill"
            style={{ marginBottom: '5px' }}
            key={node.name}
          >
            <span
              className={`node_type__${nodeType} badge node_type`}
              style={{ marginRight: '5px' }}
              role="dialog"
              aria-hidden="false"
              aria-label="NodeType"
            >
              {nodeType}
            </span>
            <a href={`/nodes/${node.name}`}>{node.name}</a>
          </li>
        );
      })}
    </ul>
  ) : (
    <span style={{ display: 'block' }}>None</span>
  );
}
