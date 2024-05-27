import { useEffect, useState } from 'react';
import * as React from 'react';
import { labelize } from '../../../utils/form';
import LoadingIcon from '../../icons/LoadingIcon';

export default function NodeDependenciesTab({ node, djClient }) {
  const [nodeDAG, setNodeDAG] = useState({
    upstreams: [],
    downstreams: [],
    dimensions: [],
  });

  const [retrieved, setRetrieved] = useState(false);

  useEffect(() => {
    const fetchData = async () => {
      let upstreams = await djClient.upstreams(node.name);
      let downstreams = await djClient.downstreams(node.name);
      let dimensions = await djClient.nodeDimensions(node.name);
      setNodeDAG({
        upstreams: upstreams,
        downstreams: downstreams,
        dimensions: dimensions,
      });
      setRetrieved(true);
    };
    fetchData().catch(console.error);
  }, [djClient, node]);

  // Builds the block of dimensions selectors, grouped by node name + path
  return (
    <div>
      <h2>Upstreams</h2>
      {retrieved ? (
        <NodeList nodes={nodeDAG.upstreams} />
      ) : (
        <span style={{ display: 'inline-block' }}>
          <LoadingIcon />
        </span>
      )}
      <h2>Downstreams</h2>
      {retrieved ? (
        <NodeList nodes={nodeDAG.downstreams} />
      ) : (
        <span style={{ display: 'inline-block' }}>
          <LoadingIcon />
        </span>
      )}
      <h2>Dimensions</h2>
      {retrieved ? (
        <NodeDimensionsList rawDimensions={nodeDAG.dimensions} />
      ) : (
        <span style={{ display: 'inline-block' }}>
          <LoadingIcon />
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
              (dim.is_primary_key ? ' (PK)' : ''),
          };
        });
        return (
          <details>
            <summary style={{ marginBottom: '10px' }}>{groupHeader}</summary>
            <div className="dimensionsList">
              {dimensionGroupOptions.map(dimension => {
                return (
                  <div>
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
      {nodes?.map(node => (
        <li className="backfill" style={{ marginBottom: '5px' }}>
          <span
            className={`node_type__${node.type} badge node_type`}
            style={{ marginRight: '5px' }}
            role="dialog"
            aria-hidden="false"
            aria-label="NodeType"
          >
            {node.type}
          </span>
          <a href={`/nodes/${node.name}`}>{node.name}</a>
        </li>
      ))}
    </ul>
  ) : (
    <span style={{ display: 'inline-block' }}>None</span>
  );
}
