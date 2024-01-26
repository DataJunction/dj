import { useEffect, useState } from 'react';
import * as React from 'react';
import { labelize } from '../../../utils/form';

export default function NodeDimensionsTab({ node, djClient }) {
  const [dimensions, setDimensions] = useState([]);
  useEffect(() => {
    const fetchData = async () => {
      if (node) {
        const data = await djClient.nodeDimensions(node.name);
        const grouped = Object.entries(
          data.reduce((group, dimension) => {
            group[dimension.node_name + dimension.path] =
              group[dimension.node_name + dimension.path] ?? [];
            group[dimension.node_name + dimension.path].push(dimension);
            return group;
          }, {}),
        );
        setDimensions(grouped);
      }
    };
    fetchData().catch(console.error);
  }, [djClient, node]);

  // Builds the block of dimensions selectors, grouped by node name + path
  return (
    <div style={{ padding: '1rem' }}>
      {dimensions.map(grouping => {
        const dimensionsInGroup = grouping[1];
        const role = dimensionsInGroup[0].path
          .map(pathItem => pathItem.split('.').slice(-1))
          .join(' → ');
        const fullPath = dimensionsInGroup[0].path.join(' → ');
        const groupHeader = (
          <h4
            style={{
              fontWeight: 'normal',
              marginBottom: '5px',
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
          </h4>
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
          <>
            {groupHeader}
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
          </>
        );
      })}
    </div>
  );
}
