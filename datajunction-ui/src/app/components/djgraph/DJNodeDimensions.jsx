import React, { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import Collapse from './Collapse';

export function DJNodeDimensions(data) {
  const [dimensions, setDimensions] = useState([]);
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  useEffect(() => {
    if (data.type === 'metric') {
      async function getDimensions() {
        try {
          const metricData = await djClient.metric(data.name);
          setDimensions(metricData.dimensions);
        } catch (err) {
          console.log(err);
        }
      }
      getDimensions();
    }
  }, [data, djClient]);
  const dimensionsToObject = dimensions => {
    return dimensions.map(dim => {
      const [attribute, ...nodeName] = dim.name.split('.').reverse();
      return {
        dimension: nodeName.reverse().join('.'),
        path: dim.path,
        column: attribute,
      };
    });
  };
  const groupedDimensions = dims =>
    dims.reduce((acc, current) => {
      const dimKey = current.dimension + ' via ' + current.path.slice(-1);
      acc[dimKey] = acc[dimKey] || {
        dimension: current.dimension,
        path: current.path.slice(-1),
        columns: [],
      };
      acc[dimKey].columns.push(current.column);
      return acc;
    }, {});
  const dimensionsRenderer = grouped =>
    Object.entries(grouped).map(([dimKey, dimValue]) => {
      if (Array.isArray(dimValue.columns)) {
        const attributes = dimValue.columns.map(col => {
          return <span className={'badge white_badge'}>{col}</span>;
        });
        return (
          <div className={'custom-node-subheader node_type__' + data.type}>
            <div className="custom-node-port">
              <a href={`/nodes/${dimValue.dimension}`}>{dimValue.dimension}</a>{' '}
              <div className={'badge node_type__metric text-black'}>
                {dimValue.path}
              </div>
            </div>
            <div className={'dimension_attributes'}>{attributes}</div>
          </div>
        );
      }
      return <></>;
    });
  return (
    <>
      {dimensions.length <= 0
        ? ''
        : dimensionsRenderer(groupedDimensions(dimensionsToObject(dimensions)))}
    </>
  );
}
