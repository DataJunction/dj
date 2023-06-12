import * as React from 'react';
import { useContext, useEffect, useState } from 'react';
import NamespaceHeader from '../../components/NamespaceHeader';
import { DataJunctionAPI } from '../../services/DJService';
import DJClientContext from '../../providers/djclient';
// const datajunction = require('datajunction');
// const dj = new datajunction.DJClient('http://localhost:8000');

export function CubeBuilderPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [metrics, setMetrics] = useState([]);
  const [selectedMetrics, setSelectedMetrics] = useState([]);
  const [commonDimensionsList, setCommonDimensionsList] = useState([]);
  const handleMetricAdd = metricName => {
    if (selectedMetrics.indexOf(metricName) === -1) {
      setSelectedMetrics([...selectedMetrics, metricName]);
    }
    setMetrics(metrics.filter(m => m !== metricName));
    const fetchData = async () => {
      const commonDimensions = await djClient.commonDimensions(selectedMetrics);
      console.log(commonDimensions);
      setCommonDimensionsList(commonDimensions);
    };
    fetchData().catch(console.error);
  };

  useEffect(() => {
    const fetchData = async () => {
      const metrics = await djClient.metrics();
      setMetrics(metrics);
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.metrics]);

  // @ts-ignore
  return (
    <>
      <div className="mid">
        <NamespaceHeader namespace="" />
        <div className="card">
          <div className="card-header">
            <h2>Cube Builder</h2>
            <div className="table-responsive">
              <table className="card-table table">
                <thead>
                  <th>Cube</th>
                </thead>
                {selectedMetrics.map(metric => (
                  <tr key={metric}>
                    <td>
                      <a href={`/nodes/${metric}`}>
                        <span className="node_type__metric badge node_type">
                          {metric}
                        </span>
                      </a>{' '}
                      <a onClick={() => handleMetricAdd(metric)}>add</a>
                    </td>
                    <td></td>
                  </tr>
                ))}
              </table>
            </div>
            <h3>Select Metrics</h3>
            <div className="table-responsive">
              <table className="card-table table">
                <thead>
                  <th>Metric</th>
                </thead>
                {metrics.map(metric => (
                  <tr key={metric}>
                    <td>
                      <span className="node_type__metric badge node_type">
                        <a onClick={() => handleMetricAdd(metric)}>{metric}</a>
                        <a href={`/nodes/${metric}`}>{'>'}</a>
                      </span>
                    </td>
                    <td></td>
                  </tr>
                ))}
              </table>
            </div>
            {commonDimensionsList.length ? (
              <>
                <h3>Select Dimensions</h3>
                <table className="card-table table">
                  <thead>
                    <th>Dimension Attribute</th>
                    <th>Type</th>
                  </thead>
                  {commonDimensionsList.map(dimension => (
                    <tr key={dimension}>
                      <td>
                        <span className="node_type__dimension badge node_type">
                          <a onClick={() => handleDimensionAdd(dimension.name)}>
                            {dimension.name}
                          </a>
                          <a href={`/nodes/${dimension.name}`}>{'>'}</a>
                        </span>
                        <span className="column_type badge node_type">
                          {dimension.type}
                        </span>
                      </td>
                      <td></td>
                    </tr>
                  ))}
                </table>
              </>
            ) : (
              <></>
            )}
          </div>
        </div>
      </div>
    </>
  );
}

CubeBuilderPage.defaultProps = {
  djClient: DataJunctionAPI,
};
