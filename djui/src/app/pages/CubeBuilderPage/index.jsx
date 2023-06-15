import * as React from 'react';
import { useContext, useEffect, useState } from 'react';
import NamespaceHeader from '../../components/NamespaceHeader';
import { DataJunctionAPI } from '../../services/DJService';
import DJClientContext from '../../providers/djclient';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import { format } from 'sql-formatter';

export function CubeBuilderPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [metrics, setMetrics] = useState([]);
  const [selectedDimensions, setSelectedDimensions] = useState([]);
  const [selectedMetrics, setSelectedMetrics] = useState([]);
  const [commonDimensionsList, setCommonDimensionsList] = useState([]);
  const [query, setQuery] = useState('');
  const [data, setData] = useState({});
  const [loadingData, setLoadingData] = useState(false);
  const [expandQuery, setExpandQuery] = useState(false);

  const toggleExpandQuery = () => setExpandQuery(current => !current);

  const handleMetricAdd = metricName => {
    if (selectedMetrics.indexOf(metricName) === -1) {
      setSelectedMetrics([...selectedMetrics, metricName]);
    }
    setMetrics(metrics.filter(m => m !== metricName));
    setSelectedDimensions([]);
  };

  const handleDimensionAdd = dimensionName => {
    setCommonDimensionsList(
      commonDimensionsList.filter(d => d !== dimensionName),
    );
    setSelectedDimensions([...selectedDimensions, dimensionName]);
  };

  const handleMetricRemove = metricName => {
    setSelectedMetrics(selectedMetrics.filter(m => m !== metricName));
    setSelectedDimensions([]);
    setQuery('');
    setMetrics([...metrics, metricName]);
  };

  const handleDimensionRemove = dimensionName => {
    setSelectedDimensions(selectedDimensions.filter(d => d !== dimensionName));
    setCommonDimensionsList([...commonDimensionsList, dimensionName]);
  };

  const getData = () => {
    setLoadingData(true);
    const fetchData = async () => {
      const data = await djClient.data(selectedMetrics, selectedDimensions);
      setLoadingData(false);
      setData(data);
    };
    fetchData().catch(console.error);
  };

  // Get metrics
  useEffect(() => {
    const fetchData = async () => {
      const metrics = await djClient.metrics();
      setMetrics(metrics);
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.metrics]);

  // Get common dimensions
  useEffect(() => {
    const fetchData = async () => {
      if (selectedMetrics.length) {
        const commonDimensions = await djClient.commonDimensions(
          selectedMetrics,
        );
        setCommonDimensionsList(commonDimensions.map(d => d.name));
      }
    };
    fetchData().catch(console.error);
  }, [selectedMetrics, djClient]);

  // Get SQL
  useEffect(() => {
    const fetchData = async () => {
      if (selectedMetrics.length && selectedDimensions.length) {
        const query = await djClient.sqls(selectedMetrics, selectedDimensions);
        setQuery(query.sql);
      }
    };
    fetchData().catch(console.error);
  }, [selectedMetrics, selectedDimensions, djClient]);

  // @ts-ignore
  return (
    <>
      <div className="mid">
        <NamespaceHeader namespace="" />
        <div className="card">
          <div className="card-header flex-container">
            <div className="flex-child raw-node-list">
              <div className="cube-builder">
                <h2>Metrics & Dimensions</h2>
                <h4>Selected</h4>
                <table>
                  <tbody className="table-responsive builder-list">
                    {selectedMetrics.sort().map(metric => (
                      <tr className="builder-list" key={`${metric}:selected`}>
                        <td className="builder-list">
                          <span
                            onClick={() => handleMetricRemove(metric)}
                            className="button-3 node_type__metric"
                          >
                            {metric}
                          </span>
                        </td>
                        <td></td>
                      </tr>
                    ))}
                    {selectedDimensions.sort().map(dimension => (
                      <tr
                        className="builder-list"
                        key={`${dimension}:selected`}
                      >
                        <td className="builder-list">
                          <span
                            onClick={() => handleDimensionRemove(dimension)}
                            className="button-3 node_type__dimension"
                          >
                            {dimension}
                          </span>
                        </td>
                        <td></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <h4>Available Metrics</h4>
                <table>
                  <tbody className="table-responsive">
                    {metrics.sort().map(metric => (
                      <tr className="builder-list" key={metric}>
                        <td className="builder-list">
                          <span
                            onClick={() => handleMetricAdd(metric)}
                            className="button-3 node_type__metric"
                          >
                            {metric}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {commonDimensionsList.length ? (
                  <>
                    <h4>Available Dimensions</h4>
                    <table>
                      <tbody className="table-responsive">
                        {commonDimensionsList.sort().map(dimension => (
                          <tr className="builder-list" key={dimension}>
                            <td className="builder-list">
                              <span
                                onClick={() => handleDimensionAdd(dimension)}
                                className="button-3 node_type__dimension"
                              >
                                {dimension}
                              </span>
                            </td>
                            <td></td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </>
                ) : (
                  <></>
                )}
              </div>
            </div>
            <div className="flex-child">
              <div className="cube-builder">
                {query ? (
                  <h6>
                    {loadingData ? (
                      <span className="button-3 executing-button">
                        {'Running Query'}
                      </span>
                    ) : (
                      <span
                        className="button-3 execute-button"
                        onClick={getData}
                      >
                        {'Run Query'}
                      </span>
                    )}
                    <span onClick={toggleExpandQuery}>
                      {expandQuery ? ' (Collapse)' : ' (Expand)'}
                    </span>
                  </h6>
                ) : (
                  <></>
                )}
                <div className={expandQuery ? '' : 'builder-view-box'}>
                  <SyntaxHighlighter language="sql" style={foundation}>
                    {format(
                      query
                        ? query
                        : '/* Select from available metrics and dimensions */',
                      {
                        language: 'spark',
                        tabWidth: 2,
                        keywordCase: 'upper',
                        denseOperators: true,
                        logicalOperatorNewline: 'before',
                        expressionWidth: 10,
                      },
                    )}
                  </SyntaxHighlighter>
                </div>
                {data ? (
                  data.state === 'FINISHED' ? (
                    <>
                      <h4>Results</h4>
                      <div className="table-responsive builder-view-box">
                        <table className="card-inner-table table">
                          <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
                            <tr>
                              {data.results[0].columns.map(columnName => (
                                <th key={columnName.name}>{columnName.name}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {data.results[0].rows.map((rowData, index) => (
                              <tr key={`data-row:${index}`}>
                                {rowData.map(rowValue => (
                                  <td key={rowValue}>{rowValue}</td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </>
                  ) : data.state ? (
                    <div>
                      {`Ran into an issue while running the query. (Query State: ${data.state}, Errors: ${data.errors})`}
                    </div>
                  ) : (
                    <></>
                  )
                ) : (
                  <></>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

CubeBuilderPage.defaultProps = {
  djClient: DataJunctionAPI,
};
