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
  };

  const handleDimensionAdd = dimensionName => {
    if (selectedDimensions.indexOf(dimensionName) === -1) {
      setSelectedDimensions([...selectedDimensions, dimensionName]);
    }
    setCommonDimensionsList(
      commonDimensionsList.filter(d => d.name !== dimensionName),
    );
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
        console.log(commonDimensions);
        setCommonDimensionsList(commonDimensions);
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
                <h6 className="mb-0 w-100">Metrics & Dimensions</h6>
                <h4 className="mb-0 w-100">Selected</h4>
                <table>
                  <tbody className="table-responsive builder-list">
                    {selectedMetrics.map(metric => (
                      <tr className="builder-list" key={`${metric}:selected`}>
                        <td className="builder-list">
                          <span className="node_type__metric badge node_type">
                            {metric}
                          </span>
                        </td>
                        <td></td>
                      </tr>
                    ))}
                    {selectedDimensions.map(dimension => (
                      <tr
                        className="builder-list"
                        key={`${dimension}:selected`}
                      >
                        <td className="builder-list">
                          <span className="node_type__dimension badge node_type">
                            {dimension}
                          </span>
                        </td>
                        <td></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <h4 className="mb-0 w-100">Available</h4>
                <table>
                  <tbody className="table-responsive">
                    {metrics.map(metric => (
                      <tr className="builder-list" key={metric}>
                        <td className="builder-list">
                          <span
                            onClick={() => handleMetricAdd(metric)}
                            className="node_type__metric badge rounded-pill node_type"
                          >
                            {metric}
                          </span>
                        </td>
                      </tr>
                    ))}
                    {commonDimensionsList.length ? (
                      commonDimensionsList.map(dimension => (
                        <tr className="builder-list" key={dimension.name}>
                          <td className="builder-list">
                            <span
                              onClick={() => handleDimensionAdd(dimension.name)}
                              className="node_type__dimension badge node_type"
                            >
                              {dimension.name}
                            </span>
                          </td>
                          <td></td>
                        </tr>
                      ))
                    ) : (
                      <></>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
            <div className="flex-child">
              <div className="cube-builder">
                <h6 className="mb-0 w-100">
                  {loadingData ? (
                    <span>{'Running Query'}</span>
                  ) : (
                    <span className="pointer" onClick={getData}>
                      {'Run Query'}
                    </span>
                  )}
                  {query ? (
                    <>
                      <div className="pointer" onClick={toggleExpandQuery}>
                        {expandQuery ? '(Collapse)' : '(Expand)'}
                      </div>
                    </>
                  ) : (
                    <></>
                  )}
                </h6>
                <div className={expandQuery ? '' : 'sql-query-view'}>
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
                    <div className="table-responsive">
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
                  ) : (
                    ''
                  )
                ) : (
                  <div>
                    {`Ran into an issue while running the query. (Query State: ${
                      data.state
                    }, Errors: ${console.log(data)})`}
                  </div>
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
