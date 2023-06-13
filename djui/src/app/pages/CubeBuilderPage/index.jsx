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
  const [expandQuery, setExpandQuery] = useState(false);

  const newWindowLinkIcon = (
    <svg class="svg-icon" viewBox="0 0 20 20" width="20" height="20">
      <path d="M16.198,10.896c-0.252,0-0.455,0.203-0.455,0.455v2.396c0,0.626-0.511,1.137-1.138,1.137H5.117c-0.627,0-1.138-0.511-1.138-1.137V7.852c0-0.626,0.511-1.137,1.138-1.137h5.315c0.252,0,0.456-0.203,0.456-0.455c0-0.251-0.204-0.455-0.456-0.455H5.117c-1.129,0-2.049,0.918-2.049,2.047v5.894c0,1.129,0.92,2.048,2.049,2.048h9.488c1.129,0,2.048-0.919,2.048-2.048v-2.396C16.653,11.099,16.45,10.896,16.198,10.896z"></path>
      <path d="M14.053,4.279c-0.207-0.135-0.492-0.079-0.63,0.133c-0.137,0.211-0.077,0.493,0.134,0.63l1.65,1.073c-4.115,0.62-5.705,4.891-5.774,5.082c-0.084,0.236,0.038,0.495,0.274,0.581c0.052,0.019,0.103,0.027,0.154,0.027c0.186,0,0.361-0.115,0.429-0.301c0.014-0.042,1.538-4.023,5.238-4.482l-1.172,1.799c-0.137,0.21-0.077,0.492,0.134,0.629c0.076,0.05,0.163,0.074,0.248,0.074c0.148,0,0.294-0.073,0.382-0.207l1.738-2.671c0.066-0.101,0.09-0.224,0.064-0.343c-0.025-0.118-0.096-0.221-0.197-0.287L14.053,4.279z"></path>
    </svg>
  );
  const dimensionNameFromAttribute = attribute => {
    return attribute
      .split('.')
      .slice(0, attribute.split('.').length - 1)
      .join('.');
  };

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
      const commonDimensions = await djClient.commonDimensions(selectedMetrics);
      console.log(commonDimensions);
      setCommonDimensionsList(commonDimensions);
    };
    fetchData().catch(console.error);
  }, [selectedMetrics]);

  // Get SQL
  useEffect(() => {
    const fetchData = async () => {
      const query = await djClient.sqls(selectedMetrics, selectedDimensions);
      setQuery(query.sql);
    };
    fetchData().catch(console.error);
  }, [selectedMetrics, selectedDimensions]);

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
                <div className="table-responsive builder-list">
                  {selectedMetrics.map(metric => (
                    <tr className="builder-list" key={metric}>
                      <td className="builder-list">
                        <span className="node_type__metric badge node_type">
                          {metric}
                        </span>
                      </td>
                      <td></td>
                    </tr>
                  ))}
                  {selectedDimensions.map(dimension => (
                    <tr className="builder-list" key={dimension}>
                      <td className="builder-list">
                        <span className="node_type__dimension badge node_type">
                          {dimension}
                        </span>
                      </td>
                      <td></td>
                    </tr>
                  ))}
                </div>
                <h4 className="mb-0 w-100">Available</h4>
                <div className="table-responsive">
                  {metrics.map(metric => (
                    <tr className="builder-list" key={metric}>
                      <td className="builder-list">
                        <a onClick={() => handleMetricAdd(metric)}>
                          <span className="node_type__metric badge rounded-pill node_type">
                            {metric}
                          </span>
                        </a>
                      </td>
                    </tr>
                  ))}
                  {commonDimensionsList.length ? (
                    <>
                      {commonDimensionsList.map(dimension => (
                        <tr className="builder-list" key={dimension}>
                          <td className="builder-list">
                            <span className="node_type__dimension badge node_type">
                              <a
                                onClick={() =>
                                  handleDimensionAdd(dimension.name)
                                }
                              >
                                {dimension.name}
                              </a>
                            </span>
                          </td>
                          <td></td>
                        </tr>
                      ))}
                    </>
                  ) : (
                    <></>
                  )}
                </div>
              </div>
            </div>
            <div className="flex-child">
              <div className="cube-builder">
                <h6 className="mb-0 w-100">
                  Query{' '}
                  {query ? (
                    <a className="pointer" onClick={toggleExpandQuery}>
                      {expandQuery ? '(Collapse)' : '(Expand)'}
                    </a>
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
