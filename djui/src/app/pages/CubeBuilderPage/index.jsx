import { useContext, useEffect, useState } from 'react';
import NamespaceHeader from '../../components/NamespaceHeader';
import { DataJunctionAPI } from '../../services/DJService';
import DJClientContext from '../../providers/djclient';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import { format } from 'sql-formatter';
import Select from 'react-select';

export function CubeBuilderPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [metrics, setMetrics] = useState([]);
  const [commonDimensionsList, setCommonDimensionsList] = useState([]);
  const [selectedDimensions, setSelectedDimensions] = useState([]);
  const [selectedMetrics, setSelectedMetrics] = useState([]);
  const [query, setQuery] = useState('');
  const [data, setData] = useState(null);
  const [loadingData, setLoadingData] = useState(false);
  const [viewData, setViewData] = useState(false);
  const [showHelp, setShowHelp] = useState(true);

  const toggleViewData = () => setViewData(current => !current);

  const getData = () => {
    setLoadingData(true);
    const fetchData = async () => {
      const data = await djClient.data(selectedMetrics, selectedDimensions);
      setLoadingData(false);
      setData(data);
      setViewData(true);
    };
    fetchData().catch(console.error);
  };

  const resetView = () => {
    setQuery('');
    setData(null);
    setViewData(false);
  };
  const handleMetricSelect = event => {
    const metrics = event.map(m => m.value);
    resetView();
    setSelectedMetrics(metrics);
  };

  const handleDimensionSelect = event => {
    const dimensions = event.map(d => d.value);
    resetView();
    setSelectedDimensions(dimensions);
  };

  // Get metrics
  useEffect(() => {
    const fetchData = async () => {
      const metrics = await djClient.metrics();
      setMetrics(metrics.map(m => ({ value: m, label: m })));
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
        setCommonDimensionsList(
          commonDimensions.map(d => ({ value: d.name, label: d.name })),
        );
      } else {
        setCommonDimensionsList([]);
      }
    };
    fetchData().catch(console.error);
  }, [selectedMetrics, djClient]);

  // Get SQL
  useEffect(() => {
    const fetchData = async () => {
      if (selectedMetrics.length && selectedDimensions.length) {
        const query = await djClient.sqls(selectedMetrics, selectedDimensions);
        setShowHelp(false);
        setQuery(query.sql);
      } else {
        resetView();
        setShowHelp(true);
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
          <div className="card-header">
            <h4>Metrics</h4>
            <Select
              name="metrics"
              options={metrics}
              isMulti
              isClearable
              onChange={handleMetricSelect}
            />
            <h4>Shared Dimensions</h4>

            <Select
              name="dimensions"
              options={commonDimensionsList}
              isMulti
              isClearable
              onChange={handleDimensionSelect}
            />
          </div>

          <div className="card-header">
            {showHelp ? (
              <div className="card-light-shadow">
                <h6>Using the Cube Builder</h6>
                <span>
                  Welcome to the DataJunction cube builder! To build a cube,
                  first select a set of one or more metrics you are interested
                  in. As you select metrics, the shared dimensions list will
                  updated to only include dimensions that are shared by the
                  currently selected metrics. If you've selected metrics but do
                  not see any dimensions, that means those metrics do not share
                  any dimensions.

                  To read more about how DJ discovers dimensions, see the
                  <a href="https://main--super-gumption-22192f.netlify.app/docs/dj-concepts/dimension-discovery/">
                    Dimension Discovery
                  </a>
                   concept page.
                </span>
              </div>
            ) : (
              <></>
            )}
            {query ? (
              <h6>
                {loadingData ? (
                  <span className="button-3 executing-button">
                    {'Running Query'}
                  </span>
                ) : (
                  <span className="button-3 execute-button" onClick={getData}>
                    {'Run Query'}
                  </span>
                )}

                {data ? (
                  viewData ? (
                    <span
                      className="button-3 neutral-button"
                      onClick={toggleViewData}
                    >
                      {'View Query'}
                    </span>
                  ) : (
                    <span
                      className="button-3 neutral-button"
                      onClick={toggleViewData}
                    >
                      {'View Data'}
                    </span>
                  )
                ) : (
                  <></>
                )}
              </h6>
            ) : (
              <></>
            )}

            <div>
              {query && !viewData ? (
                <SyntaxHighlighter language="sql" style={foundation}>
                  {format(query, {
                    language: 'spark',
                    tabWidth: 2,
                    keywordCase: 'upper',
                    denseOperators: true,
                    logicalOperatorNewline: 'before',
                    expressionWidth: 10,
                  })}
                </SyntaxHighlighter>
              ) : (
                ''
              )}
            </div>
            {data && viewData ? (
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
    </>
  );
}

CubeBuilderPage.defaultProps = {
  djClient: DataJunctionAPI,
};
