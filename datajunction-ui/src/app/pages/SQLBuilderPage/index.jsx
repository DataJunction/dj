import { useContext, useEffect, useState } from 'react';
import NamespaceHeader from '../../components/NamespaceHeader';
import { DataJunctionAPI } from '../../services/DJService';
import DJClientContext from '../../providers/djclient';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import { format } from 'sql-formatter';
import Select from 'react-select';
import QueryInfo from '../../components/QueryInfo';

export function SQLBuilderPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [metrics, setMetrics] = useState([]);
  const [commonDimensionsList, setCommonDimensionsList] = useState([]);
  const [selectedDimensions, setSelectedDimensions] = useState([]);
  const [selectedMetrics, setSelectedMetrics] = useState([]);
  const [query, setQuery] = useState('');
  const [submittedQueryInfo, setSubmittedQueryInfo] = useState(null);
  const [data, setData] = useState(null);
  const [loadingData, setLoadingData] = useState(false);
  const [viewData, setViewData] = useState(false);
  const [showHelp, setShowHelp] = useState(true);
  const toggleViewData = () => setViewData(current => !current);

  const getData = () => {
    setLoadingData(true);
    const fetchData = async () => {
      setData(null);
      const queryInfo = await djClient.data(
        selectedMetrics,
        selectedDimensions,
      );
      setLoadingData(false);
      setSubmittedQueryInfo(queryInfo);
      if (queryInfo.results && queryInfo.results?.length) {
        setData(queryInfo.results);
        setViewData(true);
      }
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
              noOptionsMessage={() => 'No metrics found.'}
              placeholder={`${metrics.length} Available Metrics`}
              isMulti
              isClearable
              closeMenuOnSelect={false}
              onChange={handleMetricSelect}
            />
            <h4>Shared Dimensions</h4>
            <Select
              name="dimensions"
              options={commonDimensionsList}
              noOptionsMessage={() =>
                'No shared dimensions found. Try selecting different metrics.'
              }
              placeholder={`${commonDimensionsList.length} Shared Dimensions`}
              isMulti
              isClearable
              closeMenuOnSelect={false}
              onChange={handleDimensionSelect}
            />
          </div>
          <div className="card-header">
            {showHelp ? (
              <div className="card-light-shadow">
                <h6>Using the SQL Builder</h6>
                <p>
                  The sql builder allows you to group multiple metrics along
                  with their shared dimensions. Using your selections,
                  DataJunction will generate the corresponding SQL.
                </p>
                <ol>
                  <li>
                    <b>Select Metrics:</b> Start by selecting one or more
                    metrics from the metrics dropdown.
                  </li>
                  <li>
                    <b>Select Dimensions:</b> Next, select the dimension
                    attributes you would like to include. As you select
                    additional metrics, the list of available dimensions will be
                    filtered to those shared by the selected metrics. If the
                    dimensions list is empty, no shared dimensions were
                    discovered.
                  </li>
                  <li>
                    <b>View the generated SQL Query:</b> As you make your
                    selections, the SQL required to retrieve the set of metrics
                    and dimensions will be generated below.
                  </li>
                  <li>
                    <b>Run the Query:</b> If query running is enabled by your
                    server, you can also run the generated SQL query to view a
                    sample of 100 records.
                  </li>
                </ol>
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
            {submittedQueryInfo ? <QueryInfo {...submittedQueryInfo} /> : <></>}
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
              <div className="table-responsive">
                <table className="card-inner-table table">
                  <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
                    <tr>
                      {data[0].columns.map(columnName => (
                        <th key={columnName.name}>{columnName.name}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data[0].rows.map((rowData, index) => (
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
              <></>
            )}
          </div>
        </div>
      </div>
    </>
  );
}

SQLBuilderPage.defaultProps = {
  djClient: DataJunctionAPI,
};
