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
  const [stagedMetrics, setStagedMetrics] = useState([]);
  const [metrics, setMetrics] = useState([]);
  const [commonDimensionsList, setCommonDimensionsList] = useState([]);
  const [selectedDimensions, setSelectedDimensions] = useState([]);
  const [stagedDimensions, setStagedDimensions] = useState([]);
  const [selectedMetrics, setSelectedMetrics] = useState([]);
  const [query, setQuery] = useState('');
  const [queryInfo, setQueryInfo] = useState({});
  const [data, setData] = useState(null);
  const [loadingData, setLoadingData] = useState(false);
  const [viewData, setViewData] = useState(false);
  const [showNumRows, setShowNumRows] = useState(100);
  const [displayedRows, setDisplayedRows] = useState(<></>);
  const numRowsOptions = [
    {
      value: 10,
      label: '10 Rows',
      isFixed: true,
    },
    {
      value: 100,
      label: '100 Rows',
      isFixed: true,
    },
    {
      value: 1000,
      label: '1,000 Rows',
      isFixed: true,
    },
  ];
  const toggleViewData = () => setViewData(current => !current);

  // Get data for the current selection of metrics and dimensions
  const getData = () => {
    setLoadingData(true);
    setQueryInfo({})
    const fetchData = async () => {
      // setData(null);
      const sse = await djClient.stream(selectedMetrics, selectedDimensions);
      sse.onmessage = e => {
        const messageData = JSON.parse(JSON.parse(e.data));
        setQueryInfo(messageData);
        if (messageData.results) {
          setLoadingData(false);
          setQueryInfo(messageData);
          setData(messageData.results);
          messageData.numRows = messageData.results?.length ? messageData.results[0].rows.length : []
          setViewData(true);
          setShowNumRows(10);
        }
      };
      sse.onerror = () => sse.close();
    };
    fetchData().catch(console.error);
  };

  const resetView = () => {
    setQuery('');
    setData(null);
    setViewData(false);
    setQueryInfo({})
  };
  const handleMetricSelect = event => {
    const metrics = event.map(m => m.value);
    resetView();
    setStagedMetrics(metrics);
  };

  const handleMetricSelectorClose = () => {
    resetView();
    setSelectedMetrics(stagedMetrics);
  };

  const handleDimensionSelect = event => {
    const dimensions = event.map(d => d.value);
    resetView();
    setStagedDimensions(dimensions);
  };

  const handleDimensionSelectorClose = () => {
    setSelectedDimensions(stagedDimensions);
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
          commonDimensions.map(d => ({
            value: d.name,
            label: d.name,
            path: d.path.join(' ▶ '),
          })),
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
        setQuery(query.sql);
      } else {
        resetView();
      }
    };
    fetchData().catch(console.error);
  }, [selectedMetrics, selectedDimensions, djClient]);

  // Set number of rows to display
  useEffect(() => {
    if (data) {
      setDisplayedRows(
        data[0]?.rows.slice(0, showNumRows).map((rowData, index) => (
          <tr key={`data-row:${index}`}>
            {rowData.map(rowValue => (
              <td key={rowValue}>{rowValue}</td>
            ))}
          </tr>
        )),
      );
    }
  }, [showNumRows, data]);
  const formatOptionLabel = ({ value, label, path }) => (
    <div className={`badge dimension_option`}>
      <div>{label}</div>
      <span className={`badge dimension_option_subheading`}>{path}</span>
    </div>
  );

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
              onMenuClose={handleMetricSelectorClose}
            />
            <h4>Group By</h4>
            <Select
              name="dimensions"
              formatOptionLabel={formatOptionLabel}
              options={commonDimensionsList}
              noOptionsMessage={() =>
                'No shared dimensions found. Try selecting different metrics.'
              }
              placeholder={`${commonDimensionsList.length} Shared Dimensions`}
              isMulti
              isClearable
              closeMenuOnSelect={false}
              onChange={handleDimensionSelect}
              onMenuClose={handleDimensionSelectorClose}
            />
          </div>
          <div className="card-header">
            {!viewData && !query ? (
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
              <>
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
                    <>
                      <span
                        className="button-3 neutral-button"
                        onClick={toggleViewData}
                      >
                        {'View Query'}
                      </span>
                      <span style={{ display: 'inline-block' }}>
                        <Select
                          name="num-rows"
                          defaultValue={numRowsOptions[0]}
                          options={numRowsOptions}
                          onChange={e => setShowNumRows(e.value)}
                        />
                      </span>
                    </>
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
              </>
            ) : (
              <></>
            )}
            {queryInfo && queryInfo.id? <QueryInfo {...queryInfo} /> : <></>}
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
                      {data[0]?.columns.map(columnName => (
                        <th key={columnName.name}>{columnName.name}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>{displayedRows}</tbody>
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
