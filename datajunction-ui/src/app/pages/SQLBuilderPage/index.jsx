import { useContext, useEffect, useState } from 'react';
import NamespaceHeader from '../../components/NamespaceHeader';
import { DataJunctionAPI } from '../../services/DJService';
import DJClientContext from '../../providers/djclient';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import Select from 'react-select';
import QueryInfo from '../../components/QueryInfo';
import 'react-querybuilder/dist/query-builder.scss';
import QueryBuilder, { formatQuery } from 'react-querybuilder';
import 'styles/styles.scss';

export function SQLBuilderPage() {
  const DEFAULT_NUM_ROWS = 100;
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const validator = ruleType => !!ruleType.value;
  const [stagedMetrics, setStagedMetrics] = useState([]);
  const [metrics, setMetrics] = useState([]);
  const [commonDimensionsList, setCommonDimensionsList] = useState([]);
  const [selectedDimensions, setSelectedDimensions] = useState([]);
  const [stagedDimensions, setStagedDimensions] = useState([]);
  const [selectedMetrics, setSelectedMetrics] = useState([]);
  const [query, setQuery] = useState('');
  const [fields, setFields] = useState([]);
  const [filters, setFilters] = useState({ combinator: 'and', rules: [] });
  const [queryInfo, setQueryInfo] = useState({});
  const [data, setData] = useState(null);
  const [loadingData, setLoadingData] = useState(false);
  const [viewData, setViewData] = useState(false);
  const [showNumRows, setShowNumRows] = useState(DEFAULT_NUM_ROWS);
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
    setQueryInfo({});
    const fetchData = async () => {
      if (process.env.REACT_USE_SSE) {
        const sse = await djClient.stream(
          selectedMetrics,
          selectedDimensions,
          formatQuery(filters, { format: 'sql', parseNumbers: true }),
        );
        sse.onmessage = e => {
          const messageData = JSON.parse(JSON.parse(e.data));
          setQueryInfo(messageData);
          if (messageData.results) {
            setLoadingData(false);
            setData(messageData.results);
            messageData.numRows = messageData.results?.length
              ? messageData.results[0].rows.length
              : [];
            setViewData(true);
            setShowNumRows(DEFAULT_NUM_ROWS);
          }
        };
        sse.onerror = () => sse.close();
      } else {
        const response = await djClient.data(
          selectedMetrics,
          selectedDimensions,
        );
        setQueryInfo(response);
        if (response.results) {
          setLoadingData(false);
          setData(response.results);
          response.numRows = response.results?.length
            ? response.results[0].rows.length
            : [];
          setViewData(true);
          setShowNumRows(DEFAULT_NUM_ROWS);
        }
      }
    };
    fetchData().catch(console.error);
  };

  const resetView = () => {
    setQuery('');
    setData(null);
    setViewData(false);
    setQueryInfo({});
  };

  // Get metrics
  useEffect(() => {
    const fetchData = async () => {
      const metrics = await djClient.metrics();
      setMetrics(metrics.map(m => ({ value: m, label: m })));
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.metrics]);

  const attributeToFormInput = dimension => {
    const attribute = {
      name: dimension.name,
      label: `${dimension.name} (via ${dimension.path.join(' ▶ ')})`,
      placeholder: `from ${dimension.path}`,
      defaultOperator: '=',
      validator,
    };
    if (dimension.type === 'bool') {
      attribute.valueEditorType = 'checkbox';
    }
    if (dimension.type === 'timestamp') {
      attribute.inputType = 'datetime-local';
      attribute.defaultOperator = 'between';
    }
    return [dimension.name, attribute];
  };

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
        const uniqueFields = Object.fromEntries(
          new Map(
            commonDimensions.map(dimension => attributeToFormInput(dimension)),
          ),
        );
        setFields(Object.keys(uniqueFields).map(f => uniqueFields[f]));
      } else {
        setCommonDimensionsList([]);
        setFields([]);
      }
    };
    fetchData().catch(console.error);
  }, [selectedMetrics, djClient]);

  // Get SQL
  useEffect(() => {
    const fetchData = async () => {
      if (
        selectedMetrics.length > 0 &&
        (selectedDimensions.length > 0 || filters.rules.length > 0)
      ) {
        const result = await djClient.sqls(
          selectedMetrics,
          selectedDimensions,
          formatQuery(filters, { format: 'sql', parseNumbers: true }),
        );
        setQuery(result.sql);
      } else {
        resetView();
      }
    };
    fetchData().catch(console.error);
  }, [djClient, filters, selectedDimensions, selectedMetrics]);

  // Set number of rows to display
  useEffect(() => {
    if (data) {
      setDisplayedRows(
        data[0]?.rows.slice(0, showNumRows).map((rowData, index) => (
          <tr key={`data-row:${index}`}>
            {rowData.map((rowValue, colIndex) => (
              <td key={`${index}-${colIndex}`}>{rowValue}</td>
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
            <span data-testid="select-metrics">
              <Select
                name="metrics"
                options={metrics}
                isDisabled={
                  !!(selectedMetrics.length && selectedDimensions.length)
                }
                noOptionsMessage={() => 'No metrics found.'}
                placeholder={`${metrics.length} Available Metrics`}
                isMulti
                isClearable
                closeMenuOnSelect={false}
                onChange={e => {
                  setSelectedDimensions([]);
                  resetView();
                  setStagedMetrics(e.map(m => m.value));
                  setSelectedMetrics(stagedMetrics);
                }}
                onMenuClose={() => {
                  resetView();
                  setSelectedDimensions([]);
                  setSelectedMetrics(stagedMetrics);
                }}
              />
            </span>
            <h4>Group By</h4>
            <span data-testid="select-dimensions">
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
                onChange={e => {
                  resetView();
                  setStagedDimensions(e.map(d => d.value));
                  setSelectedDimensions(stagedDimensions);
                }}
                onMenuClose={() => {
                  setSelectedDimensions(stagedDimensions);
                }}
              />
            </span>
            <h4>Filter By</h4>
            <QueryBuilder
              fields={fields}
              query={filters}
              onQueryChange={q => setFilters(q)}
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
                  <span
                    className="button-3 executing-button"
                    onClick={getData}
                    role="button"
                    aria-label="RunQuery"
                    aria-hidden="false"
                  >
                    {'Running Query'}
                  </span>
                ) : (
                  <span
                    className="button-3 execute-button"
                    onClick={getData}
                    role="button"
                    aria-label="RunQuery"
                    aria-hidden="false"
                  >
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
            {queryInfo && queryInfo.id ? <QueryInfo {...queryInfo} /> : <></>}
            <div>
              {query && !viewData ? (
                <SyntaxHighlighter language="sql" style={foundation}>
                  {query}
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
