import React, { useEffect, useState } from 'react';
import Select from 'react-select';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import { labelize } from '../../../utils/form';
import { Form, Formik } from 'formik';
import DimensionFilter from './DimensionFilter';
import QueryInfo from '../../components/QueryInfo';
import LoadingIcon from '../../icons/LoadingIcon';

export default function NodeValidateTab({ node, djClient }) {
  const [query, setQuery] = useState('');
  const [lookup, setLookup] = useState([]);
  const [running, setRunning] = useState(false);

  // These are a list of dimensions that are available for this node
  const [dimensions, setDimensions] = useState([]);

  // A separate structure used to store the selected dimensions to filter by and their values
  const [selectedFilters, setSelectedFilters] = useState({});

  // The set of dimensions and filters to pass to the /sql or /data endpoints for the node
  const [selection, setSelection] = useState({
    dimensions: [],
    filters: [],
  });

  // Any query result info retrieved when a node query is run
  const [queryInfo, setQueryInfo] = useState(null);

  const initialValues = {};

  const [state, setState] = useState({
    selectedTab: 'results',
  });

  const switchTab = tabName => {
    setState({ selectedTab: tabName });
  };

  useEffect(() => {
    const fetchData = async () => {
      if (node) {
        // Find all the dimensions for this node
        const dimensions = await djClient.nodeDimensions(node.name);

        // Create a dimensions lookup object
        const lookup = {};
        dimensions.forEach(dimension => {
          lookup[dimension.name] = dimension;
        });
        setLookup(lookup);

        // Group the dimensions by dimension node
        const grouped = Object.entries(
          dimensions.reduce((group, dimension) => {
            group[dimension.node_name + dimension.path] =
              group[dimension.node_name + dimension.path] ?? [];
            group[dimension.node_name + dimension.path].push(dimension);
            return group;
          }, {}),
        );
        setDimensions(grouped);

        // Build the query for this node based on the user-provided dimensions and filters
        const query = await djClient.sql(node.name, selection);
        setQuery(query.sql);
      }
    };
    fetchData().catch(console.error);
  }, [djClient, node, selection]);

  const dimensionsList = dimensions.flatMap(grouping => {
    const dimensionsInGroup = grouping[1];
    return dimensionsInGroup
      .filter(dim => dim.properties.includes('primary_key') === true)
      .map(dim => {
        return {
          value: dim.name,
          label: (
            <span>
              {labelize(dim.name.split('.').slice(-1)[0])}{' '}
              <small>{dim.name}</small>
            </span>
          ),
        };
      });
  });

  // Run the query and use SSE to stream the status of the query execution results
  const runQuery = async (values, setStatus, setSubmitting) => {
    setRunning(true);
    const sse = await djClient.streamNodeData(node?.name, selection);
    sse.onmessage = e => {
      const messageData = JSON.parse(JSON.parse(e.data));
      if (
        messageData !== null &&
        messageData?.state !== 'FINISHED' &&
        messageData?.state !== 'CANCELED' &&
        messageData?.state !== 'FAILED'
      ) {
        setRunning(false);
      }
      if (messageData.results && messageData.results?.length > 0) {
        messageData.numRows = messageData.results?.length
          ? messageData.results[0].rows.length
          : [];
        switchTab('results');
        setRunning(false);
      } else {
        switchTab('info');
      }
      setQueryInfo(messageData);
    };
    sse.onerror = () => sse.close();
  };

  // Handle form submission (runs the query)
  const handleSubmit = async (values, { setSubmitting, setStatus }) => {
    await runQuery(values, setStatus, setSubmitting);
  };

  // Handle when filter values are updated. This is available for all nodes.
  const handleAddFilters = event => {
    const updatedFilters = selectedFilters;
    if (event.dimension in updatedFilters) {
      updatedFilters[event.dimension].operator = event.operator;
      updatedFilters[event.dimension].values = event.values;
    } else {
      updatedFilters[event.dimension] = {
        operator: event.operator,
        values: event.values,
      };
    }
    setSelectedFilters(updatedFilters);
    const updatedDimensions = selection.dimensions.concat([event.dimension]);
    setSelection({
      filters: Object.entries(updatedFilters).map(obj =>
        obj[1].values
          ? `${obj[0]} IN (${obj[1].values
              .map(val =>
                ['int', 'bigint', 'float', 'double', 'long'].includes(
                  lookup[obj[0]].type,
                )
                  ? val.value
                  : "'" + val.value + "'",
              )
              .join(', ')})`
          : '',
      ),
      dimensions: updatedDimensions,
    });
  };

  // Handle when one or more dimensions are selected from the dropdown
  // Note that this is only available to metrics
  const handleAddDimensions = event => {
    const updatedDimensions = event.map(
      selectedDimension => selectedDimension.value,
    );
    setSelection({
      filters: selection.filters,
      dimensions: updatedDimensions,
    });
  };

  const filters = dimensions.map(grouping => {
    const dimensionsInGroup = grouping[1];
    const dimensionGroupOptions = dimensionsInGroup
      .filter(dim => dim.properties.includes('primary_key') === true)
      .map(dim => {
        return {
          value: dim.name,
          label: labelize(dim.name.split('.').slice(-1)[0]),
          metadata: dim,
        };
      });
    return (
      <>
        <div className="dimensionsList">
          {dimensionGroupOptions.map(dimension => {
            return (
              <DimensionFilter
                dimension={dimension}
                onChange={handleAddFilters}
              />
            );
          })}
        </div>
      </>
    );
  });

  return (
    <Formik initialValues={initialValues} onSubmit={handleSubmit}>
      {function Render({ isSubmitting, status, setFieldValue }) {
        return (
          <Form>
            <div className={'queryrunner'}>
              <div className="queryrunner-filters left">
                {node?.type === 'metric' ? (
                  <>
                    <span>
                      <label>Group By</label>
                    </span>
                    <Select
                      name="dimensions"
                      options={dimensionsList}
                      isMulti
                      isClearable
                      onChange={handleAddDimensions}
                    />
                    <br />
                  </>
                ) : null}
                <span>
                  <label>Add Filters</label>
                </span>
                {filters}
              </div>
              <div className={'righttop'}>
                <label>Generated Query</label>
                <div
                  style={{
                    height: '200px',
                    width: '80%',
                    overflow: 'scroll',
                    borderRadius: '0',
                    border: '1px solid #ccc',
                  }}
                  className="queryrunner-query"
                >
                  <SyntaxHighlighter
                    language="sql"
                    style={foundation}
                    wrapLines={true}
                  >
                    {query}
                  </SyntaxHighlighter>
                </div>
                <button
                  type="submit"
                  disabled={
                    running ||
                    (queryInfo !== null &&
                      queryInfo?.state !== 'FINISHED' &&
                      queryInfo?.state !== 'CANCELED' &&
                      queryInfo?.state !== 'FAILED')
                  }
                  className="button-3 execute-button"
                  style={{ marginTop: '1rem' }}
                >
                  {isSubmitting || running === true ? <LoadingIcon /> : '► Run'}
                </button>
              </div>
              <div
                style={{
                  width: window.innerWidth * 0.8,
                  marginTop: '1rem',
                  marginLeft: '0.5rem',
                  height: 'calc(70% - 5.5px)',
                }}
                className={'rightbottom'}
              >
                <div
                  className={'align-items-center row'}
                  style={{
                    borderBottom: '1px solid #dddddd',
                    width: '86%',
                  }}
                >
                  <div
                    className={
                      'tab-item' +
                      (state.selectedTab === 'results' ? ' active' : '')
                    }
                    onClick={_ => switchTab('results')}
                  >
                    Results
                  </div>
                  <div
                    className={
                      'tab-item' +
                      (state.selectedTab === 'info' ? ' active' : '')
                    }
                    aria-label={'QueryInfo'}
                    role={'button'}
                    onClick={_ => switchTab('info')}
                  >
                    Info
                  </div>
                </div>
                {state.selectedTab === 'info' ? (
                  <div>
                    {queryInfo && queryInfo.id ? (
                      <QueryInfo {...queryInfo} isList={true} />
                    ) : (
                      <></>
                    )}
                  </div>
                ) : null}
                {state.selectedTab === 'results' ? (
                  <div>
                    {queryInfo !== null && queryInfo.state !== 'FINISHED' ? (
                      <div style={{ padding: '2rem' }}>
                        The query has status {queryInfo.state}! Check the INFO
                        tab for more details.
                      </div>
                    ) : queryInfo !== null &&
                      queryInfo.results !== null &&
                      queryInfo.results.length === 0 ? (
                      <div style={{ padding: '2rem' }}>
                        The query finished but output no results.
                      </div>
                    ) : queryInfo !== null &&
                      queryInfo.results !== null &&
                      queryInfo.results.length > 0 ? (
                      <div
                        className="table-responsive"
                        style={{
                          gridGap: '0',
                          width: '86%',
                          padding: '0',
                          maxHeight: '50%',
                        }}
                      >
                        <table
                          style={{ marginTop: '0 !important' }}
                          className="table"
                        >
                          <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
                            <tr>
                              {queryInfo.results[0]?.columns.map(columnName => (
                                <th key={columnName.name}>
                                  {columnName.column}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {queryInfo.results[0]?.rows
                              .slice(0, 100)
                              .map((rowData, index) => (
                                <tr key={`data-row:${index}`}>
                                  {rowData.map((rowValue, colIndex) => (
                                    <td key={`${index}-${colIndex}`}>{rowValue}</td>
                                  ))}
                                </tr>
                              ))}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <div style={{ padding: '2rem' }}>
                        Click "Run" to execute the query.
                      </div>
                    )}
                  </div>
                ) : null}
              </div>
            </div>
          </Form>
        );
      }}
    </Formik>
  );
}
