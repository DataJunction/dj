import React, { useEffect, useState } from 'react';
import Select from 'react-select';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import { labelize } from '../../../utils/form';
import { Form, Formik } from 'formik';
import DimensionFilter from './DimensionFilter';
import QueryInfo from '../../components/QueryInfo';
import LoadingIcon from '../../icons/LoadingIcon';

export default function NodeSQLTab({ node, djClient }) {
  const [query, setQuery] = useState('');
  const [lookup, setLookup] = useState([]);

  const [dimensions, setDimensions] = useState([]);
  const [selectedDimensions, setSelectedDimensions] = useState([]);
  const [selectedFilters, setSelectedFilters] = useState({});

  const [selection, setSelection] = useState({
    dimensions: [],
    filters: [],
  });

  const [queryInfo, setQueryInfo] = useState({});
  const [data, setData] = useState(null);
  const [displayedRows, setDisplayedRows] = useState(<></>);

  // Set number of rows to display
  useEffect(() => {
    if (data) {
      setDisplayedRows(
        data[0]?.rows.slice(0, 100).map((rowData, index) => (
          <tr key={`data-row:${index}`}>
            {rowData.map(rowValue => (
              <td key={rowValue}>{rowValue}</td>
            ))}
          </tr>
        )),
      );
    }
  }, [data]);

  // Get data for the current selection of metrics and dimensions
  const getData = () => {
    // setLoadingData(true);
    setQueryInfo({});
    const fetchData = async () => {
      const response = await djClient.nodeData(node.name, selection);
      setQueryInfo(response);
      console.log('respon!!!', response);
      if (response.results) {
        // setLoadingData(false);
        setData(response.results);
        response.numRows = response.results?.length
          ? response.results[0].rows.length
          : [];
        // setViewData(true);
        // setShowNumRows(DEFAULT_NUM_ROWS);
      }
    };
    fetchData().catch(console.error);
  };

  useEffect(() => {
    const fetchData = async () => {
      if (node) {
        const dimensions = await djClient.nodeDimensions(node.name);

        const lookup = {};
        dimensions.forEach(dimension => {
          lookup[dimension.name] = dimension;
        });
        setLookup(lookup);

        const grouped = Object.entries(
          dimensions.reduce((group, dimension) => {
            group[dimension.node_name + dimension.path] =
              group[dimension.node_name + dimension.path] ?? [];
            group[dimension.node_name + dimension.path].push(dimension);
            return group;
          }, {}),
        );
        setDimensions(grouped);

        console.log('selection', selection);
        const query = await djClient.sql(node.name, selection);
        setQuery(query.sql);
      }
    };
    fetchData().catch(console.error);
  }, [djClient, node, selection]);

  const dimensionsList = dimensions.flatMap(grouping => {
    const dimensionsInGroup = grouping[1];
    return dimensionsInGroup
      .filter(dim => dim.is_primary_key === true)
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

  const runQuery = async (values, setStatus) => {
    const response = await djClient.nodeData(node.name, selection);
    setQueryInfo(response);
    console.log('respon!!!', response);
    if (response.results) {
      // setLoadingData(false);
      setData(response.results);
      response.numRows = response.results?.length
        ? response.results[0].rows.length
        : [];
      // setViewData(true);
      // setShowNumRows(DEFAULT_NUM_ROWS);
    }
  };

  const handleSubmit = async (values, { setSubmitting, setStatus }) => {
    await runQuery(values, setStatus).then(_ => {
      window.scrollTo({ top: 0, left: 0, behavior: 'smooth' });
      setSubmitting(false);
    });
  };

  const handleChange = event => {
    // handle filters change
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

  const filters = dimensions.map(grouping => {
    const dimensionsInGroup = grouping[1];
    const dimensionGroupOptions = dimensionsInGroup
      .filter(dim => dim.is_primary_key === true)
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
              <DimensionFilter dimension={dimension} onChange={handleChange} />
            );
          })}
        </div>
      </>
    );
  });

  const initialValues = {};

  const [state, setState] = useState({
    selectedTab: 'results',
  });

  const switchTab = (event, tabName) => {
    setState({ selectedTab: tabName });
  };

  return (
    // <form
    //   id="retrieve-sql"
    //   name="retrieve-sql"
    //   onSubmit={handleSubmit.bind(this)}
    // >

    <Formik
      initialValues={initialValues}
      // validate={validator}
      onSubmit={handleSubmit}
    >
      {function Render({ isSubmitting, status, setFieldValue }) {
        return (
          <Form>
            {/*<h2 style={{ display: 'inline-flex', paddingRight: '20px' }}>*/}
            {/*  Test Query*/}
            {/*</h2>*/}

            {/*<button*/}
            {/*  type="submit"*/}
            {/*  disabled={isSubmitting}*/}
            {/*  className="button-3 execute-button"*/}
            {/*>*/}
            {/*  {isSubmitting ? <LoadingIcon /> : 'ⓘ Explain'}*/}
            {/*</button>*/}
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
                      onChange={handleChange}
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
                {/*<h6 className="mb-0 w-100">Query</h6>*/}
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
                  <SyntaxHighlighter language="sql" style={foundation}>
                    {query}
                  </SyntaxHighlighter>
                </div>
                <button
                  type="submit"
                  disabled={isSubmitting}
                  className="button-3 execute-button"
                  style={{ marginTop: '1rem' }}
                >
                  {isSubmitting ? <LoadingIcon /> : '► Run'}
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
                    onClick={ev => switchTab(ev, 'results')}
                  >
                    Results
                  </div>
                  <div
                    className={
                      'tab-item' +
                      (state.selectedTab === 'info' ? ' active' : '')
                    }
                    onClick={ev => switchTab(ev, 'info')}
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
                    {data ? (
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
                              {data[0]?.columns.map(columnName => (
                                <th key={columnName.name}>
                                  {columnName.column}
                                </th>
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
                ) : null}
              </div>
            </div>
            {/*</form>*/}
          </Form>
        );
      }}
    </Formik>
  );
}
