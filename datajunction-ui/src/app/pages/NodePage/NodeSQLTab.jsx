import { useContext, useEffect, useState } from 'react';
import Select from 'react-select';
import DJClientContext from '../../providers/djclient';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import { format } from 'sql-formatter';

const NodeSQLTab = djNode => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [query, setQuery] = useState('');

  const [selection, setSelection] = useState({
    dimensions: [],
    filters: [],
  });

  useEffect(() => {
    const fetchData = async () => {
      const query = await djClient.sql(djNode.djNode.name, selection);
      setQuery(query.sql);
    };
    fetchData().catch(console.error);
  }, [djClient, djNode.djNode.name, selection]);
  const dimensionsList = djNode.djNode.dimensions
    ? djNode.djNode.dimensions.map(dim => ({
        value: dim.name,
        label: dim.name + ` (${dim.type})`,
      }))
    : [''];

  // const options = [
  //   { value: '>=', label: '>=' },
  //   { value: '<=', label: '<=' },
  //   { value: '>', label: '>' },
  //   { value: '<', label: '<' },
  //   { value: '=', label: '=' },
  //   { value: '!=', label: '!=' },
  //   { value: 'IN', label: 'IN' },
  // ];

  const handleSubmit = event => {
    event.preventDefault();
  };

  const handleChange = event => {
    setSelection({ filters: [], dimensions: event.map(dim => dim.value) });
  };

  return (
    <form
      id="retrieve-sql"
      name="retrieve-sql"
      onSubmit={handleSubmit.bind(this)}
    >
      <div>
        <h4>Group By</h4>
        <Select
          name="dimensions"
          options={dimensionsList}
          isMulti
          isClearable
          onChange={handleChange}
        />
        {/*<h4>Filters</h4>*/}
        {/*<Select*/}
        {/*  name="filter_name"*/}
        {/*  options={dimensionsList}*/}
        {/*  className="filters_attribute"*/}
        {/*/>*/}
        {/*<Select*/}
        {/*  name="filter_operator"*/}
        {/*  options={options}*/}
        {/*  className="filters_attribute"*/}
        {/*/>*/}
        {/*<textarea name="filter_value" className="filters_attribute" />*/}

        <div
          style={{
            width: window.innerWidth * 0.8,
            marginTop: '2rem',
          }}
        >
          <h6 className="mb-0 w-100">Query</h6>
          <SyntaxHighlighter language="sql" style={foundation}>
            {query}
          </SyntaxHighlighter>
        </div>
      </div>
    </form>
  );
};

export default NodeSQLTab;
