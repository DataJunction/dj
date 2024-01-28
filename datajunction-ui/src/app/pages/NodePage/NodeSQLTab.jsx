import { useContext, useEffect, useState } from 'react';
import Select from 'react-select';
import DJClientContext from '../../providers/djclient';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import QueryBuilder from 'react-querybuilder';

const NodeSQLTab = djNode => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [query, setQuery] = useState('');

  const [selection, setSelection] = useState({
    dimensions: [],
    filters: [],
  });
  // const [fields, setFields] = useState([]);
  const [filters, setFilters] = useState({ combinator: 'and', rules: [] });
  const validator = ruleType => !!ruleType.value;

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

  const uniqueFields = Object.fromEntries(
    new Map(djNode.djNode.dimensions.map(dim => attributeToFormInput(dim))),
  );
  const fields = Object.keys(uniqueFields).map(f => uniqueFields[f]);

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

        <h4>Filter By</h4>
        <QueryBuilder
          fields={fields}
          query={filters}
          onQueryChange={q => setFilters(q)}
        />
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
