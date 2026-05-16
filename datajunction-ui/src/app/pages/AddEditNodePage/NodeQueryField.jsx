/**
 * SQL query input field, which consists of a CodeMirror SQL editor with autocompletion
 * (for node names and columns) and syntax highlighting.
 */
import React from 'react';
import { ErrorMessage, Field, useFormikContext } from 'formik';
import CodeMirror from '@uiw/react-codemirror';
import { langs } from '@uiw/codemirror-extensions-langs';
import { extractCatalogTables } from './catalogTables';

export const NodeQueryField = ({ djClient, value }) => {
  const [schema, setSchema] = React.useState([]);
  const formik = useFormikContext();
  const sqlExt = langs.sql({ schema: schema });
  const autoRegisterTimer = React.useRef(null);
  const registeredTables = React.useRef(new Set());
  // useRef so the onChange closure always sees the latest catalog list
  const knownCatalogsRef = React.useRef([]);

  React.useEffect(() => {
    if (typeof djClient.catalogs !== 'function') return;
    Promise.resolve(djClient.catalogs())
      .then(catalogs => {
        knownCatalogsRef.current = (catalogs || []).map(c =>
          c.name.toLowerCase(),
        );
      })
      .catch(() => {
        // Auto-register is best-effort; failing to load catalogs just disables it.
      });
  }, [djClient]);

  const initialAutocomplete = async context => {
    // Based on the parsed prefix, we load node names with that prefix
    // into the autocomplete schema. At this stage we don't load the columns
    // to save on unnecessary calls
    const word = context.matchBefore(/[\.\w]*/);
    const matches = await djClient.nodes(word.text);
    matches.forEach(nodeName => {
      if (schema[nodeName] === undefined) {
        schema[nodeName] = [];
        setSchema(schema);
      }
    });
  };

  const updateFormik = val => {
    formik.setFieldValue('query', val);
  };

  const updateAutocomplete = async (value, _) => {
    // If a particular node has been chosen, load the columns of that node into
    // the autocomplete schema for column-level autocompletion
    for (var nodeName in schema) {
      if (
        value.includes(nodeName) &&
        (!schema.hasOwnProperty(nodeName) ||
          (schema.hasOwnProperty(nodeName) && schema[nodeName].length === 0))
      ) {
        const nodeDetails = await djClient.node(nodeName);
        schema[nodeName] = nodeDetails.columns.map(col => col.name);
        setSchema(schema);
      }
    }

    // Auto-register any catalog-qualified tables typed in the SQL
    if (knownCatalogsRef.current.length > 0) {
      clearTimeout(autoRegisterTimer.current);
      autoRegisterTimer.current = setTimeout(
        () => autoRegisterCatalogTables(value),
        600,
      );
    }
  };

  const autoRegisterCatalogTables = async sql => {
    const tables = extractCatalogTables(sql, knownCatalogsRef.current);
    for (const [catalog, tableSchema, table] of tables) {
      const key = `${catalog}.${tableSchema}.${table}`;
      if (registeredTables.current.has(key)) continue;
      // If autocomplete already saw this node, DJ knows about it — skip silently
      if (schema[key] !== undefined) {
        registeredTables.current.add(key);
        continue;
      }
      registeredTables.current.add(key);

      let response;
      try {
        response = await djClient.registerTable(catalog, tableSchema, table, '');
      } catch {
        registeredTables.current.delete(key);
        continue;
      }
      const { status, json } = response;
      if (status === 200 || status === 201) {
        const nodeName = json?.name || key;
        const columns = (json?.columns || []).map(col => col.name);
        schema[nodeName] = columns;
        if (table && table !== nodeName) {
          schema[table] = columns;
        }
        setSchema(schema);
      } else if (status !== 409) {
        // 409 = already exists (silent); other errors: allow retry on next edit
        registeredTables.current.delete(key);
      }
    }
  };

  return (
    <div className="QueryInput NodeCreationInput">
      <ErrorMessage name="query" component="span" />
      <label htmlFor="Query">Query *</label>
      <Field
        type="textarea"
        style={{ display: 'none' }}
        as="textarea"
        name="query"
        id="Query"
      />
      <div role="button" tabIndex={0} className="relative flex bg-[#282a36]">
        <CodeMirror
          id={'query'}
          name={'query'}
          extensions={[
            sqlExt,
            sqlExt.language.data.of({
              autocomplete: initialAutocomplete,
            }),
          ]}
          value={value}
          placeholder={
            'SELECT\n\tprimary_key,\n\tmeasure1,\n\tmeasure2,\n\tforeign_key_for_dimension1,\n\tforeign_key_for_dimension2\nFROM source.source_node\nWHERE ...'
          }
          options={{
            theme: 'default',
            lineNumbers: true,
          }}
          width="100%"
          height="400px"
          style={{
            margin: '0 0 23px 0',
            flex: 1,
            fontSize: '150%',
            textAlign: 'left',
          }}
          onChange={(value, viewUpdate) => {
            updateFormik(value);
            updateAutocomplete(value, viewUpdate);
          }}
        />
      </div>
    </div>
  );
};
