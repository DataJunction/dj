/**
 * Metric aggregate expression input field, which consists of a CodeMirror SQL
 * editor with autocompletion for node columns and syntax highlighting.
 */
import React from 'react';
import { ErrorMessage, Field, useFormikContext } from 'formik';
import CodeMirror from '@uiw/react-codemirror';
import { langs } from '@uiw/codemirror-extensions-langs';

export const MetricQueryField = ({ djClient, value }) => {
  const [schema, setSchema] = React.useState([]);
  const formik = useFormikContext();
  const sqlExt = langs.sql({ schema: schema });

  const initialAutocomplete = async context => {
    // Based on the selected upstream, we load the upstream node's columns
    // into the autocomplete schema
    const nodeName = formik.values['upstream_node'];
    const nodeDetails = await djClient.node(nodeName);
    nodeDetails.columns.forEach(col => {
      schema[col.name] = [];
    });
    setSchema(schema);
  };

  const updateFormik = val => {
    formik.setFieldValue('aggregate_expression', val);
  };

  return (
    <div className="QueryInput MetricQueryInput NodeCreationInput">
      <ErrorMessage name="query" component="span" />
      <label htmlFor="Query">Aggregate Expression *</label>
      <Field
        type="textarea"
        style={{ display: 'none' }}
        as="textarea"
        name="aggregate_expression"
        id="Query"
      />
      <div role="button" tabIndex={0} className="relative flex bg-[#282a36]">
        <CodeMirror
          id={'aggregate_expression'}
          name={'aggregate_expression'}
          extensions={[
            sqlExt,
            sqlExt.language.data.of({
              autocomplete: initialAutocomplete,
            }),
          ]}
          value={value}
          options={{
            theme: 'default',
            lineNumbers: true,
          }}
          width="100%"
          height="100px"
          style={{
            margin: '0 0 23px 0',
            flex: 1,
            fontSize: '150%',
            textAlign: 'left',
          }}
          onChange={(value, viewUpdate) => {
            updateFormik(value);
          }}
        />
      </div>
    </div>
  );
};
