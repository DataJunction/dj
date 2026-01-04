/**
 * Metric aggregate expression input field, which consists of a CodeMirror SQL
 * editor with autocompletion for node columns and syntax highlighting.
 *
 * Supports both:
 * - Regular metrics: autocomplete from upstream node columns
 * - Derived metrics: autocomplete from available metric names
 */
import React from 'react';
import { ErrorMessage, Field, useFormikContext } from 'formik';
import CodeMirror from '@uiw/react-codemirror';
import { langs } from '@uiw/codemirror-extensions-langs';

export const MetricQueryField = ({ djClient, value }) => {
  const [schema, setSchema] = React.useState({});
  const [availableMetrics, setAvailableMetrics] = React.useState([]);
  const formik = useFormikContext();
  const sqlExt = langs.sql({ schema: schema });

  // Load available metrics for derived metric autocomplete
  React.useEffect(() => {
    async function fetchMetrics() {
      try {
        const metrics = await djClient.metrics();
        setAvailableMetrics(metrics || []);
      } catch (err) {
        console.error('Failed to load metrics for autocomplete:', err);
      }
    }
    fetchMetrics();
  }, [djClient]);

  const initialAutocomplete = async context => {
    const newSchema = {};
    const nodeName = formik.values['upstream_node'];

    // If an upstream node is selected, load its columns for regular metrics
    if (nodeName && nodeName.trim() !== '') {
      try {
        const nodeDetails = await djClient.node(nodeName);
        if (nodeDetails && nodeDetails.columns) {
          nodeDetails.columns.forEach(col => {
            newSchema[col.name] = [];
          });
        }
      } catch (err) {
        console.error('Failed to load upstream node columns:', err);
      }
    }

    // Always include available metrics for derived metric expressions
    availableMetrics.forEach(metricName => {
      newSchema[metricName] = [];
    });

    setSchema(newSchema);
  };

  const updateFormik = val => {
    formik.setFieldValue('aggregate_expression', val);
  };

  // Determine the label and help text based on whether upstream is selected
  const upstreamNode = formik.values['upstream_node'];
  const isDerivedMode = !upstreamNode || upstreamNode.trim() === '';
  const labelText = isDerivedMode
    ? 'Derived Metric Expression *'
    : 'Aggregate Expression *';
  const helpText = isDerivedMode
    ? 'Reference other metrics using their full names (e.g., namespace.metric_name / namespace.other_metric)'
    : 'Use aggregate functions on columns from the upstream node (e.g., SUM(column_name))';

  return (
    <div className="QueryInput MetricQueryInput NodeCreationInput">
      <ErrorMessage name="query" component="span" />
      <label htmlFor="Query">{labelText}</label>
      <p
        className="field-help-text"
        style={{ fontSize: '0.85em', color: '#666', marginBottom: '8px' }}
      >
        {helpText}
      </p>
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
