/**
 * Upstream node select field.
 *
 * For regular metrics: Select a source, transform, or dimension node.
 * For derived metrics: Leave empty and reference other metrics directly in the expression.
 */
import { ErrorMessage } from 'formik';
import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { FormikSelect } from './FormikSelect';

export const UpstreamNodeField = ({ defaultValue }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // All available nodes (sources, transforms, dimensions)
  const [availableNodes, setAvailableNodes] = useState([]);

  useEffect(() => {
    async function fetchData() {
      const sources = await djClient.nodesWithType('source');
      const transforms = await djClient.nodesWithType('transform');
      const dimensions = await djClient.nodesWithType('dimension');
      const nodes = sources.concat(transforms).concat(dimensions);
      setAvailableNodes(
        nodes.map(node => {
          return {
            value: node,
            label: node,
          };
        }),
      );
    }
    fetchData();
  }, [djClient]);

  return (
    <div className="NodeCreationInput">
      <ErrorMessage name="upstream_node" component="span" />
      <label htmlFor="upstream_node">Upstream Node</label>
      <p
        className="field-help-text"
        style={{ fontSize: '0.85em', color: '#666', marginBottom: '8px' }}
      >
        Select a source, transform, or dimension for regular metrics. Leave
        empty for <strong>derived metrics</strong> that reference other metrics
        (e.g., <code>namespace.metric_a / namespace.metric_b</code>).
      </p>
      <span data-testid="select-upstream-node">
        <FormikSelect
          className="SelectInput"
          defaultValue={defaultValue}
          selectOptions={availableNodes}
          formikFieldName="upstream_node"
          placeholder="Select Upstream Node (optional for derived metrics)"
          isMulti={false}
          isClearable={true}
        />
      </span>
    </div>
  );
};
