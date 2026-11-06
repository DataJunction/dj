/**
 * Upstream node select field.
 *
 * For regular metrics: Select a source, transform, or dimension node.
 * For derived metrics: Leave empty and reference other metrics directly in the expression.
 */
import { ErrorMessage } from 'formik';
import { useCallback, useContext, useRef } from 'react';
import DJClientContext from '../../providers/djclient';
import { FormikSelect } from './FormikSelect';

// Large instances have tens of thousands of candidate nodes, so nothing is
// loaded up front; typing searches node names server-side. Name-only matching
// mirrors the old client-side filter, and ranked search across all three types
// is too slow on large instances to run per keystroke.
const UPSTREAM_NODE_TYPES = ['SOURCE', 'TRANSFORM', 'DIMENSION'];
const UPSTREAM_OPTIONS_LIMIT = 100;

export const UpstreamNodeField = ({ defaultValue }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const searchAbort = useRef(null);

  // react-select calls this on every keystroke; the previous request is
  // abandoned so out-of-order responses can't overwrite newer ones.
  const loadUpstreamOptions = useCallback(
    async inputValue => {
      const query = (inputValue || '').trim();
      if (!query) {
        return [];
      }
      if (searchAbort.current) {
        searchAbort.current.abort();
      }
      const controller = new AbortController();
      searchAbort.current = controller;
      try {
        const matches = await djClient.searchNodes(query, {
          nodeTypes: UPSTREAM_NODE_TYPES,
          nameOnly: true,
          signal: controller.signal,
          limit: UPSTREAM_OPTIONS_LIMIT,
        });
        return matches.map(node => ({ value: node.name, label: node.name }));
      } catch (error) {
        if (error.name !== 'AbortError') {
          console.error(error);
        }
        return [];
      }
    },
    [djClient],
  );

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
          className=""
          defaultValue={defaultValue}
          selectOptions={[]}
          loadOptions={loadUpstreamOptions}
          formikFieldName="upstream_node"
          placeholder="Type to search upstream nodes (optional for derived metrics)"
          noOptionsMessage={({ inputValue }) =>
            inputValue ? 'No matching nodes' : 'Type to search'
          }
          isMulti={false}
          isClearable={true}
        />
      </span>
    </div>
  );
};
