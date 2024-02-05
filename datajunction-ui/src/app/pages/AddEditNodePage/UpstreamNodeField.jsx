/**
 * Upstream node select field
 */
import { ErrorMessage } from 'formik';
import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { FormikSelect } from './FormikSelect';

export const UpstreamNodeField = ({ defaultValue }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // All available nodes
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
      <ErrorMessage name="mode" component="span" />
      <label htmlFor="Mode">Upstream Node *</label>
      <span data-testid="select-upstream-node">
        <FormikSelect
          className="SelectInput"
          defaultValue={defaultValue}
          selectOptions={availableNodes}
          formikFieldName="upstream_node"
          placeholder="Select Upstream Node"
          isMulti={false}
        />
      </span>
    </div>
  );
};
