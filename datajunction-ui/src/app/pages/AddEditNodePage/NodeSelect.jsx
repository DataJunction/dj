/**
 * Node select component
 */
import { useContext, useMemo, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { FormikSelect } from './FormikSelect';

export const NodeSelect = ({ defaultValue }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // All available nodes
  const [availableNodes, setAvailableNodes] = useState([]);

  useMemo(() => {
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
    <>
      <FormikSelect
        className="SelectInput"
        defaultValue={defaultValue}
        selectOptions={availableNodes}
        formikFieldName="upstream_node"
        placeholder="Select Upstream Node"
        isMulti={false}
      />
    </>
  );
};
