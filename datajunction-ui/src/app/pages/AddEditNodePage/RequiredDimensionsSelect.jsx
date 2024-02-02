/**
 * Required dimensions select component
 */
import { useFormikContext } from 'formik';
import { useContext, useMemo, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { FormikSelect } from './FormikSelect';

export const RequiredDimensionsSelect = ({ defaultValue }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // Used to pull out current form values for node validation
  const { values } = useFormikContext();

  // Additional metadata on node parents
  const [nodeParents, setNodeParents] = useState([]);

  // The available columns, determined from validating the node query
  const [availableDimensions, setAvailableDimensions] = useState([]);
  const selectableOptions = useMemo(() => {
    if (availableDimensions && availableDimensions.length > 0) {
      return availableDimensions;
    }
  }, [availableDimensions]);

  // When focus is on the primary key field, refresh the list of available
  // primary key columns for selection
  const refreshAvailableDimensions = event => {
    async function fetchData() {
      // eslint-disable-next-line no-unused-vars
      const { status, json } = await djClient.validateNode(
        values.type,
        values.name,
        values.display_name,
        values.description,
        values.query,
      );
      const parents = json.dependencies.map(dep => dep.name);
      setNodeParents(json.dependencies);
      if (parents.length === 1) {
        const available = await djClient.nodeDimensions(parents, true);
        setAvailableDimensions(
          available.map(dim => {
            return {
              value: dim.name,
              label: dim.name,
            };
          }),
        );
      }
    }
    fetchData();
  };

  return (
    <>
      {nodeParents.length > 0
        ? 'Available dimensions found from upstream '
        : ''}
      {nodeParents.length > 0
        ? nodeParents.map(parent => (
            <a href={`/nodes/${parent.name}`}>{parent.name}</a>
          ))
        : ''}
      <FormikSelect
        className="MultiSelectInput"
        defaultValue={defaultValue}
        selectOptions={selectableOptions}
        formikFieldName="required_dimensions"
        placeholder="Select Required Dimensions"
        onFocus={event => refreshAvailableDimensions(event)}
        isMulti={true}
      />
    </>
  );
};
