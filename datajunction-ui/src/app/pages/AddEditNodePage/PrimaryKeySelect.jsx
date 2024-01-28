/**
 * Primary key select component
 */
import { useFormikContext } from 'formik';
import { useContext, useMemo, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { FormikSelect } from './FormikSelect';

export const PrimaryKeySelect = ({ defaultValue }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // Used to pull out current form values for node validation
  const { values } = useFormikContext();

  // The available columns, determined from validating the node query
  const [availableColumns, setAvailableColumns] = useState([]);
  const selectableOptions = useMemo(() => {
    if (availableColumns && availableColumns.length > 0) {
      return availableColumns;
    }
  }, [availableColumns]);

  // When focus is on the primary key field, refresh the list of available
  // primary key columns for selection
  const refreshColumns = event => {
    async function fetchData() {
      // eslint-disable-next-line no-unused-vars
      const { status, json } = await djClient.validateNode(
        values.type,
        values.name,
        values.display_name,
        values.description,
        values.query,
      );
      setAvailableColumns(
        json.columns.map(col => {
          return { value: col.name, label: col.name };
        }),
      );
    }
    fetchData();
  };

  return (
    <FormikSelect
      className="MultiSelectInput"
      defaultValue={defaultValue}
      selectOptions={selectableOptions}
      formikFieldName="primary_key"
      placeholder="Choose Primary Key"
      onFocus={event => refreshColumns(event)}
      isMulti={true}
    />
  );
};
