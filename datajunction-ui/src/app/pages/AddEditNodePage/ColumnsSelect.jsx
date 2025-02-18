/**
 * Component for selecting node columns based on the current form state
 */
import { ErrorMessage, useFormikContext } from 'formik';
import { useContext, useMemo, useState, useEffect } from 'react';
import DJClientContext from '../../providers/djclient';
import { FormikSelect } from './FormikSelect';

export const ColumnsSelect = ({
  defaultValue,
  fieldName,
  label,
  isMulti = false,
}) => {
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

  // Fetch columns by validating the latest node query
  const fetchColumns = async () => {
    try {
      const { status, json } = await djClient.validateNode(
        values.type,
        values.name,
        values.display_name,
        values.description,
        values.query,
      );
      if (json?.columns) {
        setAvailableColumns(
          json.columns.map(col => ({ value: col.name, label: col.name })),
        );
      }
    } catch (error) {
      console.error('Error fetching columns:', error);
    }
  };

  useEffect(() => {
    fetchColumns();
  }, [values.type, values.name, values.query]);

  return selectableOptions === undefined ? (
    ''
  ) : (
    <div className="CubeCreationInput">
      <ErrorMessage name={fieldName} component="span" />
      <label htmlFor="react-select-3-input">{label}</label>
      <span data-testid={`select-${fieldName}`}>
        <FormikSelect
          className={isMulti ? 'MultiSelectInput' : 'SelectInput'}
          defaultValue={
            isMulti
              ? defaultValue.map(val => {
                  return {
                    value: val,
                    label: val,
                  };
                })
              : { value: defaultValue, label: defaultValue }
          }
          selectOptions={selectableOptions}
          formikFieldName={fieldName}
          onFocus={event => fetchColumns(event)}
          isMulti={isMulti}
        />
      </span>
    </div>
  );
};
