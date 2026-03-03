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
  labelStyle = {},
  isMulti = false,
  isClearable = true,
}) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // Used to pull out current form values for node validation
  const { values } = useFormikContext();

  // The available columns, determined from validating the node query
  const [availableColumns, setAvailableColumns] = useState([]);
  const [validationError, setValidationError] = useState(null);
  const selectableOptions = useMemo(() => {
    if (availableColumns && availableColumns.length > 0) {
      return availableColumns;
    }
    return [];
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
        setValidationError(null);
      } else if (json?.message) {
        setValidationError(json.message);
        setAvailableColumns([]);
      }
    } catch (error) {
      console.error('Error fetching columns:', error);
      setValidationError(
        error.message ||
          'Failed to validate query. Please check your query syntax.',
      );
      setAvailableColumns([]);
    }
  };

  useEffect(() => {
    fetchColumns();
  }, [values.type, values.name, values.query]);

  return (
    <div className="CubeCreationInput">
      <ErrorMessage name={fieldName} component="span" />
      <label htmlFor={fieldName} style={labelStyle}>
        {label}
      </label>
      {validationError && (
        <div
          className="validation-error"
          style={{
            color: '#d32f2f',
            fontSize: '0.875rem',
            marginBottom: '8px',
            padding: '8px',
            backgroundColor: '#ffebee',
            borderRadius: '4px',
            border: '1px solid #ef9a9a',
          }}
        >
          <strong>Validation Error:</strong> {validationError}
        </div>
      )}
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
              : defaultValue
              ? { value: defaultValue, label: defaultValue }
              : null
          }
          selectOptions={selectableOptions}
          formikFieldName={fieldName}
          onFocus={event => fetchColumns(event)}
          isMulti={isMulti}
          isClearable={isClearable}
        />
      </span>
    </div>
  );
};
