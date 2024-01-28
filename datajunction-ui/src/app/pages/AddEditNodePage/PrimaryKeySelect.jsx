/**
 * Primary key select component
 */
import { useFormikContext } from 'formik';
import { useContext, useMemo, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { FormikSelect } from './FormikSelect';

export const PrimaryKeySelect = ({
  defaultSelectOptions,
  defaultValue,
  style = '',
  className = 'SelectInput',
}) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // Used to pull out current form values for node validation
  const { values } = useFormikContext();

  // Select options, i.e., the available columns
  const [selectOptions, setSelectOptions] = useState([]);
  const selectableOptions = useMemo(() => {
    if (selectOptions && selectOptions.length > 0) {
      return selectOptions;
    }
    return defaultSelectOptions;
  }, [defaultSelectOptions, selectOptions]);

  const refreshColumns = event => {
    // When focus is on the primary key field, refresh the list of available
    // primary key columns for selection
    async function fetchData() {
      // eslint-disable-next-line no-unused-vars
      const { status, json } = await djClient.validateNode(
        values.type,
        values.name,
        values.display_name,
        values.description,
        values.query,
      );
      setSelectOptions(
        json.columns.map(col => {
          return { value: col.name, label: col.name };
        }),
      );
    }
    fetchData();
  };

  return (
    <FormikSelect
      className={className}
      defaultValue={defaultValue}
      selectOptions={selectableOptions}
      formikFieldName={'primary_key'}
      placeholder={'Choose Primary Key'}
      onFocus={event => refreshColumns(event)}
      styles={style}
      isMulti={true}
    />
  );
};
