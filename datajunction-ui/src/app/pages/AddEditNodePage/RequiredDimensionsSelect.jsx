/**
 * Required dimensions select component
 */
import { ErrorMessage, useFormikContext } from 'formik';
import React, { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { FormikSelect } from './FormikSelect';

export const RequiredDimensionsSelect = ({
  defaultValue,
  style,
  className = 'MultiSelectInput',
}) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // Used to pull out current form values for node validation
  const { values } = useFormikContext();

  // Select options, i.e., the available dimensions
  const [selectOptions, setSelectOptions] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      if (values.upstream_node) {
        const data = await djClient.node(values.upstream_node);
        setSelectOptions(
          data.columns.map(col => {
            return {
              value: col.name,
              label: col.name,
            };
          }),
        );
      }
    };
    fetchData().catch(console.error);
  }, [djClient, values.upstream_node]);

  return (
    <div className="RequiredDimensionsInput CubeCreationInput">
      <ErrorMessage name="required_dimensions" component="span" />
      <label htmlFor="requiredDimensions">Required Dimensions</label>
      <FormikSelect
        className={className}
        defaultValue={defaultValue}
        selectOptions={selectOptions}
        formikFieldName={'required_dimensions'}
        placeholder={'Choose Required Dimensions'}
        styles={style}
        isMulti={true}
      />
    </div>
  );
};
