/**
 * Required dimensions select component
 */
import { ErrorMessage, useFormikContext } from 'formik';
import React, { useContext, useMemo, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { FormikSelect } from './FormikSelect';

export const RequiredDimensionsSelect = ({
  defaultSelectOptions,
  defaultValue,
  style,
  className = 'SelectInput',
}) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // Used to pull out current form values for node validation
  const { values } = useFormikContext();

  // Select options, i.e., the available dimensions
  const [selectOptions, setSelectOptions] = useState([]);
  const selectableOptions = useMemo(() => {
    if (selectOptions && selectOptions.length > 0) {
      return selectOptions;
    }
    return defaultSelectOptions;
  }, [defaultSelectOptions, selectOptions]);

  // Additional metadata on node parents
  const [nodeParents, setNodeParents] = useState([]);

  const refreshDimensions = event => {
    // When focus is on the primary key field, refresh the list of available
    // primary key columns for selection
    async function fetchData() {
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
        setSelectOptions(
          available.map(dim => {
            return { value: dim.name, label: dim.name };
          }),
        );
      }
    }
    fetchData();
  };

  return (
    <div className="RequiredDimensionsInput CubeCreationInput">
      <ErrorMessage name="required_dimensions" component="span" />
      <label htmlFor="requiredDimensions">Required Dimensions</label>
      {selectableOptions && selectableOptions.length > 0 ? (
        <span style={{ paddingBottom: '12px' }}>
          Available dimensions pulled from upstream{' '}
          {nodeParents.map(parent => (
            <a href={`/nodes/${parent.name}`}>{parent.name}</a>
          ))}
          . If you are unable to find your desired dimensions, please link them
          into the above node.
        </span>
      ) : (
        <></>
      )}
      <FormikSelect
        className={className}
        defaultValue={defaultValue}
        selectOptions={selectableOptions}
        formikFieldName={'required_dimensions'}
        placeholder={'Choose Required Dimensions'}
        onFocus={event => refreshDimensions(event)}
        styles={style}
        isMulti={true}
      />
    </div>
  );
};
