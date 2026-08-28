/**
 * Semi-additive metric controls.
 */
import { ErrorMessage, Field, useField, useFormikContext } from 'formik';
import { useContext, useEffect, useMemo, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { labelize } from '../../../utils/form';
import { FormikSelect } from './FormikSelect';

const SEMI_ADDITIVE_FUNCTIONS = ['last_value', 'first_value', 'min', 'max'];

export const ReaggregateFields = () => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { values } = useFormikContext();
  const [dimensionField] = useField('reaggregate_dimension');
  const [dimensionOptions, setDimensionOptions] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      if (values.upstream_node) {
        const data = await djClient.node(values.upstream_node);
        setDimensionOptions(
          data.columns.map(col => ({
            value: col.name,
            label: col.name,
          })),
        );
      } else {
        setDimensionOptions([]);
      }
    };
    fetchData().catch(console.error);
  }, [djClient, values.upstream_node]);

  const selectOptions = useMemo(() => {
    if (
      !dimensionField.value ||
      dimensionOptions.some(option => option.value === dimensionField.value)
    ) {
      return dimensionOptions;
    }
    return [
      { value: dimensionField.value, label: dimensionField.value },
      ...dimensionOptions,
    ];
  }, [dimensionField.value, dimensionOptions]);

  return (
    <div className="ReaggregateFields node-row">
      <div className="NodeCreationInput">
        <ErrorMessage name="reaggregate_dimension" component="span" />
        <label htmlFor="reaggregate_dimension">Semi-Additive Dimension</label>
        <span data-testid="select-semi-additive-dimension">
          <FormikSelect
            className=""
            classNamePrefix="ReaggregateDimension"
            selectOptions={selectOptions}
            formikFieldName="reaggregate_dimension"
            placeholder="Choose Semi-Additive Dimension"
            isMulti={false}
            isClearable={true}
          />
        </span>
      </div>
      <div className="NodeCreationInput NodeModeInput">
        <ErrorMessage name="reaggregate_function" component="span" />
        <label htmlFor="ReaggregateFunction">Semi-Additive Type</label>
        <Field
          as="select"
          name="reaggregate_function"
          id="ReaggregateFunction"
        >
          <option value=""></option>
          {SEMI_ADDITIVE_FUNCTIONS.map(func => (
            <option value={func} key={func}>
              {labelize(func)}
            </option>
          ))}
        </Field>
      </div>
    </div>
  );
};
