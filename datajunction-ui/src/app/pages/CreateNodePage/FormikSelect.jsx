/**
 * A react-select component for use in Formik forms
 */
import { useField } from 'formik';
import Select from 'react-select';

export const FormikSelect = ({
  selectOptions,
  formikFieldName,
  placeholder,
  defaultValue,
  style,
}) => {
  // eslint-disable-next-line no-unused-vars
  const [field, _, helpers] = useField(formikFieldName);
  const { setValue } = helpers;

  return (
    <Select
      className="SelectInput"
      defaultValue={defaultValue}
      options={selectOptions}
      placeholder={placeholder}
      onBlur={field.onBlur}
      onChange={option => setValue(option.value)}
      styles={style}
    />
  );
};

FormikSelect.defaultProps = {
  placeholder: '',
};
