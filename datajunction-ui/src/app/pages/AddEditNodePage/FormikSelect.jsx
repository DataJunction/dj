/**
 * A React Select component for use in Formik forms.
 */
import { useField } from 'formik';
import Select from 'react-select';

export const FormikSelect = ({
  selectOptions,
  formikFieldName,
  placeholder,
  defaultValue,
  style,
  className = 'SelectInput',
  isMulti = false,
  onFocus = event => {},
}) => {
  // eslint-disable-next-line no-unused-vars
  const [field, _, helpers] = useField(formikFieldName);
  const { setValue } = helpers;

  // handles both multi-select and single-select cases
  const getValue = options => {
    if (options) {
      return isMulti ? options.map(option => option.value) : options.value;
    } else {
      return isMulti ? [] : '';
    }
  };

  return (
    <Select
      className={className}
      defaultValue={defaultValue}
      options={selectOptions}
      name={field.name}
      placeholder={placeholder}
      onBlur={field.onBlur}
      onChange={selected => setValue(getValue(selected))}
      styles={style}
      isMulti={isMulti}
      onFocus={event => onFocus(event)}
    />
  );
};

FormikSelect.defaultProps = {
  placeholder: '',
};
