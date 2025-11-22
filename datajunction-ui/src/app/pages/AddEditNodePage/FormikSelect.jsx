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
  isClearable = false,
  onFocus = event => {},
  onChange: customOnChange,
  menuPortalTarget,
  styles,
  ...rest
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

  const handleChange = selected => {
    setValue(getValue(selected));
    if (customOnChange) {
      customOnChange(selected);
    }
  };

  // Get the current value from field and find the matching option(s)
  const getCurrentValue = () => {
    if (isMulti) {
      return selectOptions.filter(option =>
        field.value?.includes(option.value),
      );
    } else {
      return selectOptions.find(option => option.value === field.value) || null;
    }
  };

  return (
    <Select
      className={className}
      value={getCurrentValue()}
      defaultValue={defaultValue}
      options={selectOptions}
      name={field.name}
      placeholder={placeholder}
      onBlur={field.onBlur}
      onChange={handleChange}
      styles={styles || style}
      isMulti={isMulti}
      isClearable={isClearable}
      onFocus={event => onFocus(event)}
      id={field.name}
      menuPortalTarget={menuPortalTarget}
      {...rest}
    />
  );
};

FormikSelect.defaultProps = {
  placeholder: '',
};
