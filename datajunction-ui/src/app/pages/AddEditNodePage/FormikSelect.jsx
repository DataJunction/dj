/**
 * A React Select component for use in Formik forms.
 */
import { useField } from 'formik';
import { useMemo } from 'react';
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

  // Derive the current value from field.value and selectOptions
  const getCurrentValue = useMemo(() => {
    if (!selectOptions || !field.value) {
      return isMulti ? [] : null;
    }

    if (isMulti) {
      // For multi-select, field.value is an array of values
      const values = Array.isArray(field.value) ? field.value : [field.value];
      return selectOptions.filter(option => values.includes(option.value));
    } else {
      // For single-select, field.value is a single value
      return selectOptions.find(option => option.value === field.value) || null;
    }
  }, [field.value, selectOptions, isMulti]);

  return (
    <Select
      className={className}
      value={getCurrentValue}
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
