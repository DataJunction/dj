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

  // Convert Formik field value to React Select format
  const getSelectValue = () => {
    if (!field.value) {
      return isMulti ? [] : null;
    }

    if (isMulti) {
      // For multi-select, map array of values to option objects
      return Array.isArray(field.value)
        ? field.value.map(
            val =>
              selectOptions.find(opt => opt.value === val) || {
                value: val,
                label: val,
              },
          )
        : [];
    } else {
      // For single-select, find the matching option
      return selectOptions.find(opt => opt.value === field.value) || null;
    }
  };

  return (
    <Select
      className={className}
      value={getSelectValue()}
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
