/**
 * A React Select component for use in Formik forms.
 */
import { useField } from 'formik';
import Select from 'react-select';
import AsyncSelect from 'react-select/async';

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
  // When provided, options are fetched from the server as the user types and
  // `selectOptions` is only the initial list shown before anything is typed.
  loadOptions = null,
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
    if (!field.value || !selectOptions) {
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
      // For single-select, find the matching option. When options are loaded
      // asynchronously the current value often isn't among them, so fall back
      // to the raw value rather than rendering an empty control.
      return (
        selectOptions.find(opt => opt.value === field.value) ||
        (loadOptions ? { value: field.value, label: field.value } : null)
      );
    }
  };

  const SelectComponent = loadOptions ? AsyncSelect : Select;

  return (
    <SelectComponent
      className={className}
      value={getSelectValue()}
      options={selectOptions}
      {...(loadOptions
        ? { loadOptions, defaultOptions: selectOptions, cacheOptions: true }
        : {})}
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
