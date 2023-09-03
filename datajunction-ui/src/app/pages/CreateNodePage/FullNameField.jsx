import { useField, useFormikContext } from 'formik';
import { useEffect } from 'react';

export const FullNameField = props => {
  const { values, setFieldValue } = useFormikContext();
  const [field, meta] = useField(props);

  useEffect(() => {
    // Set the value of the node's full name based on its namespace and display name
    if (values.namespace && values.display_name) {
      setFieldValue(
        props.name,
        `${values.namespace}.${values.display_name
          .toLowerCase()
          .replace(/ /g, '_')}`,
      );
    }
  }, [setFieldValue, props.name, values]);

  return (
    <>
      <input
        {...props}
        {...field}
        className="FullNameField"
        disabled="disabled"
        id="FullName"
      />
      {!!meta.touched && !!meta.error && <div>{meta.error}</div>}
    </>
  );
};
