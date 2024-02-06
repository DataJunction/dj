/**
 * A field for the full node name, which is generated based on the node's input
 * namespace and display name.
 */
import { useField, useFormikContext } from 'formik';
import { useEffect } from 'react';

export const FullNameField = props => {
  const { values, setFieldValue } = useFormikContext();
  const [field, meta] = useField(props);

  useEffect(() => {
    // Set the value of the node's full name based on its namespace and display name
    if (values.namespace || values.display_name) {
      setFieldValue(
        props.name,
        `${values.namespace}.${values.display_name
          .toLowerCase()
          .replace(/ /g, '_')
          .replace(/[^a-zA-Z0-9_]/g, '')}` || '',
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
        value={values.name || ''}
      />
      {!!meta.touched && !!meta.error && <div>{meta.error}</div>}
    </>
  );
};
