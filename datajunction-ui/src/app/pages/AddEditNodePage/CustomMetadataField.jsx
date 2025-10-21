/**
 * Custom metadata field component for nodes
 */
import { ErrorMessage, Field } from 'formik';
import { useState } from 'react';

export const CustomMetadataField = ({ initialValue = {} }) => {
  const [jsonString, setJsonString] = useState(
    JSON.stringify(initialValue, null, 2),
  );
  const [error, setError] = useState('');

  const handleChange = (e, setFieldValue) => {
    const value = e.target.value;
    setJsonString(value);

    try {
      if (value.trim() === '') {
        setFieldValue('custom_metadata', null);
        setError('');
      } else {
        const parsed = JSON.parse(value);
        setFieldValue('custom_metadata', parsed);
        setError('');
      }
    } catch (err) {
      setError('Invalid JSON format');
    }
  };

  return (
    <div className="NodeCreationInput" style={{ marginTop: '20px' }}>
      <ErrorMessage name="custom_metadata" component="span" />
      <label htmlFor="CustomMetadata">Custom Metadata (JSON)</label>
      <Field name="custom_metadata">
        {({ field, form }) => (
          <div>
            <textarea
              id="CustomMetadata"
              value={jsonString}
              onChange={e => handleChange(e, form.setFieldValue)}
              style={{
                width: '100%',
                minHeight: '100px',
                fontFamily: 'monospace',
                fontSize: '12px',
                padding: '8px',
                border: error ? '1px solid red' : '1px solid #ccc',
                borderRadius: '4px',
              }}
              placeholder='{"key": "value"}'
            />
            {error && (
              <span style={{ color: 'red', fontSize: '12px' }}>{error}</span>
            )}
          </div>
        )}
      </Field>
    </div>
  );
};
