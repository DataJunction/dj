/**
 * Custom metadata field component for nodes
 */
import { ErrorMessage, Field } from 'formik';
import { useEffect } from 'react';

export const CustomMetadataField = () => {
  const formatValue = (value) => {
    if (value === null || value === undefined) {
      return '';
    }
    if (typeof value === 'string') {
      return value;
    }
    return JSON.stringify(value, null, 2);
  };

  const handleChange = (e, setFieldValue) => {
    setFieldValue('custom_metadata', e.target.value);
  };

  return (
    <div className="NodeCreationInput" style={{ marginTop: '20px' }}>
      <label htmlFor="CustomMetadata">Custom Metadata (JSON)</label>
      <Field
        name="custom_metadata"
        validate={value => {
          if (!value || value.trim() === '') {
            return undefined;
          }
          try {
            const parsed = JSON.parse(value);

            if (
              typeof parsed === 'object' &&
              parsed !== null &&
              !Array.isArray(parsed)
            ) {
              const keys = Object.keys(parsed);
              const originalKeyMatches = value.match(/"([^"]+)"\s*:/g);
              if (
                originalKeyMatches &&
                originalKeyMatches.length > keys.length
              ) {
                return 'Duplicate keys detected';
              }
            }

            return undefined;
          } catch (err) {
            return 'Invalid JSON format';
          }
        }}
      >
        {({ field, form }) => {
          const errorMessage = form.errors.custom_metadata;
          const hasError = errorMessage && form.touched.custom_metadata;

          return (
            <div>
              <textarea
                id="CustomMetadata"
                value={formatValue(field.value)}
                onChange={e => handleChange(e, form.setFieldValue)}
                onBlur={() => form.setFieldTouched('custom_metadata', true)}
                style={{
                  width: '100%',
                  minHeight: '100px',
                  fontFamily: 'monospace',
                  fontSize: '12px',
                  padding: '8px',
                  border: hasError ? '1px solid red' : '1px solid #ccc',
                  borderRadius: '4px',
                }}
                placeholder="Define custom node metadata here..."
              />
              {hasError && (
                <span style={{ color: 'red', fontSize: '12px' }}>
                  {errorMessage}
                </span>
              )}
            </div>
          );
        }}
      </Field>
    </div>
  );
};
