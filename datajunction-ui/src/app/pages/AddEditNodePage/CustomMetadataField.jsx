/**
 * Custom metadata field component for nodes
 */
import { ErrorMessage, Field, useFormikContext } from 'formik';
import CodeMirror from '@uiw/react-codemirror';
import { langs } from '@uiw/codemirror-extensions-langs';
import { useState, useEffect } from 'react';

export const CustomMetadataField = ({ value }) => {
  const formik = useFormikContext();
  const jsonExt = langs.json();
  const [hasError, setHasError] = useState(false);

  useEffect(() => {
    if (!value || value === '') {
      setHasError(false);
      return;
    }

    const stringValue =
      typeof value === 'string' ? value : JSON.stringify(value, null, 2);

    try {
      JSON.parse(stringValue);
      setHasError(false);
    } catch (err) {
      setHasError(true);
    }
  }, [value]);

  const formatValue = value => {
    if (value === null || value === undefined) {
      return '';
    }
    if (typeof value === 'string') {
      return value;
    }
    return JSON.stringify(value, null, 2);
  };

  const updateFormik = val => {
    formik.setFieldValue('custom_metadata', val);
    formik.setFieldTouched('custom_metadata', true);

    if (!val || val.trim() === '') {
      setHasError(false);
    } else {
      try {
        JSON.parse(val);
        setHasError(false);
      } catch (err) {
        setHasError(true);
      }
    }
  };

  return (
    <div className="QueryInput NodeCreationInput">
      <details>
        <summary style={{ cursor: 'pointer' }}>
          <label
            style={{
              paddingLeft: '3px',
              display: 'inline-block',
              pointerEvents: 'none',
            }}
          >
            Custom Metadata (JSON)
          </label>
        </summary>
        <ErrorMessage name="custom_metadata" component="span" />
        <Field
          type="textarea"
          style={{ display: 'none' }}
          as="textarea"
          name="custom_metadata"
          id="CustomMetadata"
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
        />
        <div
          role="button"
          tabIndex={0}
          className={`relative flex ${
            hasError ? 'bg-red-900/20' : 'bg-[#282a36]'
          }`}
          style={{
            border: hasError ? '2px solid #ef4444' : 'none',
            borderRadius: '4px',
            boxShadow: hasError ? '0 0 0 1px rgba(239, 68, 68, 0.3)' : 'none',
          }}
        >
          <CodeMirror
            id={'custom_metadata'}
            name={'custom_metadata'}
            extensions={[jsonExt]}
            value={formatValue(value)}
            placeholder={'{\n  "key": "value"\n}'}
            options={{
              theme: 'default',
              lineNumbers: true,
            }}
            width="100%"
            height="200px"
            style={{
              margin: '0 0 23px 0',
              flex: 1,
              fontSize: '150%',
              textAlign: 'left',
            }}
            onChange={(value, viewUpdate) => {
              updateFormik(value);
            }}
          />
        </div>
      </details>
    </div>
  );
};
