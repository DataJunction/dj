/**
 * Materialization configuration field.
 */
import React from 'react';
import { ErrorMessage, Field, useFormikContext } from 'formik';
import CodeMirror from '@uiw/react-codemirror';
import { langs } from '@uiw/codemirror-extensions-langs';

export const ConfigField = ({ djClient, value }) => {
  const formik = useFormikContext();
  const jsonExt = langs.json();

  const updateFormik = val => {
    formik.setFieldValue('spark_config', JSON.parse(val));
  };

  return (
    <div className="DescriptionInput">
      <ErrorMessage name="spark_config" component="span" />
      <label htmlFor="SparkConfig">Spark Config</label>
      <Field
        type="textarea"
        style={{ display: 'none' }}
        as="textarea"
        name="spark_config"
        id="SparkConfig"
      />
      <div role="button" tabIndex={0} className="relative flex bg-[#282a36]">
        <CodeMirror
          id={'spark_config'}
          name={'spark_config'}
          extensions={[jsonExt]}
          value={JSON.stringify(value, null, "    ")}
          options={{
            theme: 'default',
            lineNumbers: true,
          }}
          width="100%"
          height="170px"
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
    </div>
  );
};
