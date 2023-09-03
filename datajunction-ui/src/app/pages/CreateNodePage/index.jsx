import { Formik, Form, Field, ErrorMessage, useFormikContext } from 'formik';
import CodeMirror from '@uiw/react-codemirror';
import { sql, StandardSQL } from '@codemirror/lang-sql';
import { StreamLanguage } from '@codemirror/stream-parser';

import NamespaceHeader from '../../components/NamespaceHeader';
import Select, { Options } from 'react-select';
import { useField } from 'formik';
import { useContext, useEffect, useRef, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';

const FormikSelect = ({ selectOptions, formikFieldName, placeholder }) => {
  // eslint-disable-next-line no-unused-vars
  const [field, _, helpers] = useField(formikFieldName);
  const { setValue } = helpers;

  return (
    <Select
      defaultValue={selectOptions.find(option => option.value === field.value)}
      options={selectOptions}
      placeholder={placeholder}
      onBlur={field.onBlur}
      onChange={option => setValue(option.value)}
    />
  );
};
FormikSelect.defaultProps = {
  placeholder: '',
};

const FullNameField = props => {
  const { values, touched, setFieldValue } = useFormikContext();
  const [field, meta] = useField(props);

  useEffect(() => {
    // set the value of textC, based on textA and textB
    console.log('values', values);
    if (values.namespace && values.displayName) {
      setFieldValue(
        props.name,
        values.namespace +
          '.' +
          values.displayName.toLowerCase().replace(/ /g, '_'),
      );
    }
  }, [setFieldValue, props.name, values]);

  return (
    <>
      <input {...props} {...field} />
      {!!meta.touched && !!meta.error && <div>{meta.error}</div>}
    </>
  );
};

export function CreateNodePage({ props }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [namespaces, setNamespaces] = useState([]);
  const formik = useFormikContext();
  const [code, setCode] = useState('');
  const textareaRef = useRef < HTMLTextAreaElement > null;

  // Get namespaces
  useEffect(() => {
    const fetchData = async () => {
      const namespaces = await djClient.namespaces();
      setNamespaces(
        namespaces.map(m => ({ value: m['namespace'], label: m['namespace'] })),
      );
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.metrics]);

  return (
    <div className="mid">
      <NamespaceHeader namespace="" />
      <div className="card">
        <div className="card-header">
          <h4>Create Node</h4>
          <center>
            <Formik
              initialValues={{ name: '', displayName: '', query: '' }}
              validate={values => {
                const errors = {};
                if (!values.name) {
                  errors.name = 'Required';
                }
                if (!values.query) {
                  errors.query = 'Required';
                }
                return errors;
              }}
              onSubmit={(values, { setSubmitting }) => {
                setTimeout(() => {
                  setSubmitting(false);
                }, 400);
              }}
            >
              {({ isSubmitting }) => (
                <Form>
                  <div className="inputContainer">
                    <ErrorMessage name="namespace" component="span" />
                    Namespace:{' '}
                    <FormikSelect
                      selectOptions={namespaces}
                      formikFieldName="namespace"
                      placeholder="Choose Namespace"
                    />
                  </div>
                  <div>
                    <ErrorMessage name="displayName" component="span" />
                    Display Name:{' '}
                    <Field
                      type="text"
                      name="displayName"
                      placeholder="Display Name"
                    />
                  </div>
                  <div className="inputContainer">
                    <ErrorMessage name="name" component="span" />
                    Full Name:{' '}
                    <FullNameField
                      type="text"
                      name="name"
                      placeholder="Full Name"
                    />
                  </div>
                  <div>
                    <ErrorMessage name="query" component="span" />
                    Query: <br />
                    <div
                      role="button"
                      tabIndex={0}
                      onKeyDown={() => textareaRef.current?.focus()}
                      onClick={() => textareaRef.current?.focus()}
                      className="relative flex bg-[#282a36]"
                    >
                      <Field
                        type="text"
                        as="textarea"
                        name="query"
                        placeholder="Query"
                        value={code}
                        onChange={e => setCode(e.target.value)}
                        innerRef={() => textareaRef}
                        className="absolute inset-0 resize-none bg-transparent p-2 font-mono text-transparent caret-white outline-none"
                      />
                      <CodeMirror
                        id={'query'}
                        name={'query'}
                        extensions={[StreamLanguage.define(sql)]}
                        options={{
                          theme: 'default',
                          lineNumbers: true,
                        }}
                        value={'select * from something'}
                        onBeforeChange={(_editor, _data, value) => {
                          formik.setFieldValue('query', value);
                        }}
                      />
                    </div>
                  </div>
                  <div>
                    <ErrorMessage name="mode" component="span" />
                    Mode:{' '}
                    <Field as="select" name="mode" placeholder="Mode">
                      <option value="draft">Draft</option>
                      <option value="published">Published</option>
                    </Field>
                  </div>
                  <button type="submit" disabled={isSubmitting}>
                    Create
                  </button>
                </Form>
              )}
            </Formik>
          </center>
        </div>
      </div>
    </div>
  );
}
