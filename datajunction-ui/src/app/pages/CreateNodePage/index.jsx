/**
 * Node creation page for transforms, metrics, and dimensions. The creation flow for these
 * node types is largely the same, with minor differences handled server-side. For the `query`
 * field, this page will render a CodeMirror SQL editor with autocompletion and syntax highlighting.
 */
import { ErrorMessage, Field, Form, Formik } from 'formik';

import NamespaceHeader from '../../components/NamespaceHeader';
import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import 'styles/node-creation.scss';
import AlertIcon from '../../icons/AlertIcon';
import ValidIcon from '../../icons/ValidIcon';
import { useParams } from 'react-router-dom';
import { FullNameField } from './FullNameField';
import { FormikSelect } from './FormikSelect';
import { NodeQueryField } from './NodeQueryField';

export function CreateNodePage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [namespaces, setNamespaces] = useState([]);
  let { nodeType, initialNamespace } = useParams();

  const createNode = async (values, setStatus) => {
    const { status, json } = await djClient.createNode(
      nodeType,
      values.name,
      values.display_name,
      values.description,
      values.query,
      values.mode,
      values.namespace,
    );
    if (status === 200 || status === 201) {
      setStatus({
        success: (
          <>
            Successfully created {json.type} node{' '}
            <a href={`/nodes/${json.name}`}>{json.name}</a>!
          </>
        ),
      });
    } else {
      setStatus({
        failure: `${json.message}`,
      });
    }
  };

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
          <h2>Create {nodeType}</h2>
          <center>
            <Formik
              initialValues={{
                name: '',
                namespace: initialNamespace,
                display_name: '',
                query: '',
                node_type: '',
                description: '',
                mode: 'draft',
              }}
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
              onSubmit={(values, { setSubmitting, setStatus }) => {
                setTimeout(() => {
                  createNode(values, setStatus);
                  setSubmitting(false);
                }, 400);
                window.scrollTo({ top: 0, left: 0, behavior: 'smooth' });
              }}
            >
              {({ isSubmitting, status }) => (
                <Form>
                  {status?.success !== undefined ? (
                    <div className="success">
                      <ValidIcon />
                      {status?.success}
                    </div>
                  ) : status?.failure !== undefined ? (
                    <div className="alert">
                      <AlertIcon />
                      {status?.failure}
                    </div>
                  ) : (
                    ''
                  )}
                  <div className="NamespaceInput">
                    <ErrorMessage name="namespace" component="span" />
                    <label htmlFor="react-select-3-input">Namespace</label>
                    <FormikSelect
                      selectOptions={namespaces}
                      formikFieldName="namespace"
                      placeholder="Choose Namespace"
                      defaultValue={{
                        value: initialNamespace,
                        label: initialNamespace,
                      }}
                    />
                  </div>
                  <div className="DisplayNameInput NodeCreationInput">
                    <ErrorMessage name="display_name" component="span" />
                    <label htmlFor="displayName">Display Name</label>
                    <Field type="text" name="display_name" id="displayName" />
                  </div>
                  <div className="FullNameInput NodeCreationInput">
                    <ErrorMessage name="name" component="span" />
                    <label htmlFor="FullName">Full Name</label>
                    <FullNameField type="text" name="name" />
                  </div>
                  <div className="DescriptionInput NodeCreationInput">
                    <ErrorMessage name="description" component="span" />
                    <label htmlFor="Description">Description:</label>
                    <Field
                      type="textarea"
                      as="textarea"
                      name="description"
                      id="Description"
                      placeholder="Describe your node"
                    />
                  </div>
                  <div className="QueryInput NodeCreationInput">
                    <ErrorMessage name="query" component="span" />
                    <label htmlFor="Query">Query</label>
                    <NodeQueryField djClient={djClient} />
                  </div>
                  <div className="NodeModeInput NodeCreationInput">
                    <ErrorMessage name="mode" component="span" />
                    <label htmlFor="Mode">Mode</label>
                    <Field as="select" name="mode" id="Mode">
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
