/**
 * Node add + edit page for transforms, metrics, and dimensions. The creation and edit flow for these
 * node types is largely the same, with minor differences handled server-side. For the `query`
 * field, this page will render a CodeMirror SQL editor with autocompletion and syntax highlighting.
 */
import { ErrorMessage, Field, Form, Formik } from 'formik';

import NamespaceHeader from '../../components/NamespaceHeader';
import React, { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import 'styles/node-creation.scss';
import { FormikSelect } from '../AddEditNodePage/FormikSelect';
import { displayMessageAfterSubmit } from '../../../utils/form';

export function RegisterTablePage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [catalogs, setCatalogs] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      const catalogs = await djClient.catalogs();
      setCatalogs(
        catalogs.map(catalog => {
          return { value: catalog.name, label: catalog.name };
        }),
      );
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.namespaces]);

  const initialValues = {
    catalog: '',
    schema: '',
    table: '',
  };

  const validator = values => {
    const errors = {};
    if (!values.table) {
      errors.table = 'Required';
    }
    if (!values.schema) {
      errors.schema = 'Required';
    }
    return errors;
  };

  const handleSubmit = async (values, { setSubmitting, setStatus }) => {
    const { status, json } = await djClient.registerTable(
      values.catalog,
      values.schema,
      values.table,
    );
    if (status === 200 || status === 201) {
      setStatus({
        success: (
          <>
            Successfully registered source node{' '}
            <a href={`/nodes/${json.name}`}>{json.name}</a>, which references
            table {values.catalog}.{values.schema}.{values.table}.
          </>
        ),
      });
    } else {
      setStatus({
        failure: `${json.message}`,
      });
    }
    setSubmitting(false);
    window.scrollTo({ top: 0, left: 0, behavior: 'smooth' });
  };

  return (
    <div className="mid">
      <NamespaceHeader namespace="" />
      <div className="card">
        <div className="card-header">
          <h2>
            Register{' '}
            <span className={`node_type__source node_type_creation_heading`}>
              Source
            </span>
          </h2>
          <center>
            <Formik
              initialValues={initialValues}
              validate={validator}
              onSubmit={handleSubmit}
            >
              {function Render({ isSubmitting, status }) {
                return (
                  <Form>
                    {displayMessageAfterSubmit(status)}
                    {
                      <>
                        <div className="SourceCreationInput">
                          <ErrorMessage name="catalog" component="span" />
                          <label htmlFor="catalog">Catalog</label>
                          <span data-testid="choose-catalog">
                            <FormikSelect
                              selectOptions={catalogs}
                              formikFieldName="catalog"
                              placeholder="Choose Catalog"
                              defaultValue={catalogs[0]}
                            />
                          </span>
                        </div>
                        <div className="SourceCreationInput">
                          <ErrorMessage name="schema" component="span" />
                          <label htmlFor="schema">Schema</label>
                          <Field
                            type="text"
                            name="schema"
                            id="schema"
                            placeholder="Schema"
                          />
                        </div>
                        <div className="SourceCreationInput NodeCreationInput">
                          <ErrorMessage name="table" component="span" />
                          <label htmlFor="table">Table</label>
                          <Field
                            type="text"
                            name="table"
                            id="table"
                            placeholder="Table"
                          />
                        </div>
                        <button type="submit" disabled={isSubmitting}>
                          Register
                        </button>
                      </>
                    }
                  </Form>
                );
              }}
            </Formik>
          </center>
        </div>
      </div>
    </div>
  );
}
