/**
 * Add or edit tags
 */
import { ErrorMessage, Field, Form, Formik } from 'formik';

import NamespaceHeader from '../../components/NamespaceHeader';
import React, { useContext } from 'react';
import DJClientContext from '../../providers/djclient';
import 'styles/node-creation.scss';
import { displayMessageAfterSubmit } from '../../../utils/form';

export function AddEditTagPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const initialValues = {
    name: '',
  };

  const validator = values => {
    const errors = {};
    if (!values.name) {
      errors.name = 'Required';
    }
    return errors;
  };

  const handleSubmit = async (values, { setSubmitting, setStatus }) => {
    const { status, json } = await djClient.addTag(
      values.name,
      values.display_name,
      values.tag_type,
      values.description,
    );
    if (status === 200 || status === 201) {
      setStatus({
        success: (
          <>
            Successfully added tag{' '}
            <a href={`/tags/${json.name}`}>{json.display_name}</a>.
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
            Add{' '}
            <span className={`node_type__source node_type_creation_heading`}>
              Tag
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
                        <div className="NodeCreationInput">
                          <ErrorMessage name="name" component="span" />
                          <label htmlFor="name">Name</label>
                          <Field
                            type="text"
                            name="name"
                            id="name"
                            placeholder="Tag Name"
                          />
                        </div>
                        <br />
                        <div className="FullNameInput NodeCreationInput">
                          <ErrorMessage name="display_name" component="span" />
                          <label htmlFor="display_name">Display Name</label>
                          <Field
                            type="text"
                            name="display_name"
                            id="display_name"
                            placeholder="Display Name"
                            className="FullNameField"
                          />
                        </div>
                        <br />
                        <div className="NodeCreationInput">
                          <ErrorMessage name="tag_type" component="span" />
                          <label htmlFor="tag_type">Tag Type</label>
                          <Field
                            type="text"
                            name="tag_type"
                            id="tag_type"
                            placeholder="Tag Type"
                          />
                        </div>
                        <div className="DescriptionInput NodeCreationInput">
                          <ErrorMessage name="description" component="span" />
                          <label htmlFor="description">Description</label>
                          <Field
                            type="textarea"
                            as="textarea"
                            name="description"
                            id="Description"
                            placeholder="Describe the tag"
                          />
                        </div>
                        <button type="submit" disabled={isSubmitting}>
                          Add Tag
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
