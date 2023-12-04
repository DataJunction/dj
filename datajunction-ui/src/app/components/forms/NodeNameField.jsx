import { ErrorMessage, Field } from 'formik';
import { FormikSelect } from '../../pages/AddEditNodePage/FormikSelect';
import { FullNameField } from '../../pages/AddEditNodePage/FullNameField';
import React, { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { useParams } from 'react-router-dom';

/*
 * This component creates the namespace selector, display name input, and
 * derived name fields in a form. It can be reused any time we need to create
 * a new node.
 */

export default function NodeNameField() {
  const [namespaces, setNamespaces] = useState([]);
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  let { initialNamespace } = useParams();

  useEffect(() => {
    const fetchData = async () => {
      const namespaces = await djClient.namespaces();
      setNamespaces(
        namespaces.map(m => ({
          value: m['namespace'],
          label: m['namespace'],
        })),
      );
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.metrics]);

  return (
    <>
      <div className="NamespaceInput">
        <ErrorMessage name="namespace" component="span" />
        <label htmlFor="react-select-3-input">Namespace *</label>
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
        <label htmlFor="displayName">Display Name *</label>
        <Field
          type="text"
          name="display_name"
          id="displayName"
          placeholder="Human readable display name"
        />
      </div>
      <div className="FullNameInput NodeCreationInput">
        <ErrorMessage name="name" component="span" />
        <label htmlFor="FullName">Full Name</label>
        <FullNameField type="text" name="name" />
      </div>
    </>
  );
}
