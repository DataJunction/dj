import { ErrorMessage } from 'formik';
import { FormikSelect } from './FormikSelect';
import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';

export const NamespaceField = ({ initialNamespace }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  const [namespaces, setNamespaces] = useState([]);

  // Get namespaces, only necessary when creating a node
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
  }, [djClient]);

  return (
    <div className="NamespaceInput">
      <ErrorMessage name="namespace" component="span" />
      <label htmlFor="namespace">Namespace *</label>
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
  );
};
