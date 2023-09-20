import DJClientContext from '../providers/djclient';
import * as React from 'react';
import DeleteIcon from '../icons/DeleteIcon';
import { Form, Formik } from 'formik';
import { useContext } from 'react';
import { displayMessageAfterSubmit } from '../../utils/form';

export default function DeleteNode({ nodeName }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const deleteNode = async (values, { setSubmitting, setStatus }) => {
    const { status, json } = await djClient.deactivate(values.nodeName);
    if (status === 200 || status === 201 || status === 204) {
      setStatus({
        success: <>Successfully deleted node {values.nodeName}</>,
      });
    } else {
      setStatus({
        failure: `${json.message}`,
      });
    }
    setSubmitting(false);
  };

  const initialValues = {
    nodeName: nodeName,
  };

  return (
    <Formik initialValues={initialValues} onSubmit={deleteNode}>
      {function Render({ isSubmitting, status, setFieldValue }) {
        return (
          <Form className="deleteNode">
            {displayMessageAfterSubmit(status)}
            {
              <>
                <button
                  type="submit"
                  disabled={isSubmitting}
                  style={{
                    marginLeft: 0,
                    all: 'unset',
                    color: '#005c72',
                    cursor: 'pointer',
                  }}
                >
                  <DeleteIcon />
                </button>
              </>
            }
          </Form>
        );
      }}
    </Formik>
  );
}
