import DJClientContext from '../providers/djclient';
import ValidIcon from '../icons/ValidIcon';
import AlertIcon from '../icons/AlertIcon';
import * as React from 'react';
import DeleteIcon from '../icons/DeleteIcon';
import { Form, Formik } from 'formik';
import { useContext } from 'react';

export default function DeleteNode({ nodeName }) {
  console.log('nodeName', nodeName);

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

  const displayMessageAfterSubmit = status => {
    return status?.success !== undefined ? (
      <div className="message success">
        <ValidIcon />
        {status?.success}
      </div>
    ) : status?.failure !== undefined ? (
      alertMessage(status?.failure)
    ) : (
      ''
    );
  };

  const alertMessage = message => {
    return (
      <div className="message alert">
        <AlertIcon />
        {message}
      </div>
    );
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
