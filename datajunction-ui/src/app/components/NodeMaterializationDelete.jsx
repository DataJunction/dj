import DJClientContext from '../providers/djclient';
import * as React from 'react';
import DeleteIcon from '../icons/DeleteIcon';
import { Form, Formik } from 'formik';
import { useContext } from 'react';
import { displayMessageAfterSubmit } from '../../utils/form';

export default function NodeMaterializationDelete({
  nodeName,
  materializationName,
  nodeVersion = null,
}) {
  const [deleteButton, setDeleteButton] = React.useState(<DeleteIcon />);

  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const deleteNode = async (values, { setStatus }) => {
    if (
      !window.confirm(
        'Deleting materialization job ' +
          values.materializationName +
          ' for node version ' +
          values.nodeVersion +
          '. Are you sure?',
      )
    ) {
      return;
    }
    const { status, json } = await djClient.deleteMaterialization(
      values.nodeName,
      values.materializationName,
      values.nodeVersion,
    );
    if (status === 200 || status === 201 || status === 204) {
      window.location.reload();
      setStatus({
        success: (
          <>
            Successfully deleted materialization job:{' '}
            {values.materializationName}
          </>
        ),
      });
      setDeleteButton(''); // hide the Delete button
    } else {
      setStatus({
        failure: `${json.message}`,
      });
    }
  };

  const initialValues = {
    nodeName: nodeName,
    materializationName: materializationName,
    nodeVersion: nodeVersion,
  };

  return (
    <div>
      <Formik
        key={`${nodeName}-${materializationName}-${nodeVersion}`}
        initialValues={initialValues}
        enableReinitialize={true}
        onSubmit={deleteNode}
      >
        {function Render({ status, setFieldValue }) {
          return (
            <Form className="deleteNode">
              {displayMessageAfterSubmit(status)}
              {
                <>
                  <button
                    type="submit"
                    style={{
                      marginLeft: 0,
                      all: 'unset',
                      color: '#005c72',
                      cursor: 'pointer',
                    }}
                  >
                    {deleteButton}
                  </button>
                </>
              }
            </Form>
          );
        }}
      </Formik>
    </div>
  );
}
