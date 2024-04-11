import DJClientContext from '../providers/djclient';
import * as React from 'react';
import DeleteIcon from '../icons/DeleteIcon';
import EditIcon from '../icons/EditIcon';
import { Form, Formik } from 'formik';
import { useContext } from 'react';
import { displayMessageAfterSubmit } from '../../utils/form';

export default function NodeListActions({ nodeName }) {
  const [editButton, setEditButton] = React.useState(<EditIcon />);
  const [deleteButton, setDeleteButton] = React.useState(<DeleteIcon />);

  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const deleteNode = async (values, { setStatus }) => {
    if (
      !window.confirm('Deleting node ' + values.nodeName + '. Are you sure?')
    ) {
      return;
    }
    const { status, json } = await djClient.deactivate(values.nodeName);
    if (status === 200 || status === 201 || status === 204) {
      setStatus({
        success: <>Successfully deleted node {values.nodeName}</>,
      });
      setEditButton(''); // hide the Edit button
      setDeleteButton(''); // hide the Delete button
    } else {
      setStatus({
        failure: `${json.message}`,
      });
    }
  };

  const initialValues = {
    nodeName: nodeName,
  };

  return (
    <div>
      <a href={`/nodes/${nodeName}/edit`} style={{ marginLeft: '0.5rem' }}>
        {editButton}
      </a>
      <Formik initialValues={initialValues} onSubmit={deleteNode}>
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
