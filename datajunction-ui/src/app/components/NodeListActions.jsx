import DJClientContext from '../providers/djclient';
import * as React from 'react';
import DeleteIcon from '../icons/DeleteIcon';
import EditIcon from '../icons/EditIcon';
import { Form, Formik } from 'formik';
import { useContext } from 'react';
import { displayMessageAfterSubmit } from '../../utils/form';
import Tooltip from './Tooltip';

export default function NodeListActions({ nodeName, iconSize = 20 }) {
  const [deleted, setDeleted] = React.useState(false);

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
      // Delay hiding component so success message is visible briefly
      setTimeout(() => setDeleted(true), 1500);
    } else {
      setStatus({
        failure: `${json.message}`,
      });
    }
  };

  const initialValues = {
    nodeName: nodeName,
  };

  if (deleted) {
    return null;
  }

  return (
    <div
      style={{ display: 'inline-flex', alignItems: 'center', gap: '0.25rem' }}
    >
      <Tooltip content="Edit node">
        <a href={`/nodes/${nodeName}/edit`} aria-label={`Edit ${nodeName}`}>
          <EditIcon size={iconSize} />
        </a>
      </Tooltip>
      <Formik initialValues={initialValues} onSubmit={deleteNode}>
        {function Render({ status, setFieldValue }) {
          return (
            <Form
              className="deleteNode"
              style={{ display: 'flex', alignItems: 'flex-start' }}
            >
              {displayMessageAfterSubmit(status)}
              {
                <>
                  <Tooltip content="Delete node">
                    <button
                      type="submit"
                      aria-label={`Delete ${nodeName}`}
                      style={{
                        marginLeft: 0,
                        all: 'unset',
                        color: '#005c72',
                        cursor: 'pointer',
                      }}
                    >
                      <DeleteIcon size={iconSize} />
                    </button>
                  </Tooltip>
                </>
              }
            </Form>
          );
        }}
      </Formik>
    </div>
  );
}
