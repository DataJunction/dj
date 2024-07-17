import { useContext, useEffect, useRef, useState } from 'react';
import * as React from 'react';
import DJClientContext from '../../providers/djclient';
import { Field, Form, Formik } from 'formik';
import { FormikSelect } from '../AddEditNodePage/FormikSelect';
import EditIcon from '../../icons/EditIcon';
import { displayMessageAfterSubmit } from '../../../utils/form';
import LoadingIcon from '../../icons/LoadingIcon';
import { NodeQueryField } from '../AddEditNodePage/NodeQueryField';

export default function LinkComplexDimensionPopover({ link, onSubmit }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [popoverAnchor, setPopoverAnchor] = useState(false);
  const ref = useRef(null);

  useEffect(() => {
    const handleClickOutside = event => {
      if (ref.current && !ref.current.contains(event.target)) {
        setPopoverAnchor(false);
      }
    };
    document.addEventListener('click', handleClickOutside, true);
    return () => {
      document.removeEventListener('click', handleClickOutside, true);
    };
  }, [setPopoverAnchor]);

  const handleSubmit = async (
    { node, column, dimension },
    { setSubmitting, setStatus },
  ) => {
    onSubmit();
  };

  const linkDimension = async (node, column, dimension, setStatus) => {
    const response = await djClient.linkDimension(node, column, dimension);
    if (response.status === 200 || response.status === 201) {
      setStatus({ success: 'Saved!' });
    } else {
      setStatus({
        failure: `${response.json.message}`,
      });
    }
  };

  const unlinkDimension = async (node, column, currentDimension, setStatus) => {
    const response = await djClient.unlinkDimension(
      node,
      column,
      currentDimension,
    );
    if (response.status === 200 || response.status === 201) {
      setStatus({ success: 'Removed dimension link!' });
    } else {
      setStatus({
        failure: `${response.json.message}`,
      });
    }
  };

  return (
    <>
      <button
        className="edit_button"
        aria-label="LinkDimension"
        tabIndex="0"
        onClick={() => {
          setPopoverAnchor(!popoverAnchor);
        }}
      >
        <EditIcon />
      </button>
      <div
        className="popover"
        role="dialog"
        aria-label="client-code"
        style={{ display: popoverAnchor === false ? 'none' : 'block' }}
        ref={ref}
      >
        <Formik
          initialValues={{
            link: link,
          }}
          onSubmit={handleSubmit}
        >
          {function Render({ isSubmitting, status, setFieldValue }) {
            console.log('link', link);
            return (
              <Form>
                {displayMessageAfterSubmit(status)}
                <span data-testid="link-dimension"></span>
                <input
                  // hidden={true}
                  disabled={true}
                  name="dimension"
                  value={link.dimension.name}
                  readOnly={true}
                />
                <label htmlFor={'join_type'}>Join Type</label>
                <input name="join_type" value={link.join_type} />
                <label htmlFor={'join_sql'}>Join On</label>
                <div style={{ width: '50%' }}>
                  <NodeQueryField
                    djClient={djClient}
                    value={link.join_sql ? link.join_sql : ''}
                  />
                </div>
                <button
                  className="add_node"
                  type="submit"
                  aria-label="SaveLinkDimension"
                  aria-hidden="false"
                  disabled={isSubmitting}
                >
                  {isSubmitting ? <LoadingIcon /> : 'Save'}
                </button>
              </Form>
            );
          }}
        </Formik>
      </div>
    </>
  );
}
