import { useContext, useEffect, useRef, useState } from 'react';
import * as React from 'react';
import DJClientContext from '../../providers/djclient';
import { Form, Formik } from 'formik';
import { FormikSelect } from '../AddEditNodePage/FormikSelect';
import EditIcon from '../../icons/EditIcon';
import { displayMessageAfterSubmit, labelize } from '../../../utils/form';

export default function EditColumnPopover({ column, node, options, onSubmit }) {
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

  const saveAttributes = async (
    { node, column, attributes },
    { setSubmitting, setStatus },
  ) => {
    setSubmitting(false);
    const response = await djClient.setAttributes(node, column, attributes);
    if (response.status === 200 || response.status === 201) {
      setStatus({ success: 'Saved!' });
    } else {
      setStatus({
        failure: `${response.json.message}`,
      });
    }
    onSubmit();
    // window.location.reload();
  };

  return (
    <>
      <button
        className="edit_button"
        aria-label="EditColumn"
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
            column: column.name,
            node: node.name,
            attributes: column.attributes.map(attr => attr.attribute_type.name),
          }}
          onSubmit={saveAttributes}
        >
          {function Render({ isSubmitting, status, setFieldValue }) {
            return (
              <Form>
                {displayMessageAfterSubmit(status)}
                <span data-testid="edit-attributes">
                  <FormikSelect
                    selectOptions={options}
                    formikFieldName="attributes"
                    placeholder="Select column attributes"
                    className=""
                    defaultValue={column.attributes.map(attr => {
                      return {
                        value: attr.attribute_type.name,
                        label: labelize(attr.attribute_type.name),
                      };
                    })}
                    isMulti={true}
                  />
                </span>
                <input
                  hidden={true}
                  name="column"
                  value={column.name}
                  readOnly={true}
                />
                <input
                  hidden={true}
                  name="node"
                  value={node.name}
                  readOnly={true}
                />
                <button
                  className="add_node"
                  type="submit"
                  aria-label="SaveEditColumn"
                  aria-hidden="false"
                >
                  Save
                </button>
              </Form>
            );
          }}
        </Formik>
      </div>
    </>
  );
}
