import { useContext, useState } from 'react';
import * as React from 'react';
import DJClientContext from '../../providers/djclient';
import { Form, Formik } from 'formik';
import { FormikSelect } from '../AddEditNodePage/FormikSelect';
import EditIcon from '../../icons/EditIcon';
import { displayMessageAfterSubmit, labelize } from '../../../utils/form';

export default function EditColumnPopover({ column, node, options, onSubmit }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [popoverAnchor, setPopoverAnchor] = useState(false);

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
        onClose={() => setPopoverAnchor(null)}
        style={{ display: popoverAnchor === false ? 'none' : 'block' }}
      >
        <Formik
          initialValues={{
            column: column.name,
            node: node.name,
            attributes: [],
          }}
          onSubmit={saveAttributes}
        >
          {function Render({ isSubmitting, status, setFieldValue }) {
            return (
              <Form>
                {displayMessageAfterSubmit(status)}
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
                <input hidden={true} name="column" value={column.name} />
                <input hidden={true} name="node" value={node.name} />
                <button className="add_node" type="submit">
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
