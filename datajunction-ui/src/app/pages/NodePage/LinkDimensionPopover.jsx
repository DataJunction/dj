import { useContext, useState } from 'react';
import * as React from 'react';
import DJClientContext from '../../providers/djclient';
import { Form, Formik } from 'formik';
import { FormikSelect } from '../AddEditNodePage/FormikSelect';
import EditIcon from '../../icons/EditIcon';
import { displayMessageAfterSubmit } from '../../../utils/form';

export default function LinkDimensionPopover({ column, node, options }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [popoverAnchor, setPopoverAnchor] = useState(false);

  const linkDimension = async (
    { node, column, dimension },
    { setSubmitting, setStatus },
  ) => {
    setSubmitting(false);
    const response = await djClient.linkDimension(node, column, dimension);
    if (response.status === 200 || response.status === 201) {
      setStatus({ success: 'Saved!' });
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
            dimension: '',
          }}
          onSubmit={linkDimension}
        >
          {function Render({ isSubmitting, status, setFieldValue }) {
            return (
              <Form>
                {displayMessageAfterSubmit(status)}
                <FormikSelect
                  selectOptions={options}
                  formikFieldName="dimension"
                  placeholder="Select dimension to link"
                  className=""
                  defaultValue={
                    column.dimension
                      ? {
                          value: column.dimension.name,
                          label: column.dimension.name,
                        }
                      : ''
                  }
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
