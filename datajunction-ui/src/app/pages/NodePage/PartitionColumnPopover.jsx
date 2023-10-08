import { useContext, useEffect, useRef, useState } from 'react';
import * as React from 'react';
import DJClientContext from '../../providers/djclient';
import { Field, Form, Formik } from 'formik';
import { FormikSelect } from '../AddEditNodePage/FormikSelect';
import EditIcon from '../../icons/EditIcon';
import { displayMessageAfterSubmit, labelize } from '../../../utils/form';

export default function PartitionColumnPopover({ column, node, onSubmit }) {
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

  const savePartition = async (
    { node, column, partition_type, partition_expression },
    { setSubmitting, setStatus },
  ) => {
    setSubmitting(false);
    console.log(partition_type, partition_expression);
    const response = await djClient.setPartition(
      node,
      column,
      partition_type,
      partition_expression,
    );
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
        aria-label="PartitionColumn"
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
            partition_type: 'temporal',
            partition_expression: '',
          }}
          onSubmit={savePartition}
        >
          {function Render({ isSubmitting, status, setFieldValue }) {
            return (
              <Form>
                {/*<h4>Set Partition</h4>*/}
                {displayMessageAfterSubmit(status)}
                <span data-testid="edit-partition">
                  <label htmlFor="react-select-3-input">Partition Type</label>
                  <Field as="select" name="partition_type" id="partitionType">
                    <option value="temporal">Temporal</option>
                    <option value="categorical">Categorical</option>
                  </Field>
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
                <br />
                <br />
                <label htmlFor="react-select-3-input">
                  Partition Expression
                </label>
                <Field
                  type="text"
                  name="partition_expression"
                  id="partitionExpression"
                  placeholder="Optional SQL expression"
                />
                <br />
                <br />
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
