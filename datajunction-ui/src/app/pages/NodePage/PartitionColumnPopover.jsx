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
    { node, column, partition_type, format, granularity },
    { setSubmitting, setStatus },
  ) => {
    setSubmitting(false);
    const response = await djClient.setPartition(
      node,
      column,
      partition_type,
      format,
      granularity,
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
            partition_type: '',
            format: 'yyyyMMdd',
            granularity: 'day',
          }}
          onSubmit={savePartition}
        >
          {function Render({ values, isSubmitting, status, setFieldValue }) {
            return (
              <Form>
                {displayMessageAfterSubmit(status)}
                <span data-testid="edit-partition">
                  <label htmlFor="partitionType">Partition Type</label>
                  <Field
                    as="select"
                    name="partition_type"
                    id="partitionType"
                    placeholder="Partition Type"
                  >
                    <option value=""></option>
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
                {values.partition_type === 'temporal' ? (
                  <>
                    <label htmlFor="partitionFormat">Partition Format</label>
                    <Field
                      type="text"
                      name="format"
                      id="partitionFormat"
                      placeholder="Optional temporal partition format (ex: yyyyMMdd)"
                    />
                    <br />
                    <br />
                    <label htmlFor="partitionGranularity">
                      Partition Granularity
                    </label>
                    <Field
                      as="select"
                      name="granularity"
                      id="partitionGranularity"
                      placeholder="Granularity"
                    >
                      <option value="day">Day</option>
                      <option value="hour">Hour</option>
                    </Field>
                  </>
                ) : (
                  ''
                )}
                <button
                  className="add_node"
                  type="submit"
                  aria-label="SaveEditColumn"
                  aria-hidden="false"
                >
                  Save
                </button>
                <button
                  className="delete_button"
                  type="button"
                  aria-label="RemovePartition"
                  aria-hidden="false"
                  onClick={() => {
                    setFieldValue('partition_type', '');
                    setFieldValue('format', '');
                    setFieldValue('granularity', '');
                    savePartition(
                      {
                        node: node.name,
                        column: column.name,
                        partition_type: '',
                        format: '',
                        granularity: '',
                      },
                      { setSubmitting: () => {}, setStatus: s => {} },
                    );
                  }}
                  style={{
                    marginLeft: '10px',
                    backgroundColor: '#dc3545',
                  }}
                >
                  Remove Partition
                </button>
              </Form>
            );
          }}
        </Formik>
      </div>
    </>
  );
}
