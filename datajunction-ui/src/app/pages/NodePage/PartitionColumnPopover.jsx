import { useContext, useEffect, useRef, useState } from 'react';
import * as React from 'react';
import DJClientContext from '../../providers/djclient';
import { Field, Form, Formik } from 'formik';
import EditIcon from '../../icons/EditIcon';
import { displayMessageAfterSubmit } from '../../../utils/form';

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
  };

  const removePartition = async setStatus => {
    const response = await djClient.removePartition(node.name, column.name);
    if (response.status === 200 || response.status === 201) {
      setStatus({ success: 'Partition removed' });
      onSubmit();
      setPopoverAnchor(false);
    } else {
      setStatus({ failure: `${response.json.message}` });
    }
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
        className="popover partition-popover"
        role="dialog"
        aria-label="client-code"
        style={{ display: popoverAnchor === false ? 'none' : 'block' }}
        ref={ref}
      >
        <Formik
          initialValues={{
            column: column.name,
            node: node.name,
            partition_type: column.partition?.type_ ?? '',
            format: column.partition?.format ?? 'yyyyMMdd',
            granularity: column.partition?.granularity ?? 'day',
          }}
          onSubmit={savePartition}
        >
          {function Render({ values, isSubmitting, status, setStatus }) {
            return (
              <Form>
                <div className="popover-header">
                  <div className="popover-title">Partition column</div>
                  <div className="popover-subtitle">
                    {popoverAnchor ? column.name : null}
                  </div>
                </div>
                {displayMessageAfterSubmit(status)}
                <div className="field-group" data-testid="edit-partition">
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
                </div>
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
                {values.partition_type === 'temporal' ? (
                  <>
                    <div className="field-group">
                      <label htmlFor="partitionFormat">Partition Format</label>
                      <Field
                        type="text"
                        name="format"
                        id="partitionFormat"
                        placeholder="e.g. yyyyMMdd"
                      />
                    </div>
                    <div className="field-group">
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
                    </div>
                  </>
                ) : null}
                <div
                  className="button-row"
                  style={{
                    justifyContent: column.partition
                      ? 'space-between'
                      : 'flex-end',
                  }}
                >
                  {column.partition ? (
                    <button
                      className="remove-link"
                      type="button"
                      aria-label="RemovePartition"
                      onClick={() => removePartition(setStatus)}
                    >
                      Remove partition
                    </button>
                  ) : null}
                  <button
                    className="add_node"
                    type="submit"
                    aria-label="SaveEditColumn"
                    aria-hidden="false"
                  >
                    Save
                  </button>
                </div>
              </Form>
            );
          }}
        </Formik>
      </div>
    </>
  );
}
