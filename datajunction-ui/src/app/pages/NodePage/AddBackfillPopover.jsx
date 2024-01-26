import { useContext, useEffect, useRef, useState } from 'react';
import * as React from 'react';
import DJClientContext from '../../providers/djclient';
import { Field, Form, Formik } from 'formik';
import { displayMessageAfterSubmit } from '../../../utils/form';

export default function AddBackfillPopover({
  node,
  materialization,
  onSubmit,
}) {
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

  const partitionColumns = node.columns.filter(col => col.partition !== null);

  const temporalPartitionColumns = partitionColumns.filter(
    col => col.partition.type_ === 'temporal',
  );

  const initialValues = {
    node: node.name,
    materializationName: materialization.name,
    partitionColumn:
      temporalPartitionColumns.length > 0
        ? temporalPartitionColumns[0].name
        : '',
    from: '',
    to: '',
  };

  const savePartition = async (values, { setSubmitting, setStatus }) => {
    setSubmitting(false);
    const response = await djClient.runBackfill(
      values.node,
      values.materializationName,
      values.partitionColumn,
      values.from,
      values.to,
    );
    if (response.status === 200 || response.status === 201) {
      setStatus({ success: 'Saved!' });
    } else {
      setStatus({
        failure: `${response.json.message}`,
      });
    }
    onSubmit();
    window.location.reload();
  };

  return (
    <>
      <button
        className="edit_button"
        aria-label="AddBackfill"
        tabIndex="0"
        onClick={() => {
          setPopoverAnchor(!popoverAnchor);
        }}
      >
        <span className="add_node">+ Add Backfill</span>
      </button>
      <div
        className="fade modal-backdrop in"
        style={{ display: popoverAnchor === false ? 'none' : 'block' }}
      ></div>
      <div
        className="centerPopover"
        role="dialog"
        aria-label="client-code"
        style={{
          display: popoverAnchor === false ? 'none' : 'block',
          width: '50%',
        }}
        ref={ref}
      >
        <Formik initialValues={initialValues} onSubmit={savePartition}>
          {function Render({ isSubmitting, status, setFieldValue }) {
            return (
              <Form>
                {displayMessageAfterSubmit(status)}
                <h2>Run Backfill</h2>
                <span data-testid="edit-partition">
                  <label
                    htmlFor="materializationName"
                    style={{ paddingBottom: '1rem' }}
                  >
                    Materialization Name
                  </label>
                  <Field
                    as="select"
                    name="materializationName"
                    id="materializationName"
                    disabled={true}
                  >
                    <option value={materialization?.name}>
                      {materialization?.name}{' '}
                    </option>
                  </Field>
                </span>
                <br />
                <br />
                <label htmlFor="partition" style={{ paddingBottom: '1rem' }}>
                  Partition Range
                </label>
                {node.columns
                  .filter(col => col.partition !== null)
                  .map(col => {
                    return (
                      <div
                        className="partition__full"
                        key={col.name}
                        style={{ width: '50%' }}
                      >
                        <div className="partition__header">
                          {col.display_name}
                        </div>
                        <div className="partition__body">
                          <span style={{ padding: '0.5rem' }}>From</span>{' '}
                          <Field
                            type="text"
                            name="from"
                            id={`${col.name}__from`}
                            placeholder="20230101"
                            default="20230101"
                            style={{ width: '7rem', paddingRight: '1rem' }}
                          />{' '}
                          <span style={{ padding: '0.5rem' }}>To</span>
                          <Field
                            type="text"
                            name="to"
                            id={`${col.name}__to`}
                            placeholder="20230102"
                            default="20230102"
                            style={{ width: '7rem' }}
                          />
                        </div>
                      </div>
                    );
                  })}
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
