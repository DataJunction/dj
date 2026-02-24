import { useContext, useEffect, useRef, useState } from 'react';
import * as React from 'react';
import DJClientContext from '../../providers/djclient';
import { Field, Form, Formik } from 'formik';
import { displayMessageAfterSubmit } from '../../../utils/form';
import PartitionValueForm from './PartitionValueForm';
import LoadingIcon from '../../icons/LoadingIcon';

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
  const initialValues = {
    node: node.name,
    materializationName: materialization.name,
    partitionValues: {},
  };

  for (const partitionCol of partitionColumns) {
    if (partitionCol.partition.type_ === 'temporal') {
      initialValues.partitionValues[partitionCol.name] = {
        from: '',
        to: '',
      };
    } else {
      initialValues.partitionValues[partitionCol.name] = '';
    }
  }

  const runBackfill = async (values, setStatus) => {
    const response = await djClient.runBackfill(
      values.node,
      values.materializationName,
      Object.entries(values.partitionValues).map(entry => {
        if (typeof entry[1] === 'object' && entry[1] !== null) {
          return {
            columnName: entry[0],
            range: [entry[1].from, entry[1].to],
          };
        }
        return {
          columnName: entry[0],
          values: [entry[1]],
        };
      }),
    );
    if (response.status === 200 || response.status === 201) {
      setStatus({ success: 'Saved!' });
      return true;
    } else {
      setStatus({
        failure: `${response.json.message}`,
      });
      return false;
    }
  };

  const submitBackfill = async (values, { setSubmitting, setStatus }) => {
    const success = await runBackfill(values, setStatus);
    window.scrollTo({ top: 0, left: 0, behavior: 'smooth' });
    setSubmitting(false);
    if (success) {
      setPopoverAnchor(false);
      if (onSubmit) {
        onSubmit();
      }
    }
  };

  return (
    <>
      <button
        className="edit_button add_button"
        aria-label="AddBackfill"
        tabIndex="0"
        onClick={() => {
          setPopoverAnchor(!popoverAnchor);
        }}
      >
        <span className="add_button">+ Run</span>
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
          minWidth: '800px',
        }}
        ref={ref}
      >
        <Formik initialValues={initialValues} onSubmit={submitBackfill}>
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
                  Partition
                </label>
                {node.columns
                  .filter(col => col.partition !== null)
                  .map(col => {
                    return (
                      <PartitionValueForm
                        col={col}
                        materialization={materialization}
                        key={col.name}
                      />
                    );
                  })}
                <br />
                <button
                  className="add_node"
                  type="submit"
                  aria-label="SaveEditColumn"
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
