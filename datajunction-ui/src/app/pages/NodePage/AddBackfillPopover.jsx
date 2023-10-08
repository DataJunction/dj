import { useContext, useEffect, useRef, useState } from 'react';
import * as React from 'react';
import DJClientContext from '../../providers/djclient';
import { ErrorMessage, Field, Form, Formik } from 'formik';
import { FormikSelect } from '../AddEditNodePage/FormikSelect';
import EditIcon from '../../icons/EditIcon';
import { displayMessageAfterSubmit, labelize } from '../../../utils/form';

export default function AddMaterializationPopover({ node, onSubmit }) {
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
    { node, engine, engineVersion, schedule, config },
    { setSubmitting, setStatus },
  ) => {
    setSubmitting(false);
    const response = await djClient.materialize(
      node,
      engine,
      engineVersion || '',
      schedule,
      config,
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
        <span className="add_node">+ Add Materialization</span>
      </button>
      <div className="fade modal-backdrop in" style={{ display: popoverAnchor === false ? 'none' : 'block'}}></div>
      <div
        className="popover"
        role="dialog"
        aria-label="client-code"
        style={{ display: popoverAnchor === false ? 'none' : 'block', border: '1px dashed #c5c5c5', width: '50%' }}
        ref={ref}
      >
        <Formik
          initialValues={{
            node: node.name,
            engineName: '',
            engineVersion: '',
            config: '',
            schedule: '@daily',
          }}
          onSubmit={savePartition}
        >
          {function Render({ isSubmitting, status, setFieldValue }) {
            return (
              <Form>
                {/*<h4>Set Partition</h4>*/}
                {displayMessageAfterSubmit(status)}
                <span data-testid="edit-partition">
                  <label htmlFor="engine">Engine</label>
                  <Field as="select" name="engine" id="engine">
                    <option value="SPARKSQL">SPARKSQL 3.3</option>
                    <option value="DRUIDSQL">DRUIDSQL</option>
                  </Field>
                </span>
                <input
                  hidden={true}
                  name="node"
                  value={node.name}
                  readOnly={true}
                />
                <br />
                <br />
                <label htmlFor="schedule">Schedule</label>
                <Field
                  type="text"
                  name="schedule"
                  id="schedule"
                  placeholder="Cron"
                  default="@daily"
                />
                <br />
                <br />
                <div className="DescriptionInput">
                  <ErrorMessage name="description" component="span" />
                  <label htmlFor="Config">Config</label>
                  <Field
                    type="textarea"
                    as="textarea"
                    name="config"
                    id="Config"
                    placeholder="Configuration (i.e., Spark conf etc)"
                    default={{ spark: {} }}
                  />
                </div>
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
