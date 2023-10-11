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
  const [engines, setEngines] = useState([]);
  const [defaultEngine, setDefaultEngine] = useState('');

  const ref = useRef(null);

  useEffect(() => {
    const fetchData = async () => {
      const engines = await djClient.engines();
      setEngines(engines);
      setDefaultEngine(
        engines && engines.length > 0
          ? engines[0].name + '__' + engines[0].version
          : '',
      );
    };
    fetchData().catch(console.error);
    const handleClickOutside = event => {
      if (ref.current && !ref.current.contains(event.target)) {
        setPopoverAnchor(false);
      }
    };
    document.addEventListener('click', handleClickOutside, true);
    return () => {
      document.removeEventListener('click', handleClickOutside, true);
    };
  }, [djClient, setPopoverAnchor]);

  const configureMaterialization = async (
    values,
    { setSubmitting, setStatus },
  ) => {
    setSubmitting(false);
    const engineVersion = values.engine.split('__').slice(-1).join('');
    const engineName = values.engine.split('__').slice(0, -1).join('');
    const response = await djClient.materialize(
      values.node,
      engineName,
      engineVersion,
      values.schedule,
      values.config,
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
        <Formik
          initialValues={{
            node: node?.name,
            engine: defaultEngine,
            config: '{"spark": {"spark.executor.memory": "6g"}}',
            schedule: '@daily',
          }}
          onSubmit={configureMaterialization}
        >
          {function Render({ isSubmitting, status, setFieldValue }) {
            return (
              <Form>
                <h2>Configure Materialization</h2>
                {displayMessageAfterSubmit(status)}
                <span data-testid="edit-partition">
                  <label htmlFor="engine">Engine</label>
                  <Field as="select" name="engine">
                    <>
                      {engines?.map(engine => (
                        <option value={engine.name + '__' + engine.version}>
                          {engine.name} {engine.version}
                        </option>
                      ))}
                      <option value=""></option>
                    </>
                  </Field>
                </span>
                <input
                  hidden={true}
                  name="node"
                  value={node?.name}
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
                    placeholder="Optional engine-specific configuration (i.e., Spark conf etc)"
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
