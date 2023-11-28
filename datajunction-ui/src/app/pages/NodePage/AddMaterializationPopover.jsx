import { useContext, useEffect, useRef, useState } from 'react';
import * as React from 'react';
import DJClientContext from '../../providers/djclient';
import { ErrorMessage, Field, Form, Formik } from 'formik';
import { displayMessageAfterSubmit, labelize } from '../../../utils/form';

export default function AddMaterializationPopover({ node, onSubmit }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [popoverAnchor, setPopoverAnchor] = useState(false);
  const [options, setOptions] = useState([]);
  const [jobs, setJobs] = useState([]);

  const ref = useRef(null);

  useEffect(() => {
    const fetchData = async () => {
      const options = await djClient.materializationInfo();
      setOptions(options);
      const allowedJobs = options.job_types?.filter(job =>
        job.allowed_node_types.includes(node.type),
      );
      setJobs(allowedJobs);
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
    const config = JSON.parse(values.config);
    config.lookback_window = values.lookback_window;
    console.log('values', values);
    const response = await djClient.materialize(
      values.node,
      values.job_type,
      values.strategy,
      values.schedule,
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
        aria-label="AddMaterialization"
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
            job_type: 'spark_sql',
            strategy: 'full',
            config: '{"spark": {"spark.executor.memory": "6g"}}',
            schedule: '@daily',
            lookback_window: '1 DAY',
          }}
          onSubmit={configureMaterialization}
        >
          {function Render({ isSubmitting, status, setFieldValue }) {
            return (
              <Form>
                <h2>Configure Materialization</h2>
                {displayMessageAfterSubmit(status)}
                <span data-testid="job-type">
                  <label htmlFor="job_type">Job Type</label>
                  <Field as="select" name="job_type">
                    <>
                      {jobs?.map(job => (
                        <option key={job.name} value={job.name}>
                          {job.label}
                        </option>
                      ))}
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
                <span data-testid="edit-partition">
                  <label htmlFor="strategy">Strategy</label>
                  <Field as="select" name="strategy">
                    <>
                      {options.strategies?.map(strategy => (
                        <option value={strategy.name}>{strategy.label}</option>
                      ))}
                    </>
                  </Field>
                </span>
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
                  <label htmlFor="Config">Lookback Window</label>
                  <Field
                    type="text"
                    name="lookback_window"
                    id="lookback_window"
                    placeholder="1 DAY"
                    default="1 DAY"
                  />
                </div>
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
