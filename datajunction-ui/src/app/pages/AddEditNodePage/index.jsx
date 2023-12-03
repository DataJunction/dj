/**
 * Node add + edit page for transforms, metrics, and dimensions. The creation and edit flow for these
 * node types is largely the same, with minor differences handled server-side. For the `query`
 * field, this page will render a CodeMirror SQL editor with autocompletion and syntax highlighting.
 */
import { ErrorMessage, Field, Form, Formik } from 'formik';

import NamespaceHeader from '../../components/NamespaceHeader';
import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import 'styles/node-creation.scss';
import AlertIcon from '../../icons/AlertIcon';
import { useParams, useNavigate } from 'react-router-dom';
import { FullNameField } from './FullNameField';
import { FormikSelect } from './FormikSelect';
import { NodeQueryField } from './NodeQueryField';
import { displayMessageAfterSubmit, labelize } from '../../../utils/form';

class Action {
  static Add = new Action('add');
  static Edit = new Action('edit');

  constructor(name) {
    this.name = name;
  }
}

export function AddEditNodePage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const navigate = useNavigate();

  let { nodeType, initialNamespace, name } = useParams();
  const action = name !== undefined ? Action.Edit : Action.Add;

  const [namespaces, setNamespaces] = useState([]);
  const [tags, setTags] = useState([]);
  const [metricUnits, setMetricUnits] = useState([]);
  const [metricDirections, setMetricDirections] = useState([]);

  const initialValues = {
    name: action === Action.Edit ? name : '',
    namespace: action === Action.Add ? initialNamespace : '',
    display_name: '',
    query: '',
    node_type: '',
    description: '',
    primary_key: '',
    mode: 'draft',
  };

  const validator = values => {
    const errors = {};
    if (!values.name) {
      errors.name = 'Required';
    }
    if (!values.query) {
      errors.query = 'Required';
    }
    return errors;
  };

  const handleSubmit = (values, { setSubmitting, setStatus }) => {
    if (action === Action.Add) {
      setTimeout(() => {
        createNode(values, setStatus);
        setSubmitting(false);
      }, 400);
    } else {
      setTimeout(() => {
        patchNode(values, setStatus);
        setSubmitting(false);
      }, 400);
    }
    window.scrollTo({ top: 0, left: 0, behavior: 'smooth' });
  };

  const pageTitle =
    action === Action.Add ? (
      <h2>
        Create{' '}
        <span className={`node_type__${nodeType} node_type_creation_heading`}>
          {nodeType}
        </span>
      </h2>
    ) : (
      <h2>Edit</h2>
    );

  const staticFieldsInEdit = node => (
    <>
      <div className="NodeNameInput NodeCreationInput">
        <label htmlFor="name">Name</label> {name}
      </div>
      <div className="NodeNameInput NodeCreationInput">
        <label htmlFor="name">Type</label> {node.type}
      </div>
    </>
  );

  const primaryKeyToList = primaryKey => {
    return primaryKey.split(',').map(columnName => columnName.trim());
  };

  const createNode = async (values, setStatus) => {
    const { status, json } = await djClient.createNode(
      nodeType,
      values.name,
      values.display_name,
      values.description,
      values.query,
      values.mode,
      values.namespace,
      values.primary_key ? primaryKeyToList(values.primary_key) : null,
      values.metric_direction,
      values.metric_unit,
    );
    if (status === 200 || status === 201) {
      if (values.tags) {
        await djClient.tagsNode(values.name, values.tags);
      }
      setStatus({
        success: (
          <>
            Successfully created {json.type} node{' '}
            <a href={`/nodes/${json.name}`}>{json.name}</a>!
          </>
        ),
      });
    } else {
      setStatus({
        failure: `${json.message}`,
      });
    }
  };

  const patchNode = async (values, setStatus) => {
    const { status, json } = await djClient.patchNode(
      values.name,
      values.display_name,
      values.description,
      values.query,
      values.mode,
      values.primary_key ? primaryKeyToList(values.primary_key) : null,
      values.metric_direction,
      values.metric_unit,
    );
    const tagsResponse = await djClient.tagsNode(
      values.name,
      values.tags.map(tag => tag),
    );
    if ((status === 200 || status === 201) && tagsResponse.status === 200) {
      setStatus({
        success: (
          <>
            Successfully updated {json.type} node{' '}
            <a href={`/nodes/${json.name}`}>{json.name}</a>!
          </>
        ),
      });
    } else {
      setStatus({
        failure: `${json.message}, ${tagsResponse.json.message}`,
      });
    }
  };

  const namespaceInput = (
    <div className="NamespaceInput">
      <ErrorMessage name="namespace" component="span" />
      <label htmlFor="react-select-3-input">Namespace *</label>
      <FormikSelect
        selectOptions={namespaces}
        formikFieldName="namespace"
        placeholder="Choose Namespace"
        defaultValue={{
          value: initialNamespace,
          label: initialNamespace,
        }}
      />
    </div>
  );

  const fullNameInput = (
    <div className="FullNameInput NodeCreationInput">
      <ErrorMessage name="name" component="span" />
      <label htmlFor="FullName">Full Name *</label>
      <FullNameField type="text" name="name" />
    </div>
  );

  const nodeCanBeEdited = nodeType => {
    return new Set(['transform', 'metric', 'dimension']).has(nodeType);
  };

  const updateFieldsWithNodeData = (data, setFieldValue) => {
    const fields = [
      'display_name',
      'query',
      'type',
      'description',
      'primary_key',
      'mode',
      'tags',
    ];
    fields.forEach(field => {
      if (
        field === 'primary_key' &&
        data[field] !== undefined &&
        Array.isArray(data[field])
      ) {
        data[field] = data[field].join(', ');
      } else {
        setFieldValue(field, data[field] || '', false);
      }
    });
    if (data.metric_metadata?.direction) {
      setFieldValue('metric_direction', data.metric_metadata.direction);
    }
    if (data.metric_metadata?.unit) {
      setFieldValue(
        'metric_unit',
        data.metric_metadata.unit.name.toLowerCase(),
      );
    }
  };

  const alertMessage = message => {
    return (
      <div className="message alert">
        <AlertIcon />
        {message}
      </div>
    );
  };

  // Get namespaces, only necessary when creating a node
  useEffect(() => {
    if (action === Action.Add) {
      const fetchData = async () => {
        const namespaces = await djClient.namespaces();
        setNamespaces(
          namespaces.map(m => ({
            value: m['namespace'],
            label: m['namespace'],
          })),
        );
      };
      fetchData().catch(console.error);
    }
  }, [action, djClient, djClient.metrics]);

  // Get list of tags
  useEffect(() => {
    const fetchData = async () => {
      const tags = await djClient.listTags();
      setTags(
        tags.map(tag => ({
          value: tag.name,
          label: tag.display_name,
        })),
      );
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.listTags]);

  // Get metric metadata values
  useEffect(() => {
    const fetchData = async () => {
      const metadata = await djClient.listMetricMetadata();
      setMetricDirections(metadata.directions);
      setMetricUnits(metadata.units);
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <div className="mid">
      <NamespaceHeader namespace="" />
      <div className="card">
        <div className="card-header">
          {pageTitle}
          <center>
            <Formik
              initialValues={initialValues}
              validate={validator}
              onSubmit={handleSubmit}
            >
              {function Render({ isSubmitting, status, setFieldValue }) {
                const [node, setNode] = useState([]);
                const [selectTags, setSelectTags] = useState(null);
                const [message, setMessage] = useState('');

                const tagsInput = (
                  <div
                    className="TagsInput"
                    style={{ width: '25%', margin: '1rem 0 1rem 1.2rem' }}
                  >
                    <ErrorMessage name="tags" component="span" />
                    <label htmlFor="react-select-3-input">Tags</label>
                    <span data-testid="select-tags">
                      {action === Action.Edit ? (
                        selectTags
                      ) : (
                        <FormikSelect
                          isMulti={true}
                          selectOptions={tags}
                          formikFieldName="tags"
                          placeholder="Choose Tags"
                        />
                      )}
                    </span>
                  </div>
                );

                const metricMetadataInput = (
                  <>
                    <div
                      className="MetricDirectionInput NodeCreationInput"
                      style={{ width: '25%' }}
                    >
                      <ErrorMessage name="metric_direction" component="span" />
                      <label htmlFor="MetricDirection">Metric Direction</label>
                      <Field
                        as="select"
                        name="metric_direction"
                        id="MetricDirection"
                      >
                        <option value=""></option>
                        {metricDirections.map(direction => (
                          <option value={direction}>
                            {labelize(direction)}
                          </option>
                        ))}
                      </Field>
                    </div>
                    <div
                      className="MetricUnitInput NodeCreationInput"
                      style={{ width: '25%' }}
                    >
                      <ErrorMessage name="metric_unit" component="span" />
                      <label htmlFor="MetricUnit">Metric Unit</label>
                      <Field as="select" name="metric_unit" id="MetricUnit">
                        <option value=""></option>
                        {metricUnits.map(unit => (
                          <option value={unit.name}>{unit.label}</option>
                        ))}
                      </Field>
                    </div>
                  </>
                );

                useEffect(() => {
                  const fetchData = async () => {
                    if (action === Action.Edit) {
                      const data = await djClient.node(name);

                      // Check if node exists
                      if (data.message !== undefined) {
                        setNode(null);
                        setMessage(`Node ${name} does not exist!`);
                        return;
                      }

                      // Check if node type can be edited
                      if (!nodeCanBeEdited(data.type)) {
                        navigate(`/nodes/${data.name}/edit-cube`);
                      }

                      // Update fields with existing data to prepare for edit
                      updateFieldsWithNodeData(data, setFieldValue);
                      setNode(data);
                      setSelectTags(
                        <FormikSelect
                          isMulti={true}
                          selectOptions={tags}
                          formikFieldName="tags"
                          placeholder="Choose Tags"
                          defaultValue={data.tags.map(t => {
                            return { value: t.name, label: t.display_name };
                          })}
                        />,
                      );
                    }
                  };
                  fetchData().catch(console.error);
                }, [setFieldValue, tags]);
                return (
                  <Form>
                    {displayMessageAfterSubmit(status)}
                    {action === Action.Edit && message ? (
                      alertMessage(message)
                    ) : (
                      <>
                        {action === Action.Add
                          ? namespaceInput
                          : staticFieldsInEdit(node)}
                        <div className="DisplayNameInput NodeCreationInput">
                          <ErrorMessage name="display_name" component="span" />
                          <label htmlFor="displayName">Display Name *</label>
                          <Field
                            type="text"
                            name="display_name"
                            id="displayName"
                            placeholder="Human readable display name"
                          />
                        </div>
                        {action === Action.Add ? fullNameInput : ''}
                        <div className="DescriptionInput NodeCreationInput">
                          <ErrorMessage name="description" component="span" />
                          <label htmlFor="Description">Description</label>
                          <Field
                            type="textarea"
                            as="textarea"
                            name="description"
                            id="Description"
                            placeholder="Describe your node"
                          />
                        </div>
                        {nodeType === 'metric' || node.type === 'metric'
                          ? metricMetadataInput
                          : ''}
                        <div className="QueryInput NodeCreationInput">
                          <ErrorMessage name="query" component="span" />
                          <label htmlFor="Query">Query *</label>
                          <NodeQueryField
                            djClient={djClient}
                            value={node.query ? node.query : ''}
                          />
                        </div>
                        <div className="PrimaryKeyInput NodeCreationInput">
                          <ErrorMessage name="primary_key" component="span" />
                          <label htmlFor="primaryKey">Primary Key</label>
                          <Field
                            type="text"
                            name="primary_key"
                            id="primaryKey"
                            placeholder="Comma-separated list of PKs"
                          />
                        </div>
                        {tagsInput}
                        <div className="NodeModeInput NodeCreationInput">
                          <ErrorMessage name="mode" component="span" />
                          <label htmlFor="Mode">Mode</label>
                          <Field as="select" name="mode" id="Mode">
                            <option value="draft">Draft</option>
                            <option value="published">Published</option>
                          </Field>
                        </div>

                        <button type="submit" disabled={isSubmitting}>
                          {action === Action.Add ? 'Create' : 'Save'} {nodeType}
                        </button>
                      </>
                    )}
                  </Form>
                );
              }}
            </Formik>
          </center>
        </div>
      </div>
    </div>
  );
}
