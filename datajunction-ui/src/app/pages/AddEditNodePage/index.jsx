/**
 * Node add + edit page for transforms, metrics, and dimensions. The creation and edit flow for these
 * node types is largely the same, with minor differences handled server-side. For the `query`
 * field, this page will render a CodeMirror SQL editor with autocompletion and syntax highlighting.
 */
import { ErrorMessage, Form, Formik } from 'formik';
import NamespaceHeader from '../../components/NamespaceHeader';
import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import 'styles/node-creation.scss';
import { useParams, useNavigate } from 'react-router-dom';
import { FullNameField } from './FullNameField';
import { MetricQueryField } from './MetricQueryField';
import { displayMessageAfterSubmit } from '../../../utils/form';
import { NodeQueryField } from './NodeQueryField';
import { MetricMetadataFields } from './MetricMetadataFields';
import { UpstreamNodeField } from './UpstreamNodeField';
import { OwnersField } from './OwnersField';
import { TagsField } from './TagsField';
import { NamespaceField } from './NamespaceField';
import { AlertMessage } from './AlertMessage';
import { DisplayNameField } from './DisplayNameField';
import { DescriptionField } from './DescriptionField';
import { NodeModeField } from './NodeModeField';
import { RequiredDimensionsSelect } from './RequiredDimensionsSelect';
import LoadingIcon from '../../icons/LoadingIcon';
import { ColumnsSelect } from './ColumnsSelect';
import { CustomMetadataField } from './CustomMetadataField';

class Action {
  static Add = new Action('add');
  static Edit = new Action('edit');

  constructor(name) {
    this.name = name;
  }
}

export function AddEditNodePage({ extensions = {} }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const navigate = useNavigate();

  let { nodeType, initialNamespace, name } = useParams();
  const action = name !== undefined ? Action.Edit : Action.Add;

  const initialValues = {
    name: action === Action.Edit ? name : '',
    namespace: action === Action.Add ? initialNamespace : '',
    display_name: '',
    query: '',
    type: nodeType,
    description: '',
    primary_key: '',
    mode: 'published',
    owners: [],
    custom_metadata: '',
  };

  const validator = values => {
    const errors = {};
    if (!values.name) {
      errors.name = 'Required';
    }
    if (values.type !== 'metric' && !values.query) {
      errors.query = 'Required';
    }
    return errors;
  };

  const handleSubmit = async (values, { setSubmitting, setStatus }) => {
    if (action === Action.Add) {
      await createNode(values, setStatus).then(_ => {
        window.scrollTo({ top: 0, left: 0, behavior: 'smooth' });
        setSubmitting(false);
      });
    } else {
      await patchNode(values, setStatus).then(_ => {
        window.scrollTo({ top: 0, left: 0, behavior: 'smooth' });
        setSubmitting(false);
      });
    }
  };
  const submitHandlers = [handleSubmit];

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
        <label>Name</label> {name}
      </div>
      <div className="NodeNameInput NodeCreationInput">
        <label>Type</label> {node.type}
      </div>
    </>
  );

  const primaryKeyToList = primaryKey => {
    return primaryKey.map(columnName => columnName.trim());
  };

  const parseCustomMetadata = customMetadata => {
    if (!customMetadata || customMetadata.trim() === '') {
      return null;
    }
    try {
      return JSON.parse(customMetadata);
    } catch (err) {
      return null;
    }
  };

  /**
   * Build the metric query based on whether an upstream node is provided.
   * - With upstream node: `SELECT <expression> FROM <upstream_node>` (regular metric)
   * - Without upstream node: `SELECT <expression>` (derived metric referencing other metrics)
   */
  const buildMetricQuery = (aggregateExpression, upstreamNode) => {
    if (upstreamNode && upstreamNode.trim() !== '') {
      return `SELECT ${aggregateExpression} \n FROM ${upstreamNode}`;
    }
    // Derived metric - no FROM clause needed, expression references other metrics directly
    return `SELECT ${aggregateExpression}`;
  };

  const createNode = async (values, setStatus) => {
    const { status, json } = await djClient.createNode(
      nodeType,
      values.name,
      values.display_name,
      values.description,
      values.type === 'metric'
        ? buildMetricQuery(values.aggregate_expression, values.upstream_node)
        : values.query,
      values.mode,
      values.namespace,
      values.primary_key ? primaryKeyToList(values.primary_key) : null,
      values.metric_direction,
      values.metric_unit,
      values.required_dimensions,
      parseCustomMetadata(values.custom_metadata),
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
      values.type === 'metric'
        ? buildMetricQuery(values.aggregate_expression, values.upstream_node)
        : values.query,
      values.mode,
      values.primary_key ? primaryKeyToList(values.primary_key) : null,
      values.metric_direction,
      values.metric_unit,
      values.significant_digits,
      values.required_dimensions,
      values.owners,
      parseCustomMetadata(values.custom_metadata),
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
        failure: `${json.message}`,
      });
    }
  };

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

  const getExistingNodeData = async name => {
    const node = await djClient.getNodeForEditing(name);
    if (node === null) {
      return { message: `Node ${name} does not exist` };
    }
    const baseData = {
      name: node.name,
      type: node.type.toLowerCase(),
      display_name: node.current.displayName,
      description: node.current.description,
      primary_key: node.current.primaryKey,
      query: node.current.query,
      tags: node.tags,
      mode: node.current.mode.toLowerCase(),
      owners: node.owners,
      custom_metadata: node.current.customMetadata,
    };

    if (node.type === 'METRIC') {
      // Check if this is a derived metric (parent is another metric)
      const firstParent = node.current.parents[0];
      const isDerivedMetric = firstParent?.type === 'METRIC';

      if (isDerivedMetric) {
        // Derived metric: no upstream node, expression is the full query projection
        // Parse the expression from the query (format: "SELECT <expression>")
        const query = node.current.query || '';
        const selectMatch = query.match(/SELECT\s+(.+)/is);
        const derivedExpression = selectMatch
          ? selectMatch[1].trim()
          : node.current.metricMetadata?.expression || '';

        return {
          ...baseData,
          metric_direction:
            node.current.metricMetadata?.direction?.toLowerCase(),
          metric_unit: node.current.metricMetadata?.unit?.name?.toLowerCase(),
          significant_digits: node.current.metricMetadata?.significantDigits,
          required_dimensions: node.current.requiredDimensions.map(
            dim => dim.name,
          ),
          upstream_node: '', // Derived metrics have no upstream node
          aggregate_expression: derivedExpression,
        };
      } else {
        // Regular metric: has upstream node
        return {
          ...baseData,
          metric_direction:
            node.current.metricMetadata?.direction?.toLowerCase(),
          metric_unit: node.current.metricMetadata?.unit?.name?.toLowerCase(),
          significant_digits: node.current.metricMetadata?.significantDigits,
          required_dimensions: node.current.requiredDimensions.map(
            dim => dim.name,
          ),
          upstream_node: firstParent?.name || '',
          aggregate_expression: node.current.metricMetadata?.expression,
        };
      }
    }
    return baseData;
  };

  const runValidityChecks = (data, setNode, setMessage) => {
    // Check if node exists
    if (data.message !== undefined) {
      setNode(null);
      setMessage(`Node ${name} does not exist!`);
      return;
    }
    // Check if node type can be edited
    if (!nodeCanBeEdited(data.type)) {
      setNode(null);
      if (data.type === 'cube') {
        navigate(`/nodes/${data.name}/edit-cube`);
      }
      setMessage(`Node ${name} is of type ${data.type} and cannot be edited`);
    }
  };

  const updateFieldsWithNodeData = (
    data,
    setFieldValue,
    setNode,
    setSelectTags,
    setSelectPrimaryKey,
    setSelectUpstreamNode,
    setSelectRequiredDims,
    setSelectOwners,
  ) => {
    // Update fields with existing data to prepare for edit
    const fields = [
      'display_name',
      'query',
      'type',
      'description',
      'primary_key',
      'mode',
      'tags',
      'aggregate_expression',
      'upstream_node',
      'metric_unit',
      'metric_direction',
      'significant_digits',
      'owners',
      'custom_metadata',
    ];
    fields.forEach(field => {
      if (field === 'tags') {
        setFieldValue(
          field,
          data[field].map(tag => tag.name),
        );
      } else if (field === 'owners') {
        setFieldValue(
          field,
          data[field].map(owner => owner.username),
        );
      } else if (field === 'custom_metadata') {
        const value = data[field] ? JSON.stringify(data[field], null, 2) : '';
        setFieldValue(field, value, false);
      } else {
        setFieldValue(field, data[field] || '', false);
      }
    });
    setNode(data);

    // For react-select fields, we have to explicitly set the entire
    // field rather than just the values
    setSelectTags(
      <TagsField
        defaultValue={data.tags.map(t => {
          return { value: t.name, label: t.displayName };
        })}
      />,
    );
    setSelectPrimaryKey(
      <ColumnsSelect
        defaultValue={data.primary_key}
        fieldName="primary_key"
        label="Primary Key"
        isMulti={true}
      />,
    );
    if (data.required_dimensions) {
      setSelectRequiredDims(
        <RequiredDimensionsSelect
          defaultValue={data.required_dimensions.map(dim => {
            return { value: dim, label: dim };
          })}
        />,
      );
    }
    // For derived metrics, upstream_node is empty - pass null to clear the select
    setSelectUpstreamNode(
      <UpstreamNodeField
        defaultValue={
          data.upstream_node
            ? { value: data.upstream_node, label: data.upstream_node }
            : null
        }
      />,
    );
    if (data.owners) {
      setSelectOwners(
        <OwnersField
          defaultValue={data.owners.map(owner => {
            return { value: owner.username, label: owner.username };
          })}
        />,
      );
    }
  };

  return (
    <div className="mid">
      <NamespaceHeader
        namespace={
          initialNamespace
            ? initialNamespace
            : name
            ? name.substring(0, name.lastIndexOf('.'))
            : ''
        }
      />
      <div className="card">
        <div className="card-header">
          {pageTitle}
          <center>
            <Formik
              initialValues={initialValues}
              validate={validator}
              validateOnChange={true}
              validateOnBlur={true}
              onSubmit={async (values, { setSubmitting, setStatus }) => {
                try {
                  for (const handler of submitHandlers) {
                    await handler(values, { setSubmitting, setStatus });
                  }
                } catch (error) {
                  console.error('Error in submission', error);
                } finally {
                  setSubmitting(false);
                }
              }}
            >
              {function Render(formikProps) {
                const {
                  isSubmitting,
                  status,
                  setFieldValue,
                  errors,
                  touched,
                  isValid,
                  dirty,
                } = formikProps;
                const [node, setNode] = useState([]);
                const [selectPrimaryKey, setSelectPrimaryKey] = useState(null);
                const [selectRequiredDims, setSelectRequiredDims] =
                  useState(null);
                const [selectUpstreamNode, setSelectUpstreamNode] =
                  useState(null);
                const [selectOwners, setSelectOwners] = useState(null);
                const [selectTags, setSelectTags] = useState(null);
                const [message, setMessage] = useState('');

                useEffect(() => {
                  const fetchData = async () => {
                    if (action === Action.Edit) {
                      const data = await getExistingNodeData(name);
                      runValidityChecks(data, setNode, setMessage);
                      updateFieldsWithNodeData(
                        data,
                        setFieldValue,
                        setNode,
                        setSelectTags,
                        setSelectPrimaryKey,
                        setSelectUpstreamNode,
                        setSelectRequiredDims,
                        setSelectOwners,
                      );
                    }
                  };
                  fetchData().catch(console.error);
                }, [setFieldValue]);
                return (
                  <Form>
                    {displayMessageAfterSubmit(status)}
                    {action === Action.Edit && message ? (
                      <AlertMessage message={message} />
                    ) : (
                      <>
                        {action === Action.Add ? (
                          <NamespaceField initialNamespace={initialNamespace} />
                        ) : (
                          staticFieldsInEdit(node)
                        )}
                        <DisplayNameField />
                        {action === Action.Add ? fullNameInput : ''}
                        <DescriptionField />
                        <br />
                        {nodeType === 'metric' || node?.type === 'metric' ? (
                          action === Action.Edit ? (
                            selectUpstreamNode
                          ) : (
                            <UpstreamNodeField />
                          )
                        ) : (
                          ''
                        )}
                        <br />
                        <br />
                        {action === Action.Edit ? (
                          selectOwners
                        ) : (
                          <OwnersField />
                        )}
                        <br />
                        <br />
                        {nodeType === 'metric' || node.type === 'metric' ? (
                          <MetricQueryField
                            djClient={djClient}
                            value={
                              node.aggregate_expression
                                ? node.aggregate_expression
                                : ''
                            }
                          />
                        ) : (
                          <NodeQueryField
                            djClient={djClient}
                            value={node.query ? node.query : ''}
                          />
                        )}
                        <br />
                        {nodeType === 'metric' || node.type === 'metric' ? (
                          <MetricMetadataFields />
                        ) : (
                          ''
                        )}
                        {nodeType !== 'metric' && node.type !== 'metric' ? (
                          action === Action.Edit ? (
                            selectPrimaryKey
                          ) : (
                            <ColumnsSelect
                              defaultValue={[]}
                              fieldName="primary_key"
                              label="Primary Key"
                              isMulti={true}
                            />
                          )
                        ) : action === Action.Edit ? (
                          selectRequiredDims
                        ) : (
                          <RequiredDimensionsSelect />
                        )}
                        <CustomMetadataField
                          value={
                            node.custom_metadata ? node.custom_metadata : ''
                          }
                        />
                        {Object.entries(extensions).map(
                          ([key, ExtensionComponent]) => (
                            <div key={key} className="mt-4 border-t pt-4">
                              <ExtensionComponent
                                node={node}
                                action={action}
                                registerSubmitHandler={(
                                  onSubmit,
                                  { prepend } = {},
                                ) => {
                                  if (!submitHandlers.includes(onSubmit)) {
                                    if (prepend) {
                                      submitHandlers.unshift(onSubmit);
                                    } else {
                                      submitHandlers.push(onSubmit);
                                    }
                                  }
                                }}
                              />
                            </div>
                          ),
                        )}
                        {action === Action.Edit ? selectTags : <TagsField />}
                        <NodeModeField />

                        <button
                          type="submit"
                          disabled={isSubmitting || !isValid}
                        >
                          {isSubmitting ? (
                            <LoadingIcon />
                          ) : (
                            (action === Action.Add ? 'Create ' : 'Save ') +
                            (nodeType ? nodeType : '')
                          )}
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
