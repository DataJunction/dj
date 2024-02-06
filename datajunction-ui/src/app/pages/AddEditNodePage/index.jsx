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
import { useParams, useNavigate } from 'react-router-dom';
import { FullNameField } from './FullNameField';
import { MetricQueryField } from './MetricQueryField';
import { displayMessageAfterSubmit } from '../../../utils/form';
import { PrimaryKeySelect } from './PrimaryKeySelect';
import { NodeQueryField } from './NodeQueryField';
import { MetricMetadataFields } from './MetricMetadataFields';
import { UpstreamNodeField } from './UpstreamNodeField';
import { TagsField } from './TagsField';
import { NamespaceField } from './NamespaceField';
import { AlertMessage } from './AlertMessage';
import { DisplayNameField } from './DisplayNameField';
import { DescriptionField } from './DescriptionField';
import { NodeModeField } from './NodeModeField';
import { RequiredDimensionsSelect } from './RequiredDimensionsSelect';

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

  const initialValues = {
    name: action === Action.Edit ? name : '',
    namespace: action === Action.Add ? initialNamespace : '',
    display_name: '',
    query: '',
    type: nodeType,
    description: '',
    primary_key: '',
    mode: 'draft',
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
    return primaryKey.map(columnName => columnName.trim());
  };

  const createNode = async (values, setStatus) => {
    const { status, json } = await djClient.createNode(
      nodeType,
      values.name,
      values.display_name,
      values.description,
      values.type === 'metric'
        ? `SELECT ${values.aggregate_expression} FROM ${values.upstream_node}`
        : values.query,
      values.mode,
      values.namespace,
      values.primary_key ? primaryKeyToList(values.primary_key) : null,
      values.metric_direction,
      values.metric_unit,
      values.required_dimensions,
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
        ? `SELECT ${values.aggregate_expression} FROM ${values.upstream_node}`
        : values.query,
      values.mode,
      values.primary_key ? primaryKeyToList(values.primary_key) : null,
      values.metric_direction,
      values.metric_unit,
      values.required_dimensions,
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
    const data = await djClient.node(name);
    if (data.type === 'metric') {
      const metric = await djClient.metric(name);
      data.upstream_node = metric.upstream_node;
      data.expression = metric.expression;
      data.required_dimensions = metric.required_dimensions;
    }
    return data;
  };

  const primaryKeyFromNode = node => {
    return node.columns
      .filter(
        col =>
          col.attributes &&
          col.attributes.filter(
            attr => attr.attribute_type.name === 'primary_key',
          ).length > 0,
      )
      .map(col => col.name);
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
      'expression',
      'upstream_node',
    ];
    const primaryKey = primaryKeyFromNode(data);
    fields.forEach(field => {
      if (field === 'primary_key') {
        setFieldValue(field, primaryKey);
      } else if (field === 'tags') {
        setFieldValue(
          field,
          data[field].map(tag => tag.name),
        );
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
    if (data.expression) {
      setFieldValue('aggregate_expression', data.expression);
    }
    if (data.upstream_node) {
      setFieldValue('upstream_node', data.upstream_node);
    }
    setNode(data);

    // For react-select fields, we have to explicitly set the entire
    // field rather than just the values
    setSelectTags(
      <TagsField
        defaultValue={data.tags.map(t => {
          return { value: t.name, label: t.display_name };
        })}
      />,
    );
    setSelectPrimaryKey(
      <PrimaryKeySelect
        defaultValue={primaryKey.map(col => {
          return { value: col, label: col };
        })}
      />,
    );
    setSelectRequiredDims(
      <RequiredDimensionsSelect
        defaultValue={data.required_dimensions.map(dim => {
          return { value: dim, label: dim };
        })}
      />,
    );
    setSelectUpstreamNode(
      <UpstreamNodeField
        defaultValue={{
          value: data.upstream_node,
          label: data.upstream_node,
        }}
      />,
    );
  };

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
                const [selectPrimaryKey, setSelectPrimaryKey] = useState(null);
                const [selectRequiredDims, setSelectRequiredDims] =
                  useState(null);
                const [selectUpstreamNode, setSelectUpstreamNode] =
                  useState(null);
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
                        {nodeType === 'metric' || node.type === 'metric' ? (
                          <MetricQueryField
                            djClient={djClient}
                            value={node.expression ? node.expression : ''}
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
                            <PrimaryKeySelect />
                          )
                        ) : action === Action.Edit ? (
                          selectRequiredDims
                        ) : (
                          <RequiredDimensionsSelect />
                        )}
                        {action === Action.Edit ? selectTags : <TagsField />}
                        <NodeModeField />

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
