import React, { useContext, useEffect, useState } from 'react';
import NamespaceHeader from '../../components/NamespaceHeader';
import { DataJunctionAPI } from '../../services/DJService';
import DJClientContext from '../../providers/djclient';
import 'react-querybuilder/dist/query-builder.scss';
import 'styles/styles.scss';
import { ErrorMessage, Field, Form, Formik } from 'formik';
import { displayMessageAfterSubmit } from '../../../utils/form';
import { useParams } from 'react-router-dom';
import { Action } from '../../components/forms/Action';
import NodeNameField from '../../components/forms/NodeNameField';
import { MetricsSelect } from './MetricsSelect';
import { DimensionsSelect } from './DimensionsSelect';
import { TagsField } from '../AddEditNodePage/TagsField';

export function CubeBuilderPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  let { nodeType, initialNamespace, name } = useParams();
  const action = name !== undefined ? Action.Edit : Action.Add;
  const validator = ruleType => !!ruleType.value;

  const initialValues = {
    name: action === Action.Edit ? name : '',
    namespace: action === Action.Add ? initialNamespace : '',
    display_name: '',
    description: '',
    mode: 'published',
    metrics: [],
    dimensions: [],
    filters: [],
    tags: [],
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

  const createNode = async (values, setStatus) => {
    const { status, json } = await djClient.createCube(
      values.name,
      values.display_name,
      values.description,
      values.mode,
      values.metrics,
      values.dimensions,
      values.filters || [],
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
    const { status, json } = await djClient.patchCube(
      values.name,
      values.display_name,
      values.description,
      values.mode,
      values.metrics,
      values.dimensions,
      values.filters || [],
    );
    const tagsResponse = await djClient.tagsNode(
      values.name,
      (values.tags || []).map(tag => tag),
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

  const updateFieldsWithNodeData = (data, setFieldValue, setSelectTags) => {
    setFieldValue('display_name', data.display_name || '', false);
    setFieldValue('description', data.description || '', false);
    setFieldValue('mode', data.mode || 'draft', false);
    setFieldValue(
      'tags',
      data.tags.map(tag => tag.name),
    );
    // For react-select fields, we have to explicitly set the entire
    // field rather than just the values
    setSelectTags(
      <TagsField
        defaultValue={data.tags.map(t => {
          return { value: t.name, label: t.display_name };
        })}
      />,
    );
  };

  const staticFieldsInEdit = () => (
    <>
      <div className="NodeNameInput NodeCreationInput">
        <label htmlFor="name">Name</label> {name}
      </div>
      <div className="NodeNameInput NodeCreationInput">
        <label htmlFor="name">Type</label> cube
      </div>
      <div className="DisplayNameInput NodeCreationInput">
        <ErrorMessage name="display_name" component="span" />
        <label htmlFor="displayName">Display Name</label>
        <Field
          type="text"
          name="display_name"
          id="displayName"
          placeholder="Human readable display name"
        />
      </div>
    </>
  );

  // @ts-ignore
  return (
    <>
      <div className="mid">
        <NamespaceHeader namespace="" />
        <Formik
          initialValues={initialValues}
          validate={validator}
          onSubmit={handleSubmit}
        >
          {function Render({ isSubmitting, status, setFieldValue, props }) {
            const [node, setNode] = useState([]);
            const [selectTags, setSelectTags] = useState(null);

            // Get cube
            useEffect(() => {
              const fetchData = async () => {
                if (name) {
                  const cube = await djClient.cube(name);
                  setNode(cube);
                  updateFieldsWithNodeData(cube, setFieldValue, setSelectTags);
                }
              };
              fetchData().catch(console.error);
            }, [setFieldValue]);

            return (
              <Form>
                <div className="card">
                  <div className="card-header">
                    <h2>
                      {action === Action.Edit ? 'Edit' : 'Create'}{' '}
                      <span
                        className={`node_type__cube node_type_creation_heading`}
                      >
                        Cube
                      </span>
                    </h2>
                    {displayMessageAfterSubmit(status)}
                    {action === Action.Add ? (
                      <NodeNameField />
                    ) : (
                      staticFieldsInEdit(node)
                    )}
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
                    <div className="CubeCreationInput">
                      <label htmlFor="react-select-3-input">Metrics *</label>
                      <p>Select metrics to include in the cube.</p>
                      <span
                        data-testid="select-metrics"
                        style={{ marginTop: '15px' }}
                      >
                        {action === Action.Edit ? (
                          <MetricsSelect cube={node} />
                        ) : (
                          <MetricsSelect />
                        )}
                      </span>
                    </div>
                    <br />
                    <br />
                    <div className="CubeCreationInput">
                      <label htmlFor="react-select-3-input">Dimensions *</label>
                      <p>
                        Select dimensions to include in the cube. As metrics are
                        selected above, the list of available dimensions will be
                        filtered to those shared by the selected metrics. If the
                        dimensions list is empty, no shared dimensions were
                        discovered.
                      </p>
                      <span data-testid="select-dimensions">
                        {action === Action.Edit ? (
                          <DimensionsSelect cube={node} />
                        ) : (
                          <DimensionsSelect />
                        )}
                      </span>
                    </div>
                    <div className="NodeModeInput NodeCreationInput">
                      <ErrorMessage name="mode" component="span" />
                      <label htmlFor="Mode">Mode</label>
                      <Field as="select" name="mode" id="Mode">
                        <option value="draft">Draft</option>
                        <option value="published">Published</option>
                      </Field>
                    </div>
                    {action === Action.Edit ? selectTags : <TagsField />}
                    <button
                      type="submit"
                      disabled={isSubmitting}
                      aria-label="CreateCube"
                    >
                      {action === Action.Add ? 'Create Cube' : 'Save'}{' '}
                      {nodeType}
                    </button>
                  </div>
                </div>
              </Form>
            );
          }}
        </Formik>
      </div>
    </>
  );
}

CubeBuilderPage.defaultProps = {
  djClient: DataJunctionAPI,
};
