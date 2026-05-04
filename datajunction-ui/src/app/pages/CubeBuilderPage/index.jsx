import React, { useContext, useEffect, useState } from 'react';
import Select from 'react-select';
import NamespaceHeader from '../../components/NamespaceHeader';
import { DataJunctionAPI } from '../../services/DJService';
import DJClientContext from '../../providers/djclient';
import { useCurrentUser } from '../../providers/UserProvider';
import 'react-querybuilder/dist/query-builder.scss';
import 'styles/styles.scss';
import './styles.css';
import '../QueryPlannerPage/styles.css';
import { ErrorMessage, Field, Form, Formik, useField } from 'formik';
import { displayMessageAfterSubmit } from '../../../utils/form';
import { useParams, useNavigate } from 'react-router-dom';
import { Action } from '../../components/forms/Action';
import NodeNameField from '../../components/forms/NodeNameField';
import { MetricsSelect } from './MetricsSelect';
import { DimensionsSelect } from './DimensionsSelect';
import { CubePreviewPanel } from './CubePreviewPanel';

// Description textarea that directly binds to Formik so typing is reflected
const DescriptionField = () => {
  const [field] = useField('description');
  return (
    <textarea
      id="Description"
      name="description"
      placeholder="Describe your cube"
      value={field.value ?? ''}
      onChange={field.onChange}
      onBlur={field.onBlur}
    />
  );
};

// Simple Tags select matching MetricsSelect styling
const CubeTagsSelect = ({ defaultValue }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [field, , helpers] = useField('tags');
  const [options, setOptions] = useState([]);
  const [selected, setSelected] = useState(defaultValue || []);

  useEffect(() => {
    djClient.listTags().then(tags => {
      setOptions(tags.map(t => ({ value: t.name, label: t.display_name })));
    });
  }, [djClient]);

  useEffect(() => {
    if (defaultValue) setSelected(defaultValue);
  }, [defaultValue]);

  const handleChange = sel => {
    setSelected(sel || []);
    helpers.setValue((sel || []).map(s => s.value));
  };

  return (
    <Select
      value={selected}
      options={options}
      onChange={handleChange}
      isMulti
      placeholder="Select tags..."
    />
  );
};

// Simple Owners select matching MetricsSelect styling
const CubeOwnersSelect = ({ defaultValue, isEdit = false }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { currentUser } = useCurrentUser();
  const [field, , helpers] = useField('owners');
  const [options, setOptions] = useState([]);
  const [selected, setSelected] = useState(defaultValue || []);

  useEffect(() => {
    djClient.users().then(users => {
      setOptions(users.map(u => ({ value: u.username, label: u.username })));
    });
  }, [djClient]);

  // Only auto-default to current user in Add mode; in Edit mode wait for defaultValue
  useEffect(() => {
    if (defaultValue) {
      setSelected(defaultValue);
      helpers.setValue(defaultValue.map(d => d.value));
    } else if (!isEdit && currentUser) {
      const def = [
        { value: currentUser.username, label: currentUser.username },
      ];
      setSelected(def);
      helpers.setValue([currentUser.username]);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [defaultValue, currentUser, isEdit]);

  const handleChange = sel => {
    setSelected(sel || []);
    helpers.setValue((sel || []).map(s => s.value));
  };

  return (
    <Select
      value={selected}
      options={options}
      onChange={handleChange}
      isMulti
      placeholder="Select owners..."
    />
  );
};

export function CubeBuilderPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const navigate = useNavigate();

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
    owners: [],
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
        success: true,
        savedName: json.name,
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
      values.owners,
    );
    const tagsResponse = await djClient.tagsNode(
      values.name,
      (values.tags || []).map(tag => tag),
    );
    if ((status === 200 || status === 201) && tagsResponse.status === 200) {
      setStatus({
        success: true,
        savedName: json.name,
      });
    } else {
      setStatus({
        failure: `${json.message}`,
      });
    }
  };

  const updateFieldsWithNodeData = (
    data,
    setFieldValue,
    setSelectTags,
    setSelectOwners,
  ) => {
    setFieldValue('display_name', data.current.displayName || '', false);
    setFieldValue('description', data.current.description || '', false);
    setFieldValue('mode', data.current.mode.toLowerCase() || 'draft', false);
    setFieldValue(
      'tags',
      data.tags.map(tag => tag.name),
    );
    // Store default values as arrays for the Select components
    setSelectTags(
      data.tags.map(t => ({ value: t.name, label: t.displayName })),
    );
    if (data.owners) {
      setSelectOwners(
        data.owners.map(owner => ({
          value: owner.username,
          label: owner.username,
        })),
      );
    }
  };

  // @ts-ignore
  return (
    <>
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
        <Formik
          initialValues={initialValues}
          validate={validator}
          onSubmit={handleSubmit}
        >
          {function Render({ isSubmitting, status, setFieldValue, props }) {
            const [node, setNode] = useState([]);
            const [selectTags, setSelectTags] = useState(null);
            const [selectOwners, setSelectOwners] = useState(null);
            const [justSaved, setJustSaved] = useState(false);
            const [saveError, setSaveError] = useState(null);

            // Get cube
            useEffect(() => {
              const fetchData = async () => {
                if (name) {
                  const cube = await djClient.getCubeForEditing(name);
                  setNode(cube);
                  updateFieldsWithNodeData(
                    cube,
                    setFieldValue,
                    setSelectTags,
                    setSelectOwners,
                  );
                }
              };
              fetchData().catch(console.error);
            }, [setFieldValue]);

            // Briefly show "Saved" state on the button, then redirect to the cube page
            useEffect(() => {
              if (status?.success) {
                setJustSaved(true);
                setSaveError(null);
                const savedName = status.savedName;
                const timer = setTimeout(() => {
                  if (savedName) {
                    navigate(`/nodes/${savedName}`);
                  }
                }, 1200);
                return () => clearTimeout(timer);
              }
              if (status?.failure) {
                setSaveError(status.failure);
                setJustSaved(false);
              }
            }, [status, navigate]);

            return (
              <Form className="cube-builder">
                {/* Header */}
                <div className="cube-builder-header">
                  <h2>
                    {action === Action.Edit ? 'Edit' : 'Create'}{' '}
                    <span className="node_type__cube node_type_creation_heading">
                      Cube
                    </span>
                  </h2>
                </div>

                {/* Two-column layout */}
                <div className="cube-builder-layout">
                  {/* Left: Main form */}
                  <div className="cube-builder-main">
                    {/* Details Section */}
                    <div className="cube-form-section">
                      <div className="cube-form-section-body">
                        {action === Action.Add ? (
                          <>
                            {/* Add mode uses NodeNameField for namespace/name */}
                            <NodeNameField />

                            {/* Row: Description | Mode */}
                            <div className="cube-field-row">
                              <div className="cube-field cube-field-grow">
                                <ErrorMessage
                                  name="description"
                                  component="span"
                                />
                                <label
                                  className="cube-field-label"
                                  htmlFor="Description"
                                >
                                  Description
                                </label>
                                <DescriptionField />
                              </div>
                              <div className="cube-field cube-field-small">
                                <label
                                  className="cube-field-label"
                                  htmlFor="Mode"
                                >
                                  Mode
                                </label>
                                <Field as="select" name="mode" id="Mode">
                                  <option value="draft">Draft</option>
                                  <option value="published">Published</option>
                                </Field>
                              </div>
                            </div>

                            {/* Row: Tags | Owners */}
                            <div className="cube-field-row">
                              <div className="cube-field cube-field-half">
                                <label className="cube-field-label">Tags</label>
                                <CubeTagsSelect />
                              </div>
                              <div className="cube-field cube-field-half">
                                <label className="cube-field-label">
                                  Owners
                                </label>
                                <CubeOwnersSelect />
                              </div>
                            </div>
                          </>
                        ) : (
                          <>
                            {/* Row 1: Name (full width) */}
                            <div className="cube-field">
                              <label className="cube-field-label">Name</label>
                              <div className="cube-field-static">{name}</div>
                            </div>

                            {/* Row 2: Display Name | Mode */}
                            <div className="cube-field-row">
                              <div className="cube-field cube-field-grow">
                                <ErrorMessage
                                  name="display_name"
                                  component="span"
                                />
                                <label
                                  className="cube-field-label"
                                  htmlFor="displayName"
                                >
                                  Display Name
                                </label>
                                <Field
                                  type="text"
                                  name="display_name"
                                  id="displayName"
                                  placeholder="Human readable name"
                                />
                              </div>
                              <div className="cube-field cube-field-small">
                                <label
                                  className="cube-field-label"
                                  htmlFor="Mode"
                                >
                                  Mode
                                </label>
                                <Field as="select" name="mode" id="Mode">
                                  <option value="draft">Draft</option>
                                  <option value="published">Published</option>
                                </Field>
                              </div>
                            </div>

                            {/* Row 3: Description (full width) */}
                            <div className="cube-field">
                              <ErrorMessage
                                name="description"
                                component="span"
                              />
                              <label
                                className="cube-field-label"
                                htmlFor="Description"
                              >
                                Description
                              </label>
                              <DescriptionField />
                            </div>

                            {/* Row 4: Tags | Owners */}
                            <div className="cube-field-row">
                              <div className="cube-field cube-field-half">
                                <label className="cube-field-label">Tags</label>
                                <CubeTagsSelect defaultValue={selectTags} />
                              </div>
                              <div className="cube-field cube-field-half">
                                <label className="cube-field-label">
                                  Owners
                                </label>
                                <CubeOwnersSelect
                                  defaultValue={selectOwners}
                                  isEdit={true}
                                />
                              </div>
                            </div>
                          </>
                        )}
                      </div>
                    </div>

                    {/* Metrics Section */}
                    <div className="cube-form-section">
                      <div className="cube-form-section-header">
                        <h3>Metrics</h3>
                      </div>
                      <div className="cube-form-section-body">
                        <div data-testid="select-metrics">
                          {action === Action.Edit ? (
                            <MetricsSelect cube={node} />
                          ) : (
                            <MetricsSelect />
                          )}
                        </div>
                      </div>
                    </div>

                    {/* Dimensions Section */}
                    <div className="cube-form-section">
                      <div className="cube-form-section-header">
                        <h3>Dimensions</h3>
                      </div>
                      <div className="cube-form-section-body">
                        <div data-testid="select-dimensions">
                          {action === Action.Edit ? (
                            <DimensionsSelect cube={node} />
                          ) : (
                            <DimensionsSelect />
                          )}
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Right: Sidebar */}
                  <div className="cube-builder-sidebar">
                    <CubePreviewPanel />

                    <div className="cube-settings">
                      {saveError && (
                        <div className="save-error-message">{saveError}</div>
                      )}
                      <button
                        type="submit"
                        disabled={isSubmitting || justSaved}
                        aria-label="CreateCube"
                        className={`save-cube-btn${
                          justSaved ? ' save-cube-btn--saved' : ''
                        }${isSubmitting ? ' save-cube-btn--loading' : ''}`}
                      >
                        {isSubmitting ? (
                          <>
                            <span className="save-spinner" aria-hidden="true" />
                            Saving...
                          </>
                        ) : justSaved ? (
                          '✓ Saved'
                        ) : action === Action.Add ? (
                          'Create Cube'
                        ) : (
                          'Save'
                        )}
                      </button>
                    </div>
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
