import React, { useContext, useEffect, useState } from 'react';
import NamespaceHeader from '../../components/NamespaceHeader';
import { DataJunctionAPI } from '../../services/DJService';
import DJClientContext from '../../providers/djclient';
import Select from 'react-select';
import 'react-querybuilder/dist/query-builder.scss';
import QueryBuilder from 'react-querybuilder';
import 'styles/styles.scss';
import { ErrorMessage, Field, Form, Formik } from 'formik';
import { displayMessageAfterSubmit, labelize } from '../../../utils/form';
import { FullNameField } from '../AddEditNodePage/FullNameField';
import { FormikSelect } from '../AddEditNodePage/FormikSelect';
import { useParams } from 'react-router-dom';

class Action {
  static Add = new Action('add');
  static Edit = new Action('edit');

  constructor(name) {
    this.name = name;
  }
}

export function CubeBuilderPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  let { nodeType, initialNamespace, name } = useParams();
  const action = name !== undefined ? Action.Edit : Action.Add;
  const validator = ruleType => !!ruleType.value;
  const [stagedMetrics, setStagedMetrics] = useState([]);
  const [metrics, setMetrics] = useState([]);
  const [commonDimensionsList, setCommonDimensionsList] = useState([]);
  const [selectedDimensions, setSelectedDimensions] = useState([]);

  const [namespaces, setNamespaces] = useState([]);
  const [stagedDimensions, setStagedDimensions] = useState([]);
  const [selectedMetrics, setSelectedMetrics] = useState([]);
  const [fields, setFields] = useState([]);
  const [filters, setFilters] = useState({ combinator: 'and', rules: [] });

  // Get namespaces, only necessary when creating a node
  useEffect(() => {
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
  }, [djClient, djClient.metrics]);

  // Get metrics
  useEffect(() => {
    const fetchData = async () => {
      const metrics = await djClient.metrics();
      setMetrics(metrics.map(m => ({ value: m, label: m })));
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.metrics]);

  const initialValues = {
    name: action === Action.Edit ? name : '',
    namespace: action === Action.Add ? initialNamespace : '',
    display_name: '',
    description: '',
    mode: 'draft',
    metrics: [],
    dimensions: [],
    filters: [],
  };

  const handleSubmit = (values, { setSubmitting, setStatus }) => {
    if (action === Action.Add) {
      values.metrics = selectedMetrics;
      values.dimensions = selectedDimensions;
      setTimeout(() => {
        console.log('calling createNode with', values);
        createNode(values, setStatus);
        setSubmitting(false);
      }, 400);
    }
    // else {
    //   setTimeout(() => {
    //     patchNode(values, setStatus);
    //     setSubmitting(false);
    //   }, 400);
    // }
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
      values.filters,
    );
    if (status === 200 || status === 201) {
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

  const attributeToFormInput = dimension => {
    const attribute = {
      name: dimension.name,
      label: `${dimension.name} (via ${dimension.path.join(' ▶ ')})`,
      placeholder: `from ${dimension.path}`,
      defaultOperator: '=',
      validator,
    };
    if (dimension.type === 'bool') {
      attribute.valueEditorType = 'checkbox';
    }
    if (dimension.type === 'timestamp') {
      attribute.inputType = 'datetime-local';
      attribute.defaultOperator = 'between';
    }
    return [dimension.name, attribute];
  };

  // Get common dimensions
  useEffect(() => {
    const fetchData = async () => {
      if (selectedMetrics.length) {
        const commonDimensions = await djClient.commonDimensions(
          selectedMetrics,
        );
        const grouped = Object.entries(
          Object.groupBy(
            commonDimensions,
            ({ path, node_display_name }) => node_display_name + ' via ' + path,
          ),
        );
        setCommonDimensionsList(grouped);
        const uniqueFields = Object.fromEntries(
          new Map(
            commonDimensions.map(dimension => attributeToFormInput(dimension)),
          ),
        );
        setFields(Object.keys(uniqueFields).map(f => uniqueFields[f]));
      } else {
        setCommonDimensionsList([]);
        setFields([]);
      }
    };
    fetchData().catch(console.error);
  }, [selectedMetrics, djClient]);

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
            return (
              <Form>
                <div className="card">
                  <div className="card-header">
                    <h2>
                      Create{' '}
                      <span
                        className={`node_type__cube node_type_creation_heading`}
                      >
                        Cube
                      </span>
                    </h2>
                    {displayMessageAfterSubmit(status)}
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
                    <div className="FullNameInput NodeCreationInput">
                      <ErrorMessage name="name" component="span" />
                      <label htmlFor="FullName">Full Name</label>
                      <FullNameField type="text" name="name" />
                    </div>
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
                        <Select
                          name="metrics"
                          options={metrics}
                          noOptionsMessage={() => 'No metrics found.'}
                          placeholder={`${metrics.length} Available Metrics`}
                          isMulti
                          isClearable
                          closeMenuOnSelect={false}
                          isDisabled={
                            !!(
                              selectedMetrics.length &&
                              selectedDimensions.length
                            )
                          }
                          onChange={e => {
                            setStagedMetrics(e.map(m => m.value));
                            setSelectedMetrics(stagedMetrics);
                          }}
                          onMenuClose={() => {
                            setSelectedMetrics(stagedMetrics);
                          }}
                        />
                      </span>
                    </div>
                    <br />
                    <br />
                    <div className="CubeCreationInput">
                      <label htmlFor="react-select-3-input">Dimensions *</label>
                      <p>
                        Select the dimensions you would like to include in the
                        cube. As you select metrics, the list of available
                        dimensions will be filtered to those shared by the
                        selected metrics. If the dimensions list is empty, no
                        shared dimensions were discovered.
                      </p>
                      <span data-testid="select-dimensions">
                        {commonDimensionsList.map(grouping => {
                          const group = grouping[1];
                          const dimensionGroupOptions = group.map(dim => {
                            return {
                              value: dim.name,
                              label: labelize(dim.name.split('.').slice(-1)[0]),
                            };
                          });
                          return (
                            <>
                              <h5
                                style={{
                                  fontWeight: 'normal',
                                  marginBottom: '5px',
                                  marginTop: '15px',
                                }}
                              >
                                <a href={`/nodes/${group[0].node_name}`}>
                                  <b>{group[0].node_display_name}</b>
                                </a>{' '}
                                via{' '}
                                <span className="HighlightPath">
                                  {group[0].path.join(' ▶ ')}
                                </span>
                              </h5>
                              <Select
                                className=""
                                name={'dimensions-' + group[0].node_name}
                                options={dimensionGroupOptions}
                                isMulti={true}
                                isClearable
                                closeMenuOnSelect={false}
                                onChange={e => {
                                  const uniqDims = e.map(d => d.value);
                                  const groupDimNames = group.map(d => d.name);
                                  if (uniqDims.length === 0) {
                                    setStagedDimensions(
                                      stagedDimensions.filter(
                                        s => !groupDimNames.includes(s),
                                      ),
                                    );
                                    setSelectedDimensions(stagedDimensions);
                                  } else {
                                    setStagedDimensions(
                                      Array.from(
                                        new Set([
                                          ...stagedDimensions,
                                          ...uniqDims,
                                        ]),
                                      ),
                                    );
                                    setSelectedDimensions(stagedDimensions);
                                  }
                                }}
                                onMenuClose={() => {
                                  setSelectedDimensions(stagedDimensions);
                                }}
                              />
                            </>
                          );
                        })}
                      </span>
                    </div>
                    <div className="CubeCreationInput">
                      <label htmlFor="react-select-3-input">Filters</label>
                      <QueryBuilder
                        fields={fields}
                        query={filters}
                        onQueryChange={q => setFilters(q)}
                      />
                    </div>
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
