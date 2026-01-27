import { useContext, useEffect, useRef, useState } from 'react';
import * as React from 'react';
import DJClientContext from '../../providers/djclient';
import { ErrorMessage, Field, Form, Formik } from 'formik';
import { FormikSelect } from '../AddEditNodePage/FormikSelect';
import AddItemIcon from '../../icons/AddItemIcon';
import { displayMessageAfterSubmit } from '../../../utils/form';
import LoadingIcon from '../../icons/LoadingIcon';
import CodeMirror from '@uiw/react-codemirror';
import { langs } from '@uiw/codemirror-extensions-langs';

export default function AddComplexDimensionLinkPopover({
  node,
  dimensions,
  existingLink = null,
  isEditMode = false,
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

  const joinTypeOptions = [
    { value: 'left', label: 'LEFT' },
    { value: 'right', label: 'RIGHT' },
    { value: 'inner', label: 'INNER' },
    { value: 'full', label: 'FULL' },
    { value: 'cross', label: 'CROSS' },
  ];

  const joinCardinalityOptions = [
    { value: 'one_to_one', label: 'ONE TO ONE' },
    { value: 'one_to_many', label: 'ONE TO MANY' },
    { value: 'many_to_one', label: 'MANY TO ONE' },
    { value: 'many_to_many', label: 'MANY TO MANY' },
  ];

  const handleSubmit = async (values, { setSubmitting, setStatus }) => {
    try {
      // If editing, remove the old link first
      if (isEditMode && existingLink) {
        await djClient.removeComplexDimensionLink(
          node.name,
          existingLink.dimension.name,
          existingLink.role,
        );
      }

      // Add the new/updated link
      const response = await djClient.addComplexDimensionLink(
        node.name,
        values.dimensionNode,
        values.joinOn.trim(),
        values.joinType || 'left',
        values.joinCardinality || 'many_to_one',
        values.role?.trim() || null,
        values.defaultValue?.trim() || null,
      );

      if (response.status === 200 || response.status === 201) {
        setStatus({
          success: `Complex dimension link ${
            isEditMode ? 'updated' : 'added'
          } successfully!`,
        });
        setTimeout(() => {
          setPopoverAnchor(false);
          window.location.reload();
        }, 1000);
      } else {
        setStatus({
          failure:
            response.json?.message ||
            `Failed to ${isEditMode ? 'update' : 'add'} link`,
        });
      }
    } catch (error) {
      setStatus({ failure: error.message });
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <>
      {isEditMode ? (
        <button
          onClick={() => setPopoverAnchor(!popoverAnchor)}
          style={{
            padding: '0.25rem 0.5rem',
            fontSize: '0.75rem',
            background: '#007bff',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            marginRight: '0.5rem',
          }}
        >
          Edit
        </button>
      ) : (
        <button
          className="edit_button"
          aria-label="AddComplexDimensionLinkTogglePopover"
          tabIndex="0"
          onClick={() => setPopoverAnchor(!popoverAnchor)}
          title="Add complex dimension link with custom SQL"
          style={{
            marginLeft: '0.5rem',
            padding: '0.25rem 0.5rem',
            fontSize: '0.875rem',
          }}
        >
          <AddItemIcon />
        </button>
      )}
      {popoverAnchor && (
        <>
          {/* Backdrop overlay */}
          <div
            style={{
              position: 'fixed',
              top: 0,
              left: 0,
              right: 0,
              bottom: 0,
              backgroundColor: 'rgba(0, 0, 0, 0.5)',
              zIndex: 1000,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              padding: '2rem',
            }}
            onClick={() => setPopoverAnchor(false)}
          >
            {/* Modal */}
            <div
              role="dialog"
              aria-label="AddComplexDimensionLinkPopover"
              ref={ref}
              onClick={e => e.stopPropagation()}
              style={{
                backgroundColor: 'white',
                borderRadius: '8px',
                boxShadow: '0 4px 20px rgba(0, 0, 0, 0.15)',
                width: '65%',
                maxWidth: '100%',
                maxHeight: '100%',
                overflow: 'visible',
                padding: '1.5rem',
                textTransform: 'none',
                fontWeight: 'normal',
                position: 'relative',
                zIndex: 1000,
              }}
            >
              <div
                style={{ maxHeight: 'calc(85vh - 4rem)', overflowY: 'auto' }}
              >
                <Formik
                  initialValues={{
                    dimensionNode: existingLink?.dimension.name || '',
                    joinType: existingLink?.join_type || 'left',
                    joinOn: existingLink?.join_sql || '',
                    joinCardinality:
                      existingLink?.join_cardinality || 'many_to_one',
                    role: existingLink?.role || '',
                    defaultValue: existingLink?.default_value || '',
                  }}
                  onSubmit={handleSubmit}
                  validate={values => {
                    const errors = {};
                    if (!values.dimensionNode) {
                      errors.dimensionNode = 'Required';
                    }
                    if (!values.joinOn) {
                      errors.joinOn = 'Required';
                    }
                    return errors;
                  }}
                >
                  {function Render({ isSubmitting, status, values }) {
                    return (
                      <Form>
                        <h3
                          style={{
                            margin: '0 0 1rem 0',
                            fontSize: '1.25rem',
                            fontWeight: '600',
                          }}
                        >
                          {isEditMode ? 'Edit' : 'Add'} Complex Dimension Link
                        </h3>
                        {displayMessageAfterSubmit(status)}

                        <div style={{ marginBottom: '1rem' }}>
                          <ErrorMessage name="dimensionNode" component="span" />
                          <label htmlFor="dimensionNode">
                            Dimension Node *
                          </label>
                          {isEditMode ? (
                            <div
                              style={{
                                padding: '0.5rem',
                                backgroundColor: '#f5f5f5',
                                borderRadius: '4px',
                                color: '#666',
                              }}
                            >
                              {existingLink?.dimension.name}
                              <small
                                style={{
                                  display: 'block',
                                  marginTop: '0.25rem',
                                  fontSize: '0.75rem',
                                  color: '#999',
                                }}
                              >
                                To link a different dimension node, remove this
                                link and create a new one
                              </small>
                            </div>
                          ) : (
                            <FormikSelect
                              selectOptions={dimensions}
                              formikFieldName="dimensionNode"
                              placeholder="Select dimension"
                            />
                          )}
                        </div>

                        <div
                          style={{
                            display: 'flex',
                            gap: '1rem',
                            marginBottom: '1rem',
                          }}
                        >
                          <div style={{ flex: 1 }}>
                            <label htmlFor="joinType">Join Type</label>
                            <FormikSelect
                              selectOptions={joinTypeOptions}
                              formikFieldName="joinType"
                              placeholder="Select join type"
                              defaultValue={
                                values.joinType
                                  ? joinTypeOptions.find(
                                      opt => opt.value === values.joinType,
                                    )
                                  : null
                              }
                            />
                          </div>
                          <div style={{ flex: 1 }}>
                            <label htmlFor="joinCardinality">
                              Join Cardinality
                            </label>
                            <FormikSelect
                              selectOptions={joinCardinalityOptions}
                              formikFieldName="joinCardinality"
                              placeholder="Select join cardinality"
                              defaultValue={
                                values.joinCardinality
                                  ? joinCardinalityOptions.find(
                                      opt =>
                                        opt.value === values.joinCardinality,
                                    )
                                  : null
                              }
                            />
                          </div>
                        </div>

                        <div style={{ marginBottom: '1rem' }}>
                          <ErrorMessage name="joinOn" component="span" />
                          <label htmlFor="joinOn">Join SQL *</label>
                          <small
                            style={{ color: '#6c757d', fontSize: '0.75rem' }}
                          >
                            Specify the join condition
                          </small>
                          <Field name="joinOn">
                            {({ field, form }) => (
                              <div
                                role="button"
                                tabIndex={0}
                                className="relative flex bg-[#282a36]"
                              >
                                <CodeMirror
                                  id="joinOn"
                                  name="joinOn"
                                  extensions={[langs.sql()]}
                                  value={field.value?.trim()}
                                  placeholder="e.g., node_table.dimension_id = dimension_table.id"
                                  width="100%"
                                  height="150px"
                                  style={{
                                    fontSize: '14px',
                                    textAlign: 'left',
                                  }}
                                  onChange={value => {
                                    form.setFieldValue('joinOn', value);
                                  }}
                                />
                              </div>
                            )}
                          </Field>
                        </div>

                        <div style={{ marginBottom: '1rem' }}>
                          <label htmlFor="role">Role (Optional)</label>
                          <Field
                            type="text"
                            name="role"
                            id="role"
                            placeholder="e.g., birth_date, registration_date"
                            style={{
                              width: '100%',
                              padding: '0.5rem',
                            }}
                          />
                          <small
                            style={{ color: '#6c757d', fontSize: '0.75rem' }}
                          >
                            Optional role if linking the same dimension multiple
                            times
                          </small>
                        </div>

                        <div style={{ marginBottom: '1rem' }}>
                          <label htmlFor="defaultValue">
                            Default Value (Optional)
                          </label>
                          <Field
                            type="text"
                            name="defaultValue"
                            id="defaultValue"
                            placeholder="e.g., Unknown, N/A"
                            style={{
                              width: '100%',
                              padding: '0.5rem',
                            }}
                          />
                          <small
                            style={{ color: '#6c757d', fontSize: '0.75rem' }}
                          >
                            Value to use when LEFT or RIGHT JOIN produces NULL
                            (wraps dimension column in COALESCE)
                          </small>
                        </div>

                        <button
                          className="add_node"
                          type="submit"
                          aria-label="SaveComplexDimensionLink"
                          disabled={isSubmitting}
                          style={{ marginTop: '1rem' }}
                        >
                          {isSubmitting ? (
                            <LoadingIcon />
                          ) : isEditMode ? (
                            'Save Changes'
                          ) : (
                            'Add Link'
                          )}
                        </button>
                      </Form>
                    );
                  }}
                </Formik>
              </div>
            </div>
          </div>
        </>
      )}
    </>
  );
}
