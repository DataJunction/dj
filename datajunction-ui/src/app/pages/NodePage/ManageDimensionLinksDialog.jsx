import { useContext, useEffect, useRef, useState } from 'react';
import * as React from 'react';
import DJClientContext from '../../providers/djclient';
import { ErrorMessage, Field, Form, Formik } from 'formik';
import { FormikSelect } from '../AddEditNodePage/FormikSelect';
import EditIcon from '../../icons/EditIcon';
import { displayMessageAfterSubmit } from '../../../utils/form';
import LoadingIcon from '../../icons/LoadingIcon';

export default function ManageDimensionLinksDialog({
  column,
  node,
  dimensions,
  fkLinks,
  referenceLink,
  onSubmit,
}) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [isOpen, setIsOpen] = useState(false);
  const [activeTab, setActiveTab] = useState('fk');
  const ref = useRef(null);
  const [dimensionColumns, setDimensionColumns] = useState([]);
  const [selectedDimension, setSelectedDimension] = useState(
    referenceLink?.dimension || '',
  );

  useEffect(() => {
    const handleClickOutside = event => {
      if (ref.current && !ref.current.contains(event.target)) {
        setIsOpen(false);
      }
    };
    document.addEventListener('click', handleClickOutside, true);
    return () => {
      document.removeEventListener('click', handleClickOutside, true);
    };
  }, []);

  // Fetch columns for the selected dimension
  useEffect(() => {
    const fetchDimensionColumns = async () => {
      if (selectedDimension) {
        try {
          const dimensionNode = await djClient.node(selectedDimension);
          const columns = dimensionNode.columns.map(col => ({
            value: col.name,
            label: col.display_name || col.name,
          }));
          setDimensionColumns(columns);
        } catch (error) {
          console.error('Failed to fetch dimension columns:', error);
        }
      }
    };
    fetchDimensionColumns();
  }, [selectedDimension, djClient]);

  const handleFKSubmit = async (values, { setSubmitting, setStatus }) => {
    try {
      // Add FK links that are not already present
      const existingDims = new Set(fkLinks);
      const newDims = new Set(values.fkDimensions);

      // Links to add
      const toAdd = Array.from(newDims).filter(dim => !existingDims.has(dim));
      // Links to remove
      const toRemove = Array.from(existingDims).filter(
        dim => !newDims.has(dim),
      );

      const addPromises = toAdd.map(dim =>
        djClient.linkDimension(node.name, column.name, dim),
      );
      const removePromises = toRemove.map(dim =>
        djClient.unlinkDimension(node.name, column.name, dim),
      );

      await Promise.all([...addPromises, ...removePromises]);

      setStatus({ success: 'FK links updated successfully!' });
      setTimeout(() => {
        onSubmit();
      }, 500);
    } catch (error) {
      setStatus({ failure: error.message });
    } finally {
      setSubmitting(false);
    }
  };

  const handleReferenceSubmit = async (
    values,
    { setSubmitting, setStatus },
  ) => {
    try {
      const response = await djClient.addReferenceDimensionLink(
        node.name,
        column.name,
        values.dimensionNode,
        values.dimensionColumn,
        values.role || null,
      );

      if (response.status === 200 || response.status === 201) {
        setStatus({ success: 'Reference link updated successfully!' });
        setTimeout(() => {
          onSubmit();
        }, 500);
      } else {
        setStatus({ failure: response.json?.message || 'Failed to add link' });
      }
    } catch (error) {
      setStatus({ failure: error.message });
    } finally {
      setSubmitting(false);
    }
  };

  const handleRemoveReference = async () => {
    if (
      !window.confirm('Are you sure you want to remove this reference link?')
    ) {
      return;
    }

    try {
      const response = await djClient.removeReferenceDimensionLink(
        node.name,
        column.name,
      );

      if (response.status === 200 || response.status === 201) {
        onSubmit();
      } else {
        alert(response.json?.message || 'Failed to remove link');
      }
    } catch (error) {
      alert(error.message);
    }
  };

  return (
    <>
      <button
        className="edit_button dimension-link-edit"
        aria-label="ManageDimensionLinksToggle"
        tabIndex="0"
        onClick={() => setIsOpen(!isOpen)}
        title="Manage dimension links for this column"
        style={{
          marginLeft: '0.35rem',
          padding: '0',
          opacity: 0.5,
          transition: 'opacity 0.2s ease',
          background: 'transparent',
          border: 'none',
          cursor: 'pointer',
          fontSize: '12px',
          color: '#999',
          display: 'inline-flex',
          alignItems: 'center',
        }}
      >
        <EditIcon />
      </button>
      {isOpen && (
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
            onClick={() => setIsOpen(false)}
          >
            {/* Modal */}
            <div
              role="dialog"
              aria-label="ManageDimensionLinksDialog"
              ref={ref}
              onClick={e => e.stopPropagation()}
              style={{
                backgroundColor: 'white',
                borderRadius: '8px',
                boxShadow: '0 4px 20px rgba(0, 0, 0, 0.15)',
                width: '550px',
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
                <div style={{ display: 'flex', marginBottom: '1rem' }}>
                  <h3
                    style={{
                      margin: '0 1em 0 0',
                      fontSize: '1.25rem',
                      fontWeight: '600',
                    }}
                  >
                    Manage Dimension Links
                  </h3>
                  <span
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.5rem',
                    }}
                  >
                    <span
                      style={{
                        fontSize: '1rem',
                        fontWeight: '500',
                        color: '#333',
                      }}
                    >
                      {column.name}
                    </span>
                    <span
                      className="rounded-pill badge bg-secondary-soft"
                      style={{ fontSize: '0.75rem', fontWeight: 'normal' }}
                    >
                      {column.type}
                    </span>
                  </span>
                </div>

                {/* Tab Navigation */}
                <div
                  style={{
                    display: 'flex',
                    borderBottom: '2px solid #e0e0e0',
                    marginBottom: '1rem',
                  }}
                >
                  <button
                    type="button"
                    onClick={() => setActiveTab('fk')}
                    style={{
                      flex: 1,
                      padding: '0.5rem 1rem',
                      border: 'none',
                      background: 'transparent',
                      borderBottom:
                        activeTab === 'fk' ? '3px solid #007bff' : 'none',
                      color: activeTab === 'fk' ? '#007bff' : '#6c757d',
                      fontWeight: activeTab === 'fk' ? 'bold' : 'normal',
                      cursor: 'pointer',
                    }}
                  >
                    FK Links
                  </button>
                  <button
                    type="button"
                    onClick={() => setActiveTab('reference')}
                    style={{
                      flex: 1,
                      padding: '0.5rem 1rem',
                      border: 'none',
                      background: 'transparent',
                      borderBottom:
                        activeTab === 'reference'
                          ? '3px solid #007bff'
                          : 'none',
                      color: activeTab === 'reference' ? '#007bff' : '#6c757d',
                      fontWeight: activeTab === 'reference' ? 'bold' : 'normal',
                      cursor: 'pointer',
                    }}
                  >
                    Reference Links
                  </button>
                </div>

                {/* FK Links Tab */}
                {activeTab === 'fk' && (
                  <div>
                    <p
                      style={{
                        fontSize: '0.875rem',
                        color: '#6c757d',
                        marginBottom: '1rem',
                      }}
                    >
                      FK Links automatically join via the dimension's primary
                      key. Select one or more dimensions to link to this column.
                    </p>
                    <Formik
                      initialValues={{
                        fkDimensions: fkLinks,
                      }}
                      onSubmit={handleFKSubmit}
                    >
                      {function Render({ isSubmitting, status }) {
                        return (
                          <Form>
                            {displayMessageAfterSubmit(status)}
                            <div style={{ marginBottom: '1rem' }}>
                              <label htmlFor="fkDimensions">
                                Select Dimensions
                              </label>
                              <FormikSelect
                                selectOptions={dimensions}
                                formikFieldName="fkDimensions"
                                placeholder="Select dimensions"
                                isMulti={true}
                                defaultValue={fkLinks.map(dim => ({
                                  value: dim,
                                  label: dim,
                                }))}
                                menuPosition="fixed"
                              />
                            </div>
                            <button
                              className="add_node"
                              type="submit"
                              disabled={isSubmitting}
                              style={{ marginLeft: '0' }}
                            >
                              {isSubmitting ? <LoadingIcon /> : 'Save'}
                            </button>
                          </Form>
                        );
                      }}
                    </Formik>
                  </div>
                )}

                {/* Reference Links Tab */}
                {activeTab === 'reference' && (
                  <div>
                    <p
                      style={{
                        fontSize: '0.875rem',
                        color: '#6c757d',
                        marginBottom: '1rem',
                      }}
                    >
                      Reference Links explicitly map this column to a specific
                      dimension attribute. Use when the relationship is not
                      through a primary key.
                    </p>
                    <Formik
                      initialValues={{
                        dimensionNode: referenceLink?.dimension || '',
                        dimensionColumn: referenceLink?.dimension_column || '',
                        role: referenceLink?.role || '',
                      }}
                      onSubmit={handleReferenceSubmit}
                      validate={values => {
                        const errors = {};
                        if (!values.dimensionNode) {
                          errors.dimensionNode = 'Required';
                        }
                        if (!values.dimensionColumn) {
                          errors.dimensionColumn = 'Required';
                        }
                        return errors;
                      }}
                    >
                      {function Render({
                        isSubmitting,
                        status,
                        setFieldValue,
                      }) {
                        return (
                          <Form>
                            {displayMessageAfterSubmit(status)}
                            <div style={{ marginBottom: '1rem' }}>
                              <ErrorMessage
                                name="dimensionNode"
                                component="span"
                              />
                              <label htmlFor="dimensionNode">
                                Dimension Node *
                              </label>
                              <FormikSelect
                                selectOptions={dimensions}
                                formikFieldName="dimensionNode"
                                placeholder="Select dimension"
                                defaultValue={
                                  referenceLink
                                    ? {
                                        value: referenceLink.dimension,
                                        label: referenceLink.dimension,
                                      }
                                    : null
                                }
                                onChange={option => {
                                  setFieldValue(
                                    'dimensionNode',
                                    option?.value || '',
                                  );
                                  setSelectedDimension(option?.value || '');
                                  setFieldValue('dimensionColumn', '');
                                }}
                              />
                            </div>

                            <div style={{ marginBottom: '1rem' }}>
                              <ErrorMessage
                                name="dimensionColumn"
                                component="span"
                              />
                              <label htmlFor="dimensionColumn">
                                Dimension Column *
                              </label>
                              {dimensionColumns.length > 0 ? (
                                <FormikSelect
                                  selectOptions={dimensionColumns}
                                  formikFieldName="dimensionColumn"
                                  placeholder="Select column"
                                  defaultValue={
                                    referenceLink
                                      ? {
                                          value: referenceLink.dimension_column,
                                          label: referenceLink.dimension_column,
                                        }
                                      : null
                                  }
                                />
                              ) : (
                                <Field
                                  type="text"
                                  name="dimensionColumn"
                                  id="dimensionColumn"
                                  placeholder="Enter dimension column name"
                                  style={{
                                    width: '100%',
                                    padding: '0.5rem',
                                  }}
                                />
                              )}
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
                                style={{
                                  color: '#6c757d',
                                  fontSize: '0.75rem',
                                }}
                              >
                                Optional role if linking the same dimension
                                multiple times
                              </small>
                            </div>

                            <div
                              style={{
                                display: 'flex',
                                gap: '0.5rem',
                                marginTop: '1rem',
                              }}
                            >
                              <button
                                className="add_node"
                                type="submit"
                                disabled={isSubmitting}
                              >
                                {isSubmitting ? (
                                  <LoadingIcon />
                                ) : referenceLink ? (
                                  'Update Link'
                                ) : (
                                  'Add Link'
                                )}
                              </button>
                              {referenceLink && (
                                <button
                                  type="button"
                                  onClick={handleRemoveReference}
                                  style={{
                                    padding: '0.5rem 1rem',
                                    background: '#dc3545',
                                    color: 'white',
                                    border: 'none',
                                    borderRadius: '4px',
                                    cursor: 'pointer',
                                    textTransform: 'none',
                                  }}
                                >
                                  Remove Link
                                </button>
                              )}
                            </div>
                          </Form>
                        );
                      }}
                    </Formik>
                  </div>
                )}
              </div>
            </div>
          </div>
        </>
      )}
    </>
  );
}
