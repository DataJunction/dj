import React from 'react';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Formik, Form } from 'formik';
import { FormikSelect } from '../FormikSelect';

describe('FormikSelect', () => {
  const namespaces = [
    { value: 'default', label: 'default' },
    { value: 'basic', label: 'basic' },
    { value: 'basic.one', label: 'basic.one' },
    { value: 'basic.two', label: 'basic.two' },
  ];

  const singleSelect = () => {
    const utils = render(
      <Formik initialValues={{ namespace: '' }} onSubmit={jest.fn()}>
        <Form>
          <FormikSelect
            selectOptions={namespaces}
            formikFieldName="namespace"
            placeholder="Choose Namespace"
            defaultValue={{
              value: 'basic.one',
              label: 'basic.one',
            }}
          />
        </Form>
      </Formik>,
    );

    const selectInput = screen.getByRole('combobox');
    return {
      ...utils,
      selectInput,
    };
  };

  const multiSelect = () => {
    const utils = render(
      <Formik initialValues={{ namespace: [] }} onSubmit={jest.fn()}>
        <Form>
          <FormikSelect
            selectOptions={namespaces}
            formikFieldName="namespace"
            placeholder="Choose Namespace"
            defaultValue={{
              value: 'basic.one',
              label: 'basic.one',
            }}
            isMulti={true}
          />
        </Form>
      </Formik>,
    );

    const selectInput = screen.getByRole('combobox');
    return {
      ...utils,
      selectInput,
    };
  };

  it('renders the single select component with provided options', async () => {
    singleSelect();
    await userEvent.click(screen.getByRole('combobox')); // to open the dropdown
    expect(screen.getByText('basic.one')).toBeInTheDocument();
  });

  it('renders the multi-select component with provided options', async () => {
    multiSelect();
    await userEvent.click(screen.getByRole('combobox')); // to open the dropdown
    expect(screen.getByText('basic.one')).toBeInTheDocument();
  });

  it('handles undefined selectOptions gracefully for single-select', () => {
    const utils = render(
      <Formik initialValues={{ test: '' }} onSubmit={jest.fn()}>
        <Form>
          <FormikSelect
            selectOptions={undefined}
            formikFieldName="test"
            placeholder="Choose Option"
          />
        </Form>
      </Formik>,
    );

    // Should render without crashing
    const selectInput = screen.getByRole('combobox');
    expect(selectInput).toBeInTheDocument();
  });

  it('handles undefined selectOptions gracefully for multi-select', () => {
    const utils = render(
      <Formik initialValues={{ test: [] }} onSubmit={jest.fn()}>
        <Form>
          <FormikSelect
            selectOptions={undefined}
            formikFieldName="test"
            placeholder="Choose Options"
            isMulti={true}
          />
        </Form>
      </Formik>,
    );

    // Should render without crashing
    const selectInput = screen.getByRole('combobox');
    expect(selectInput).toBeInTheDocument();
  });

  it('handles undefined selectOptions with existing field value for single-select', () => {
    const utils = render(
      <Formik initialValues={{ test: 'existing-value' }} onSubmit={jest.fn()}>
        <Form>
          <FormikSelect
            selectOptions={undefined}
            formikFieldName="test"
            placeholder="Choose Option"
          />
        </Form>
      </Formik>,
    );

    // Should render without crashing even when field has a value
    const selectInput = screen.getByRole('combobox');
    expect(selectInput).toBeInTheDocument();
  });

  it('handles undefined selectOptions with existing field value for multi-select', () => {
    const utils = render(
      <Formik
        initialValues={{ test: ['value1', 'value2'] }}
        onSubmit={jest.fn()}
      >
        <Form>
          <FormikSelect
            selectOptions={undefined}
            formikFieldName="test"
            placeholder="Choose Options"
            isMulti={true}
          />
        </Form>
      </Formik>,
    );

    // Should render without crashing even when field has values
    const selectInput = screen.getByRole('combobox');
    expect(selectInput).toBeInTheDocument();
  });
});
