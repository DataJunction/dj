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
      <Formik initialValues={{ namespace: 'basic.one' }} onSubmit={jest.fn()}>
        <Form>
          <FormikSelect
            selectOptions={namespaces}
            formikFieldName="namespace"
            placeholder="Choose Namespace"
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
      <Formik initialValues={{ namespace: ['basic.one'] }} onSubmit={jest.fn()}>
        <Form>
          <FormikSelect
            selectOptions={namespaces}
            formikFieldName="namespace"
            placeholder="Choose Namespace"
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

  it('renders the single select component with provided options', () => {
    singleSelect();
    userEvent.click(screen.getByRole('combobox')); // to open the dropdown
    expect(screen.getByText('basic.one')).toBeInTheDocument();
  });

  it('renders the multi-select component with provided options', () => {
    multiSelect();
    userEvent.click(screen.getByRole('combobox')); // to open the dropdown
    expect(screen.getByText('basic.one')).toBeInTheDocument();
  });
});
