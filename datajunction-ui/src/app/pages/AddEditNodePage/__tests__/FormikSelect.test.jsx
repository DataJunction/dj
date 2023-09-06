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

  const setup = () => {
    const utils = render(
      <Formik initialValues={{ selectedOption: '' }} onSubmit={jest.fn()}>
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

  it('renders the select component with provided options', () => {
    setup();
    userEvent.click(screen.getByRole('combobox')); // to open the dropdown
    expect(screen.getByText('basic.one')).toBeInTheDocument();
  });
});
