import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { Formik, Form } from 'formik';
import { FullNameField } from '../FullNameField';

describe('FullNameField', () => {
  const setup = initialValues => {
    return render(
      <Formik initialValues={initialValues} onSubmit={jest.fn()}>
        <Form>
          <FullNameField name="full_name" />
        </Form>
      </Formik>,
    );
  };

  it('generates the full name based on namespace and display name', () => {
    setup({ namespace: 'cats', display_name: 'Jasper the Cat' });
    const fullNameInput = screen.getByRole('textbox');
    waitFor(() => {
      expect(fullNameInput.value).toBe('cats.jasper_the_cat');
    });
  });

  it('does not set the full name if namespace or display name is missing', () => {
    setup({ namespace: '', display_name: '' });

    const fullNameInput = screen.getByRole('textbox');
    expect(fullNameInput.value).toBe('');
  });
});
