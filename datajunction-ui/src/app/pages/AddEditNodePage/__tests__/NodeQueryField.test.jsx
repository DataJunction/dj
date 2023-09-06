import React from 'react';
import { render, screen } from '@testing-library/react';
import { Formik, Form } from 'formik';
import { NodeQueryField } from '../NodeQueryField';

describe('NodeQueryField', () => {
  const mockDjClient = {
    nodes: jest.fn(),
    node: jest.fn(),
  };
  const renderWithFormik = (djClient = mockDjClient) => {
    return render(
      <Formik initialValues={{ query: '' }} onSubmit={jest.fn()}>
        <Form>
          <NodeQueryField djClient={djClient} />
        </Form>
      </Formik>,
    );
  };

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('renders without crashing', () => {
    renderWithFormik();
    expect(screen.getByRole('textbox')).toBeInTheDocument();
    expect(screen.getByRole('button')).toBeInTheDocument();
  });
});
