import React from 'react';
import { render, screen } from '@testing-library/react';
import { Formik, Form } from 'formik';
import { NodeQueryField } from '../NodeQueryField';

describe('NodeQueryField', () => {
  const mockDjClient = {
    nodes: vi.fn(),
    node: vi.fn(),
    catalogs: vi.fn().mockResolvedValue([]),
    registerTable: vi
      .fn()
      .mockResolvedValue({ status: 200, json: { name: '' } }),
  };
  const renderWithFormik = (djClient = mockDjClient) => {
    return render(
      <Formik initialValues={{ query: '' }} onSubmit={vi.fn()}>
        <Form>
          <NodeQueryField djClient={djClient} />
        </Form>
      </Formik>,
    );
  };

  afterEach(() => {
    vi.clearAllMocks();
  });

  it('renders without crashing', () => {
    renderWithFormik();
    expect(screen.getByRole('textbox')).toBeInTheDocument();
    expect(screen.getByRole('button')).toBeInTheDocument();
  });
});
