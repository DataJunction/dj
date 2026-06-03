import React from 'react';
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { Formik, Form } from 'formik';
import { NodeQueryField } from '../NodeQueryField';

describe('NodeQueryField', () => {
  const mockDjClient = {
    nodes: vi.fn().mockResolvedValue([]),
    node: vi.fn(),
    catalogs: vi.fn().mockResolvedValue([]),
    registerTable: vi
      .fn()
      .mockResolvedValue({ status: 200, json: { name: '' } }),
    validateNode: vi.fn(),
  };
  const renderWithFormik = (
    djClient = mockDjClient,
    initialValues = { query: '', type: 'transform', name: 'draft' },
  ) => {
    return render(
      <Formik initialValues={initialValues} onSubmit={vi.fn()}>
        <Form>
          <NodeQueryField djClient={djClient} />
        </Form>
      </Formik>,
    );
  };

  afterEach(() => {
    vi.clearAllMocks();
    vi.useRealTimers();
  });

  it('renders without crashing', () => {
    renderWithFormik();
    expect(screen.getByRole('textbox')).toBeInTheDocument();
    expect(screen.getByRole('button')).toBeInTheDocument();
  });

  it('surfaces server-side validation errors as an alert banner', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const djClient = {
      ...mockDjClient,
      validateNode: vi.fn().mockResolvedValue({
        status: 422,
        json: {
          status: 'invalid',
          message: 'Node `draft` is invalid.',
          errors: [
            {
              code: 'INVALID_SQL_QUERY',
              message:
                'All rows in a VALUES clause must have the same number of columns; got widths [1, 2].',
            },
          ],
          dependencies: [],
          missing_parents: [],
        },
      }),
    };
    renderWithFormik(djClient);

    // Type a query — the editor's onChange triggers a 700ms-debounced call to
    // validateNode. Use fireEvent on the contenteditable to simulate typing.
    const editor = document.querySelector('.cm-content');
    expect(editor).not.toBeNull();
    await act(async () => {
      fireEvent.focus(editor);
      fireEvent.input(editor, {
        target: { textContent: "SELECT * FROM VALUES (1, 'a'), AS t(x, y)" },
      });
      // advance past the 700ms debounce
      await vi.advanceTimersByTimeAsync(800);
    });

    await waitFor(() => expect(djClient.validateNode).toHaveBeenCalled());
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('Invalid SQL');
    expect(alert).toHaveTextContent(
      'All rows in a VALUES clause must have the same number of columns',
    );
  });
});
