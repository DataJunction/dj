import React from 'react';
import { render, fireEvent, waitFor } from '@testing-library/react';
import DJClientContext from '../../../providers/djclient';
import AddBackfillPopover from '../AddBackfillPopover';
import { mocks } from '../../../../mocks/mockNodes';

const mockDjClient = {
  DataJunctionAPI: {
    runBackfill: vi.fn(),
  },
};

let reloadMock = vi.fn();

beforeEach(() => {
  delete window.location;
  window.location = { reload: reloadMock };
});

afterEach(() => {
  reloadMock.mockClear();
});

describe('<AddBackfillPopover />', () => {
  it('renders correctly and handles form submission', async () => {
    // Mock onSubmit function
    const onSubmitMock = vi.fn();

    mockDjClient.DataJunctionAPI.runBackfill.mockReturnValue({
      status: 201,
      json: { message: '' },
    });

    // Render the component
    const { getByLabelText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <AddBackfillPopover
          node={mocks.mockTransformNode}
          materialization={mocks.nodeMaterializations}
          onSubmit={onSubmitMock}
        />
      </DJClientContext.Provider>,
    );

    // Open the popover
    fireEvent.click(getByLabelText('AddBackfill'));

    fireEvent.click(getByText('Save'));

    // Expect setAttributes to be called
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.runBackfill).toHaveBeenCalled();
      expect(getByText('Saved!')).toBeInTheDocument();
    });
  });

  it('keeps role-played cube partitions distinct', async () => {
    mockDjClient.DataJunctionAPI.runBackfill.mockReturnValue({
      status: 201,
      json: { message: '' },
    });
    const node = {
      name: 'v3.role_cube',
      type: 'cube',
      columns: ['order', 'ship'].map(role => ({
        name: 'v3.date.date_id',
        dimension_column: `[${role}]`,
        display_name: `${role} date`,
        partition: {
          type_: 'temporal',
        },
      })),
    };

    const { getByLabelText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <AddBackfillPopover
          node={node}
          materialization={{ name: 'role_cube_mat' }}
          onSubmit={vi.fn()}
        />
      </DJClientContext.Provider>,
    );

    fireEvent.click(getByLabelText('AddBackfill'));
    fireEvent.click(getByText('Save'));

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.runBackfill).toHaveBeenCalledWith(
        'v3.role_cube',
        'role_cube_mat',
        expect.arrayContaining([
          expect.objectContaining({
            columnName: 'v3.date.date_id[order]',
          }),
          expect.objectContaining({
            columnName: 'v3.date.date_id[ship]',
          }),
        ]),
      );
    });
  });
});
