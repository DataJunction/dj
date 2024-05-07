import React from 'react';
import { render, fireEvent, waitFor } from '@testing-library/react';
import DJClientContext from '../../../providers/djclient';
import AddBackfillPopover from '../AddBackfillPopover';
import { mocks } from '../../../../mocks/mockNodes';

const mockDjClient = {
  DataJunctionAPI: {
    runBackfill: jest.fn(),
  },
};

let reloadMock = jest.fn();

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
    const onSubmitMock = jest.fn();

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
});
