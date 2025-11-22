import React from 'react';
import { render, fireEvent, waitFor, screen } from '@testing-library/react';
import LinkDimensionPopover from '../LinkDimensionPopover';
import DJClientContext from '../../../providers/djclient';

const mockDjClient = {
  DataJunctionAPI: {
    linkDimension: jest.fn(),
    unlinkDimension: jest.fn(),
  },
};

describe('<LinkDimensionPopover />', () => {
  it('renders correctly and handles form submission', async () => {
    // Mock necessary data
    const column = {
      name: 'column1',
      dimension: { name: 'default.dimension1' },
    };
    const node = { name: 'default.node1' };
    const options = [
      { value: 'default.dimension1', label: 'Dimension 1' },
      { value: 'default.dimension2', label: 'Dimension 2' },
    ];

    // Mock onSubmit function
    const onSubmitMock = jest.fn();

    mockDjClient.DataJunctionAPI.linkDimension.mockReturnValue({
      status: 200,
      json: { message: 'Success' },
    });

    mockDjClient.DataJunctionAPI.unlinkDimension.mockReturnValue({
      status: 200,
      json: { message: 'Success' },
    });

    // Render the component
    const { getByLabelText, getByText, getByTestId } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <LinkDimensionPopover
          column={column}
          dimensionNodes={['default.dimension1']}
          node={node}
          options={options}
          onSubmit={onSubmitMock}
        />
      </DJClientContext.Provider>,
    );

    // Open the popover
    fireEvent.click(getByLabelText('LinkDimension'));

    // Verify dimension1 is initially selected (react-select shows label, not value in remove button)
    await waitFor(() => {
      expect(screen.getByLabelText('Remove Dimension 1')).toBeInTheDocument();
    });

    // Click on a dimension to add it
    const linkDimension = getByTestId('link-dimension');
    fireEvent.keyDown(linkDimension.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getAllByText('Dimension 2')[0]);

    // Click on the 'Remove' button for dimension1 (using the label)
    await waitFor(() => {
      const removeButton = screen.getByLabelText('Remove Dimension 1');
      fireEvent.click(removeButton);
    });

    // Now save (this will link dimension2 and unlink dimension1)
    fireEvent.click(getByText('Save'));

    // Expect both operations to be called with success message
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.linkDimension).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        'default.dimension2',
      );
      expect(mockDjClient.DataJunctionAPI.unlinkDimension).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        'default.dimension1',
      );
      // Since unlinkDimension runs last, its message should be shown
      expect(getByText('Removed dimension link!')).toBeInTheDocument();
    });
  });

  it('handles failed form submission', async () => {
    // Mock necessary data
    const column = {
      name: 'column1',
      dimension: { name: 'default.dimension1' },
    };
    const node = { name: 'default.node1' };
    const options = [
      { value: 'default.dimension1', label: 'Dimension 1' },
      { value: 'default.dimension2', label: 'Dimension 2' },
    ];

    // Mock onSubmit function
    const onSubmitMock = jest.fn();

    mockDjClient.DataJunctionAPI.linkDimension.mockReturnValue({
      status: 500,
      json: { message: 'Failed due to nonexistent dimension' },
    });

    mockDjClient.DataJunctionAPI.unlinkDimension.mockReturnValue({
      status: 500,
      json: { message: 'Failed due to no dimension link' },
    });

    // Render the component
    const { getByLabelText, getByText, getByTestId } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <LinkDimensionPopover
          column={column}
          dimensionNodes={['default.dimension1']}
          node={node}
          options={options}
          onSubmit={onSubmitMock}
        />
      </DJClientContext.Provider>,
    );

    // Open the popover
    fireEvent.click(getByLabelText('LinkDimension'));

    // Click on a dimension to add it
    const linkDimension = getByTestId('link-dimension');
    fireEvent.keyDown(linkDimension.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getAllByText('Dimension 2')[0]);

    // Click on the 'Remove' button for dimension1 (using the label)
    await waitFor(() => {
      const removeButton = screen.getByLabelText('Remove Dimension 1');
      fireEvent.click(removeButton);
    });

    // Now save (this will attempt to link dimension2 and unlink dimension1)
    fireEvent.click(getByText('Save'));

    // Expect both operations to be called with failure messages
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.linkDimension).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        'default.dimension2',
      );
      expect(mockDjClient.DataJunctionAPI.unlinkDimension).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        'default.dimension1',
      );
      // The unlinkDimension failure message should be shown since it runs last
      expect(getByText('Failed due to no dimension link')).toBeInTheDocument();
    });
  });
});
