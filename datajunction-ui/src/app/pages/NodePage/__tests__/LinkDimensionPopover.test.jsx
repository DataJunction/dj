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

    // Render the component - start with no dimensions
    const { getByLabelText, getByText, getByTestId } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <LinkDimensionPopover
          column={column}
          dimensionNodes={[]}
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
    await waitFor(() => {
      expect(screen.getByText('Dimension 2')).toBeInTheDocument();
    });
    fireEvent.click(screen.getByText('Dimension 2'));

    // Now save (this will link dimension2)
    fireEvent.click(getByText('Save'));

    // Expect linkDimension to be called with success message
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.linkDimension).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        'default.dimension2',
      );
      expect(getByText('Saved!')).toBeInTheDocument();
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

    // Render the component - start with no dimensions to test adding one that fails
    const { getByLabelText, getByText, getByTestId } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <LinkDimensionPopover
          column={column}
          dimensionNodes={[]}
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
    await waitFor(() => {
      expect(screen.getByText('Dimension 2')).toBeInTheDocument();
    });
    fireEvent.click(screen.getByText('Dimension 2'));

    // Now save (this will attempt to link dimension2 which will fail)
    fireEvent.click(getByText('Save'));

    // Expect linkDimension to be called with failure message
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.linkDimension).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        'default.dimension2',
      );
      // The linkDimension failure message should be shown
      expect(
        getByText('Failed due to nonexistent dimension'),
      ).toBeInTheDocument();
    });
  });
});
