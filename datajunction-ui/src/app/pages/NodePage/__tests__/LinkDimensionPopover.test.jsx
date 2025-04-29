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

    // Click on a dimension and save
    const linkDimension = getByTestId('link-dimension');
    fireEvent.keyDown(linkDimension.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getByText('Dimension 2'));
    fireEvent.click(getByText('Save'));

    // Expect linkDimension to be called
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.linkDimension).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        'default.dimension2',
      );
      expect(getByText('Saved!')).toBeInTheDocument();
    });

    // Click on the 'Remove' option and save
    const removeButton = screen.getByLabelText('Remove default.dimension1');
    fireEvent.click(removeButton);
    fireEvent.click(getByText('Save'));

    // Expect unlinkDimension to be called
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.unlinkDimension).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        'default.dimension1',
      );
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

    // Click on a dimension and save
    const linkDimension = getByTestId('link-dimension');
    fireEvent.keyDown(linkDimension.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getByText('Dimension 2'));
    fireEvent.click(getByText('Save'));

    // Expect linkDimension to be called
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.linkDimension).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        'default.dimension2',
      );
      expect(
        getByText('Failed due to nonexistent dimension'),
      ).toBeInTheDocument();
    });

    // Click on the 'Remove' option and save
    const removeButton = screen.getByLabelText('Remove default.dimension1');
    fireEvent.click(removeButton);
    fireEvent.click(getByText('Save'));

    // Expect unlinkDimension to be called
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.unlinkDimension).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        'default.dimension1',
      );
      expect(getByText('Failed due to no dimension link')).toBeInTheDocument();
    });
  });
});
