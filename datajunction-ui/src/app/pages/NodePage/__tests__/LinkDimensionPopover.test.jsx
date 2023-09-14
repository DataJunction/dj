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
      { value: 'Remove', label: '[Remove dimension link]' },
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
    fireEvent.click(screen.getByText('Dimension 1'));
    fireEvent.click(getByText('Save'));
    getByText('Save').click();

    // Expect linkDimension to be called
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.linkDimension).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        'default.dimension1',
      );
      expect(getByText('Saved!')).toBeInTheDocument();
    });

    // Click on the 'Remove' option and save
    fireEvent.keyDown(linkDimension.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getByText('[Remove dimension link]'));
    fireEvent.click(getByText('Save'));
    getByText('Save').click();

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
      { value: 'Remove', label: '[Remove dimension link]' },
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
    fireEvent.click(screen.getByText('Dimension 1'));
    fireEvent.click(getByText('Save'));
    getByText('Save').click();

    // Expect linkDimension to be called
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.linkDimension).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        'default.dimension1',
      );
      expect(
        getByText('Failed due to nonexistent dimension'),
      ).toBeInTheDocument();
    });

    // Click on the 'Remove' option and save
    fireEvent.keyDown(linkDimension.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getByText('[Remove dimension link]'));
    fireEvent.click(getByText('Save'));
    getByText('Save').click();

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
