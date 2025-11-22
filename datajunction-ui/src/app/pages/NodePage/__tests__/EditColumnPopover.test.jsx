import React from 'react';
import { render, fireEvent, waitFor, screen } from '@testing-library/react';
import EditColumnPopover from '../EditColumnPopover';
import DJClientContext from '../../../providers/djclient';

const mockDjClient = {
  DataJunctionAPI: {
    setAttributes: jest.fn(),
  },
};

describe('<EditColumnPopover />', () => {
  it('renders correctly and handles form submission', async () => {
    // Mock necessary data
    const column = {
      name: 'column1',
      dimension: { name: 'dimension1' },
      attributes: [
        { attribute_type: { name: 'primary_key', namespace: 'system' } },
      ],
    };
    const node = { name: 'default.node1' };
    const options = [
      { value: 'dimension', label: 'Dimension' },
      { value: 'primary_key', label: 'Primary Key' },
    ];

    // Mock onSubmit function
    const onSubmitMock = jest.fn();

    mockDjClient.DataJunctionAPI.setAttributes.mockReturnValue({
      status: 201,
      json: [
        {
          name: 'id',
          type: 'int',
          attributes: [
            { attribute_type: { name: 'primary_key', namespace: 'system' } },
          ],
          dimension: null,
        },
      ],
    });

    // Render the component
    const { getByLabelText, getByText, getByTestId } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <EditColumnPopover
          column={column}
          node={node}
          options={options}
          onSubmit={onSubmitMock}
        />
      </DJClientContext.Provider>,
    );

    // Open the popover
    fireEvent.click(getByLabelText('EditColumn'));

    // Click on one attribute in the select
    const editAttributes = getByTestId('edit-attributes');
    fireEvent.keyDown(editAttributes.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getByText('Dimension'));
    fireEvent.click(getByText('Save'));

    // Expect setAttributes to be called
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.setAttributes).toHaveBeenCalled();
      expect(getByText('Saved!')).toBeInTheDocument();
    });

    // Add Primary Key to the existing Dimension selection (don't click Dimension again)
    fireEvent.keyDown(editAttributes.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getByText('Primary Key'));
    fireEvent.click(getByText('Save'));

    // Expect setAttributes to be called with both attributes
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.setAttributes).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        ['dimension', 'primary_key'],
      );
      expect(getByText('Saved!')).toBeInTheDocument();
    });

    // Close the popover
    fireEvent.click(getByLabelText('EditColumn'));
  });

  it('handles failed form submission', async () => {
    // Mock necessary data
    const column = {
      name: 'column1',
      dimension: { name: 'dimension1' },
      attributes: [
        { attribute_type: { name: 'primary_key', namespace: 'system' } },
      ],
    };
    const node = { name: 'default.node1' };
    const options = [
      { value: 'dimension', label: 'Dimension' },
      { value: 'primary_key', label: 'Primary Key' },
    ];

    // Mock onSubmit function
    const onSubmitMock = jest.fn();

    mockDjClient.DataJunctionAPI.setAttributes.mockReturnValue({
      status: 500,
      json: { message: 'bad request' },
    });

    // Render the component
    const { getByLabelText, getByText, getByTestId } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <EditColumnPopover
          column={column}
          node={node}
          options={options}
          onSubmit={onSubmitMock}
        />
      </DJClientContext.Provider>,
    );

    // Open the popover
    fireEvent.click(getByLabelText('EditColumn'));

    // Click on one attribute in the select
    const editAttributes = getByTestId('edit-attributes');
    fireEvent.keyDown(editAttributes.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getByText('Dimension'));
    fireEvent.click(getByText('Save'));

    // Expect setAttributes to be called and the failure message to show up
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.setAttributes).toHaveBeenCalled();
      expect(getByText('bad request')).toBeInTheDocument();
    });

    // Close the popover
    fireEvent.click(getByLabelText('EditColumn'));
  });
});
