import React from 'react';
import { render, fireEvent, waitFor, screen, within } from '@testing-library/react';
import EditColumnDescriptionPopover from '../EditColumnDescriptionPopover';
import DJClientContext from '../../../providers/djclient';

const mockDjClient = {
  DataJunctionAPI: {
    setColumnDescription: jest.fn(),
  },
};

describe('<EditColumnDescriptionPopover />', () => {
  it('renders correctly and handles successful form submission', async () => {
    // Mock necessary data
    const column = {
      name: 'column1',
      description: 'Initial description',
    };
    const node = { name: 'default.node1' };

    // Mock onSubmit function
    const onSubmitMock = jest.fn();

    mockDjClient.DataJunctionAPI.setColumnDescription.mockReturnValue({
      status: 200,
      json: { message: 'Description updated successfully' },
    });

    // Render the component
    render(
      <DJClientContext.Provider value={mockDjClient}>
        <EditColumnDescriptionPopover
          column={column}
          node={node}
          onSubmit={onSubmitMock}
        />
      </DJClientContext.Provider>,
    );

    // Open the popover
    fireEvent.click(screen.getByLabelText('EditColumnDescription'));

    // Update the description
    const dialog = screen.getByRole('dialog', { name: /edit-description/i });
    const descriptionTextarea = within(dialog).getByRole('textbox');
    fireEvent.change(descriptionTextarea, { target: { value: 'Updated description' } });

    // Submit the form
    fireEvent.click(screen.getByText('Save'));

    // Expect setColumnDescription to be called
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.setColumnDescription).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        'Updated description',
      );
      expect(screen.getByText('Saved!')).toBeInTheDocument();
      expect(onSubmitMock).toHaveBeenCalled();
    });
  });

  it('handles failed form submission', async () => {
    // Mock necessary data
    const column = {
      name: 'column1',
      description: 'Initial description',
    };
    const node = { name: 'default.node1' };

    // Mock onSubmit function
    const onSubmitMock = jest.fn();

    mockDjClient.DataJunctionAPI.setColumnDescription.mockReturnValue({
      status: 500,
      json: { message: 'Server error' },
    });

    // Render the component
    render(
      <DJClientContext.Provider value={mockDjClient}>
        <EditColumnDescriptionPopover
          column={column}
          node={node}
          onSubmit={onSubmitMock}
        />
      </DJClientContext.Provider>,
    );

    // Open the popover
    fireEvent.click(screen.getByLabelText('EditColumnDescription'));

    // Update the description
    const dialog = screen.getByRole('dialog', { name: /edit-description/i });
    const descriptionTextarea = within(dialog).getByRole('textbox');
    fireEvent.change(descriptionTextarea, { target: { value: 'Updated description' } });

    // Submit the form
    fireEvent.click(screen.getByText('Save'));

    // Expect setColumnDescription to be called and the failure message to show up
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.setColumnDescription).toHaveBeenCalledWith(
        'default.node1',
        'column1',
        'Updated description',
      );
      expect(screen.getByText('Server error')).toBeInTheDocument();
      expect(onSubmitMock).toHaveBeenCalled();
    });
  });

  it('renders with empty initial description', async () => {
    // Mock necessary data with no description
    const column = {
      name: 'column1',
      description: null,
    };
    const node = { name: 'default.node1' };

    // Mock onSubmit function
    const onSubmitMock = jest.fn();

    // Render the component
    render(
      <DJClientContext.Provider value={mockDjClient}>
        <EditColumnDescriptionPopover
          column={column}
          node={node}
          onSubmit={onSubmitMock}
        />
      </DJClientContext.Provider>,
    );

    // Open the popover
    fireEvent.click(screen.getByLabelText('EditColumnDescription'));

    // Check that the textarea is empty
    const dialog = screen.getByRole('dialog', { name: /edit-description/i });
    const descriptionTextarea = within(dialog).getByRole('textbox');
    expect(descriptionTextarea.value).toBe('');
  });
});
