import React from 'react';
import { render, fireEvent, waitFor, act } from '@testing-library/react';
import AddNamespacePopover from '../AddNamespacePopover';
import DJClientContext from '../../../providers/djclient';

// Mock window.location.reload
delete window.location;
window.location = { reload: jest.fn() };

const mockDjClient = {
  DataJunctionAPI: {
    addNamespace: jest.fn(),
  },
};

describe('<AddNamespacePopover />', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  const defaultProps = {
    namespace: 'default',
  };

  it('renders the toggle button', () => {
    const { getByLabelText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <AddNamespacePopover {...defaultProps} />
      </DJClientContext.Provider>,
    );

    expect(getByLabelText('AddNamespaceTogglePopover')).toBeInTheDocument();
  });

  it('opens popover when toggle button is clicked', async () => {
    const { getByLabelText, getByRole } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <AddNamespacePopover {...defaultProps} />
      </DJClientContext.Provider>,
    );

    fireEvent.click(getByLabelText('AddNamespaceTogglePopover'));

    await waitFor(() => {
      expect(getByRole('dialog', { name: 'AddNamespacePopover' })).toBeVisible();
    });
  });

  it('pre-fills namespace field with parent namespace', async () => {
    const { getByLabelText, getByDisplayValue } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <AddNamespacePopover namespace="parent" />
      </DJClientContext.Provider>,
    );

    fireEvent.click(getByLabelText('AddNamespaceTogglePopover'));

    await waitFor(() => {
      expect(getByDisplayValue('parent.')).toBeInTheDocument();
    });
  });

  it('displays namespace input field and save button', async () => {
    const { getByLabelText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <AddNamespacePopover {...defaultProps} />
      </DJClientContext.Provider>,
    );

    fireEvent.click(getByLabelText('AddNamespaceTogglePopover'));

    await waitFor(() => {
      expect(getByLabelText('Namespace')).toBeInTheDocument();
      expect(getByLabelText('SaveNamespace')).toBeInTheDocument();
      expect(getByText('Save')).toBeInTheDocument();
    });
  });

  it('allows typing in namespace field', async () => {
    const { getByLabelText, getByPlaceholderText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <AddNamespacePopover {...defaultProps} />
      </DJClientContext.Provider>,
    );

    fireEvent.click(getByLabelText('AddNamespaceTogglePopover'));

    const namespaceInput = getByPlaceholderText('New namespace');
    
    await act(async () => {
      fireEvent.change(namespaceInput, { target: { value: 'default.new_namespace' } });
    });

    expect(namespaceInput.value).toBe('default.new_namespace');
  });

  it('calls addNamespace with correct value on form submission - success', async () => {
    mockDjClient.DataJunctionAPI.addNamespace.mockResolvedValue({
      status: 200,
      json: { message: 'Namespace created' },
    });

    const { getByLabelText, getByPlaceholderText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <AddNamespacePopover {...defaultProps} />
      </DJClientContext.Provider>,
    );

    fireEvent.click(getByLabelText('AddNamespaceTogglePopover'));

    const namespaceInput = getByPlaceholderText('New namespace');
    
    await act(async () => {
      fireEvent.change(namespaceInput, { target: { value: 'default.child' } });
    });

    const saveButton = getByLabelText('SaveNamespace');
    
    await act(async () => {
      fireEvent.click(saveButton);
    });

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.addNamespace).toHaveBeenCalledWith('default.child');
      expect(getByText('Saved')).toBeInTheDocument();
    });

    // Should reload page after success
    expect(window.location.reload).toHaveBeenCalled();
  });

  it('calls addNamespace with correct value on form submission - status 201', async () => {
    mockDjClient.DataJunctionAPI.addNamespace.mockResolvedValue({
      status: 201,
      json: { message: 'Namespace created' },
    });

    const { getByLabelText, getByPlaceholderText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <AddNamespacePopover {...defaultProps} />
      </DJClientContext.Provider>,
    );

    fireEvent.click(getByLabelText('AddNamespaceTogglePopover'));

    const namespaceInput = getByPlaceholderText('New namespace');
    
    await act(async () => {
      fireEvent.change(namespaceInput, { target: { value: 'default.another' } });
    });

    const saveButton = getByLabelText('SaveNamespace');
    
    await act(async () => {
      fireEvent.click(saveButton);
    });

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.addNamespace).toHaveBeenCalledWith('default.another');
      expect(getByText('Saved')).toBeInTheDocument();
    });

    expect(window.location.reload).toHaveBeenCalled();
  });

  it('displays error message when addNamespace fails', async () => {
    mockDjClient.DataJunctionAPI.addNamespace.mockResolvedValue({
      status: 400,
      json: { message: 'Namespace already exists' },
    });

    const { getByLabelText, getByPlaceholderText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <AddNamespacePopover {...defaultProps} />
      </DJClientContext.Provider>,
    );

    fireEvent.click(getByLabelText('AddNamespaceTogglePopover'));

    const namespaceInput = getByPlaceholderText('New namespace');
    
    await act(async () => {
      fireEvent.change(namespaceInput, { target: { value: 'default.duplicate' } });
    });

    const saveButton = getByLabelText('SaveNamespace');
    
    await act(async () => {
      fireEvent.click(saveButton);
    });

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.addNamespace).toHaveBeenCalledWith('default.duplicate');
      expect(getByText('Namespace already exists')).toBeInTheDocument();
    });

    // Should still reload page even on failure
    expect(window.location.reload).toHaveBeenCalled();
  });

  it('closes popover when toggle button is clicked again', async () => {
    const { getByLabelText, getByRole, container } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <AddNamespacePopover {...defaultProps} />
      </DJClientContext.Provider>,
    );

    // Open popover
    fireEvent.click(getByLabelText('AddNamespaceTogglePopover'));

    await waitFor(() => {
      expect(getByRole('dialog')).toBeVisible();
    });

    // Close popover
    fireEvent.click(getByLabelText('AddNamespaceTogglePopover'));

    // Popover should still exist but be hidden
    const popover = container.querySelector('[role="dialog"]');
    expect(popover).toHaveStyle({ display: 'none' });
  });

  it('handles nested namespace creation', async () => {
    mockDjClient.DataJunctionAPI.addNamespace.mockResolvedValue({
      status: 200,
      json: { message: 'Namespace created' },
    });

    const { getByLabelText, getByDisplayValue } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <AddNamespacePopover namespace="parent.child" />
      </DJClientContext.Provider>,
    );

    fireEvent.click(getByLabelText('AddNamespaceTogglePopover'));

    await waitFor(() => {
      expect(getByDisplayValue('parent.child.')).toBeInTheDocument();
    });
  });

  it('submits with initial value if not changed', async () => {
    mockDjClient.DataJunctionAPI.addNamespace.mockResolvedValue({
      status: 200,
      json: { message: 'Namespace created' },
    });

    const { getByLabelText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <AddNamespacePopover namespace="test" />
      </DJClientContext.Provider>,
    );

    fireEvent.click(getByLabelText('AddNamespaceTogglePopover'));

    const saveButton = getByLabelText('SaveNamespace');
    
    await act(async () => {
      fireEvent.click(saveButton);
    });

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.addNamespace).toHaveBeenCalledWith('test.');
      expect(getByText('Saved')).toBeInTheDocument();
    });
  });
});

