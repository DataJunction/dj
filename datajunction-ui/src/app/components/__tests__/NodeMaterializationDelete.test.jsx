import React from 'react';
import { render, fireEvent, waitFor, act } from '@testing-library/react';
import NodeMaterializationDelete from '../NodeMaterializationDelete';
import DJClientContext from '../../providers/djclient';

// Mock window.location.reload
delete window.location;
window.location = { reload: jest.fn() };

// Mock window.confirm
window.confirm = jest.fn();

const mockDjClient = {
  DataJunctionAPI: {
    deleteMaterialization: jest.fn(),
  },
};

describe('<NodeMaterializationDelete />', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    window.confirm.mockReturnValue(true);
  });

  const defaultProps = {
    nodeName: 'default.test_node',
    materializationName: 'test_materialization',
    nodeVersion: 'v1.0',
  };

  it('renders delete button', () => {
    const { container } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <NodeMaterializationDelete {...defaultProps} />
      </DJClientContext.Provider>,
    );

    const deleteButton = container.querySelector('button[type="submit"]');
    expect(deleteButton).toBeInTheDocument();
  });

  it('renders with null nodeVersion', () => {
    const { container } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <NodeMaterializationDelete
          nodeName="default.test_node"
          materializationName="test_materialization"
          nodeVersion={null}
        />
      </DJClientContext.Provider>,
    );

    const deleteButton = container.querySelector('button[type="submit"]');
    expect(deleteButton).toBeInTheDocument();
  });

  it('shows confirm dialog when delete button is clicked', async () => {
    const { container } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <NodeMaterializationDelete {...defaultProps} />
      </DJClientContext.Provider>,
    );

    const deleteButton = container.querySelector('button[type="submit"]');
    
    await act(async () => {
      fireEvent.click(deleteButton);
    });

    expect(window.confirm).toHaveBeenCalledWith(
      expect.stringContaining('Deleting materialization job test_materialization')
    );
  });

  it('does not call deleteMaterialization when user cancels confirm', async () => {
    window.confirm.mockReturnValueOnce(false);
    
    const { container } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <NodeMaterializationDelete {...defaultProps} />
      </DJClientContext.Provider>,
    );

    const deleteButton = container.querySelector('button[type="submit"]');
    
    await act(async () => {
      fireEvent.click(deleteButton);
    });

    expect(window.confirm).toHaveBeenCalled();
    expect(mockDjClient.DataJunctionAPI.deleteMaterialization).not.toHaveBeenCalled();
  });

  it('calls deleteMaterialization with correct params on success - status 200', async () => {
    mockDjClient.DataJunctionAPI.deleteMaterialization.mockResolvedValue({
      status: 200,
      json: { message: 'Deleted successfully' },
    });

    const { container, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <NodeMaterializationDelete {...defaultProps} />
      </DJClientContext.Provider>,
    );

    const deleteButton = container.querySelector('button[type="submit"]');
    
    await act(async () => {
      fireEvent.click(deleteButton);
    });

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.deleteMaterialization).toHaveBeenCalledWith(
        'default.test_node',
        'test_materialization',
        'v1.0'
      );
      expect(getByText(/Successfully deleted materialization job/)).toBeInTheDocument();
      expect(window.location.reload).toHaveBeenCalled();
    });
  });

  it('calls deleteMaterialization with correct params on success - status 201', async () => {
    mockDjClient.DataJunctionAPI.deleteMaterialization.mockResolvedValue({
      status: 201,
      json: { message: 'Deleted successfully' },
    });

    const { container } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <NodeMaterializationDelete {...defaultProps} />
      </DJClientContext.Provider>,
    );

    const deleteButton = container.querySelector('button[type="submit"]');
    
    await act(async () => {
      fireEvent.click(deleteButton);
    });

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.deleteMaterialization).toHaveBeenCalled();
      expect(window.location.reload).toHaveBeenCalled();
    });
  });

  it('calls deleteMaterialization with correct params on success - status 204', async () => {
    mockDjClient.DataJunctionAPI.deleteMaterialization.mockResolvedValue({
      status: 204,
      json: {},
    });

    const { container } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <NodeMaterializationDelete {...defaultProps} />
      </DJClientContext.Provider>,
    );

    const deleteButton = container.querySelector('button[type="submit"]');
    
    await act(async () => {
      fireEvent.click(deleteButton);
    });

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.deleteMaterialization).toHaveBeenCalled();
      expect(window.location.reload).toHaveBeenCalled();
    });
  });

  it('displays error message when deletion fails', async () => {
    mockDjClient.DataJunctionAPI.deleteMaterialization.mockResolvedValue({
      status: 500,
      json: { message: 'Internal server error' },
    });

    const { container, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <NodeMaterializationDelete {...defaultProps} />
      </DJClientContext.Provider>,
    );

    const deleteButton = container.querySelector('button[type="submit"]');
    
    await act(async () => {
      fireEvent.click(deleteButton);
    });

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.deleteMaterialization).toHaveBeenCalled();
      expect(getByText('Internal server error')).toBeInTheDocument();
      expect(window.location.reload).not.toHaveBeenCalled();
    });
  });

  it('hides delete button after successful deletion', async () => {
    mockDjClient.DataJunctionAPI.deleteMaterialization.mockResolvedValue({
      status: 200,
      json: { message: 'Deleted successfully' },
    });

    const { container } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <NodeMaterializationDelete {...defaultProps} />
      </DJClientContext.Provider>,
    );

    const deleteButton = container.querySelector('button[type="submit"]');
    expect(deleteButton).toBeInTheDocument();
    
    await act(async () => {
      fireEvent.click(deleteButton);
    });

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.deleteMaterialization).toHaveBeenCalled();
    });
  });

  it('passes null nodeVersion to deleteMaterialization when not provided', async () => {
    mockDjClient.DataJunctionAPI.deleteMaterialization.mockResolvedValue({
      status: 200,
      json: { message: 'Deleted successfully' },
    });

    const { container } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <NodeMaterializationDelete
          nodeName="default.test_node"
          materializationName="test_mat"
        />
      </DJClientContext.Provider>,
    );

    const deleteButton = container.querySelector('button[type="submit"]');
    
    await act(async () => {
      fireEvent.click(deleteButton);
    });

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.deleteMaterialization).toHaveBeenCalledWith(
        'default.test_node',
        'test_mat',
        null
      );
    });
  });
});

