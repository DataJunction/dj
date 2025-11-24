import React from 'react';
import { render, fireEvent, waitFor, screen, act } from '@testing-library/react';
import ManageDimensionLinksDialog from '../ManageDimensionLinksDialog';
import DJClientContext from '../../../providers/djclient';

// Mock window.location.reload
delete window.location;
window.location = { reload: jest.fn() };

// Mock window.confirm
window.confirm = jest.fn(() => true);

const mockDjClient = {
  DataJunctionAPI: {
    linkDimension: jest.fn(),
    unlinkDimension: jest.fn(),
    addReferenceDimensionLink: jest.fn(),
    removeReferenceDimensionLink: jest.fn(),
  },
  node: jest.fn().mockResolvedValue({
    name: 'default.test_dimension',
    columns: [{ name: 'id', type: 'int' }],
  }),
};

describe('<ManageDimensionLinksDialog />', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    window.alert = jest.fn();
    window.confirm.mockReturnValue(true);
  });

  const defaultProps = {
    column: { name: 'test_column', type: 'int' },
    node: { name: 'default.node1' },
    dimensions: [
      { value: 'default.dim1', label: 'dim1 (5 links)' },
      { value: 'default.dim2', label: 'dim2 (3 links)' },
    ],
    fkLinks: [],
    onSubmit: jest.fn(),
  };

  it('renders the toggle button', () => {
    const { getByLabelText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <ManageDimensionLinksDialog {...defaultProps} />
      </DJClientContext.Provider>,
    );
    
    expect(getByLabelText('ManageDimensionLinksToggle')).toBeInTheDocument();
  });

  it('opens modal when toggle button is clicked', async () => {
    const { getByLabelText, getByRole } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <ManageDimensionLinksDialog {...defaultProps} />
      </DJClientContext.Provider>,
    );
    
    fireEvent.click(getByLabelText('ManageDimensionLinksToggle'));
    
    await waitFor(() => {
      expect(getByRole('dialog', { name: 'ManageDimensionLinksDialog' })).toBeInTheDocument();
    });
  });

  it('displays column name and type in header', async () => {
    const { getByLabelText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <ManageDimensionLinksDialog
          column={{ name: 'test_column', type: 'varchar' }}
          node={defaultProps.node}
          dimensions={defaultProps.dimensions}
          fkLinks={[]}
          onSubmit={defaultProps.onSubmit}
        />
      </DJClientContext.Provider>,
    );
    
    fireEvent.click(getByLabelText('ManageDimensionLinksToggle'));
    
    await waitFor(() => {
      expect(getByText('test_column')).toBeInTheDocument();
      expect(getByText('varchar')).toBeInTheDocument();
    });
  });

  it('shows two tabs: FK Links and Reference Links', async () => {
    const { getByLabelText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <ManageDimensionLinksDialog {...defaultProps} />
      </DJClientContext.Provider>,
    );
    
    fireEvent.click(getByLabelText('ManageDimensionLinksToggle'));
    
    await waitFor(() => {
      expect(getByText('FK Links')).toBeInTheDocument();
      expect(getByText('Reference Links')).toBeInTheDocument();
    });
  });

  it('switches to reference links tab when clicked', async () => {
    const { getByLabelText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <ManageDimensionLinksDialog {...defaultProps} />
      </DJClientContext.Provider>,
    );
    
    fireEvent.click(getByLabelText('ManageDimensionLinksToggle'));
    
    await waitFor(() => {
      expect(getByText('Reference Links')).toBeInTheDocument();
    });

    fireEvent.click(getByText('Reference Links'));
    
    await waitFor(() => {
      expect(getByText('Dimension Node *')).toBeInTheDocument();
      expect(getByText('Dimension Column *')).toBeInTheDocument();
    });
  });


  it('displays FK links when provided', async () => {
    const propsWithFkLinks = {
      ...defaultProps,
      fkLinks: ['default.dim1', 'default.dim2'],
    };

    const { getByLabelText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <ManageDimensionLinksDialog {...propsWithFkLinks} />
      </DJClientContext.Provider>,
    );
    
    fireEvent.click(getByLabelText('ManageDimensionLinksToggle'));
    
    await waitFor(() => {
      // The FK Links tab should be displayed by default
      expect(getByLabelText('ManageDimensionLinksToggle')).toBeInTheDocument();
    });
  });

  it('shows FK Links form with select dimensions field', async () => {
    const { getByLabelText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <ManageDimensionLinksDialog {...defaultProps} />
      </DJClientContext.Provider>,
    );
    
    fireEvent.click(getByLabelText('ManageDimensionLinksToggle'));
    
    await waitFor(() => {
      expect(getByText('Select Dimensions')).toBeInTheDocument();
      expect(getByText('Save')).toBeInTheDocument();
    });
  });

  it('displays reference link form fields', async () => {
    const { getByLabelText, getByText, getByPlaceholderText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <ManageDimensionLinksDialog {...defaultProps} />
      </DJClientContext.Provider>,
    );
    
    fireEvent.click(getByLabelText('ManageDimensionLinksToggle'));
    
    await waitFor(() => {
      expect(getByText('Reference Links')).toBeInTheDocument();
    });

    // Switch to reference link tab
    fireEvent.click(getByText('Reference Links'));
    
    await waitFor(() => {
      expect(getByText('Dimension Node *')).toBeInTheDocument();
      expect(getByText('Dimension Column *')).toBeInTheDocument();
      expect(getByPlaceholderText('e.g., birth_date, registration_date')).toBeInTheDocument();
      expect(getByText('Add Link')).toBeInTheDocument();
    });
  });

  it('handles removing reference link', async () => {
    mockDjClient.DataJunctionAPI.removeReferenceDimensionLink.mockResolvedValue({
      status: 200,
    });

    const propsWithReferenceLink = {
      ...defaultProps,
      referenceLink: {
        dimension: 'default.dim1',
        dimension_column: 'id',
      },
    };

    const { getByLabelText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <ManageDimensionLinksDialog {...propsWithReferenceLink} />
      </DJClientContext.Provider>,
    );
    
    fireEvent.click(getByLabelText('ManageDimensionLinksToggle'));
    
    await waitFor(() => {
      expect(getByText('Reference Links')).toBeInTheDocument();
    });

    // Switch to reference link tab
    fireEvent.click(getByText('Reference Links'));
    
    await waitFor(() => {
      expect(getByText('Remove Link')).toBeInTheDocument();
    });

    // Remove the link
    const removeButton = getByText('Remove Link');
    await act(async () => {
      fireEvent.click(removeButton);
    });

    await waitFor(() => {
      expect(window.confirm).toHaveBeenCalledWith('Are you sure you want to remove this reference link?');
      expect(mockDjClient.DataJunctionAPI.removeReferenceDimensionLink).toHaveBeenCalledWith(
        'default.node1',
        'test_column'
      );
      expect(window.location.reload).toHaveBeenCalled();
    });
  });

  it('does not remove reference link when user cancels confirm dialog', async () => {
    window.confirm.mockReturnValueOnce(false);

    const propsWithReferenceLink = {
      ...defaultProps,
      referenceLink: {
        dimension: 'default.dim1',
        dimension_column: 'id',
      },
    };

    const { getByLabelText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <ManageDimensionLinksDialog {...propsWithReferenceLink} />
      </DJClientContext.Provider>,
    );
    
    fireEvent.click(getByLabelText('ManageDimensionLinksToggle'));
    fireEvent.click(getByText('Reference Links'));
    
    await waitFor(() => {
      expect(getByText('Remove Link')).toBeInTheDocument();
    });

    const removeButton = getByText('Remove Link');
    await act(async () => {
      fireEvent.click(removeButton);
    });

    expect(mockDjClient.DataJunctionAPI.removeReferenceDimensionLink).not.toHaveBeenCalled();
  });

  it('handles failed reference link removal', async () => {
    mockDjClient.DataJunctionAPI.removeReferenceDimensionLink.mockResolvedValue({
      status: 500,
      json: { message: 'Server error' },
    });

    const propsWithReferenceLink = {
      ...defaultProps,
      referenceLink: {
        dimension: 'default.dim1',
        dimension_column: 'id',
      },
    };

    const { getByLabelText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <ManageDimensionLinksDialog {...propsWithReferenceLink} />
      </DJClientContext.Provider>,
    );
    
    fireEvent.click(getByLabelText('ManageDimensionLinksToggle'));
    fireEvent.click(getByText('Reference Links'));
    
    await waitFor(() => {
      expect(getByText('Remove Link')).toBeInTheDocument();
    });

    const removeButton = getByText('Remove Link');
    await act(async () => {
      fireEvent.click(removeButton);
    });

    await waitFor(() => {
      expect(window.alert).toHaveBeenCalled();
      expect(window.location.reload).not.toHaveBeenCalled();
    });
  });

  it('shows Update Link button when reference link exists', async () => {
    const propsWithReferenceLink = {
      ...defaultProps,
      referenceLink: {
        dimension: 'default.dim1',
        dimension_column: 'user_id',
      },
    };

    const { getByLabelText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <ManageDimensionLinksDialog {...propsWithReferenceLink} />
      </DJClientContext.Provider>,
    );
    
    fireEvent.click(getByLabelText('ManageDimensionLinksToggle'));
    fireEvent.click(getByText('Reference Links'));
    
    await waitFor(() => {
      expect(getByText('Update Link')).toBeInTheDocument();
      expect(getByText('Remove Link')).toBeInTheDocument();
    });
  });

  it('shows validation error when submitting reference link without required fields', async () => {
    const { getByLabelText, getByText, getAllByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <ManageDimensionLinksDialog {...defaultProps} />
      </DJClientContext.Provider>,
    );
    
    fireEvent.click(getByLabelText('ManageDimensionLinksToggle'));
    fireEvent.click(getByText('Reference Links'));
    
    await waitFor(() => {
      expect(getByText('Add Link')).toBeInTheDocument();
    });

    // Try to submit without filling required fields
    const saveButton = getByText('Add Link');
    await act(async () => {
      fireEvent.click(saveButton);
    });

    await waitFor(() => {
      const requiredErrors = getAllByText('Required');
      expect(requiredErrors.length).toBeGreaterThan(0);
    });
  });

  it('closes modal when clicking backdrop', async () => {
    const { getByLabelText, getByRole, queryByRole } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <ManageDimensionLinksDialog {...defaultProps} />
      </DJClientContext.Provider>,
    );
    
    fireEvent.click(getByLabelText('ManageDimensionLinksToggle'));
    
    await waitFor(() => {
      expect(getByRole('dialog')).toBeInTheDocument();
    });

    // Click the backdrop
    const backdrop = getByRole('dialog').parentElement;
    fireEvent.click(backdrop);
    
    await waitFor(() => {
      expect(queryByRole('dialog')).not.toBeInTheDocument();
    });
  });

});
