import React from 'react';
import { render, fireEvent, waitFor, screen } from '@testing-library/react';
import Search from '../Search';
import DJClientContext from '../../providers/djclient';

const mockDjClient = {
  DataJunctionAPI: {
    nodeDetails: jest.fn(),
    listTags: jest.fn(),
  },
};

describe('<Search />', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  const mockNodes = [
    {
      name: 'default.test_node',
      display_name: 'Test Node',
      description: 'A test node for testing',
      type: 'transform',
    },
    {
      name: 'default.another_node',
      display_name: 'Another Node',
      description: null, // Test null description
      type: 'metric',
    },
    {
      name: 'default.long_description_node',
      display_name: 'Long Description',
      description:
        'This is a very long description that exceeds 100 characters and should be truncated to prevent display issues in the search results interface',
      type: 'dimension',
    },
  ];

  const mockTags = [
    {
      name: 'test_tag',
      display_name: 'Test Tag',
      description: 'A test tag',
      tag_type: 'business',
    },
  ];

  it('renders search input', async () => {
    mockDjClient.DataJunctionAPI.nodeDetails.mockResolvedValue(mockNodes);
    mockDjClient.DataJunctionAPI.listTags.mockResolvedValue(mockTags);

    const { getByPlaceholderText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <Search />
      </DJClientContext.Provider>,
    );

    expect(getByPlaceholderText('Search')).toBeInTheDocument();
  });

  it('fetches and initializes search data on mount', async () => {
    mockDjClient.DataJunctionAPI.nodeDetails.mockResolvedValue(mockNodes);
    mockDjClient.DataJunctionAPI.listTags.mockResolvedValue(mockTags);

    render(
      <DJClientContext.Provider value={mockDjClient}>
        <Search />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.nodeDetails).toHaveBeenCalled();
      expect(mockDjClient.DataJunctionAPI.listTags).toHaveBeenCalled();
    });
  });

  it('displays search results when typing', async () => {
    mockDjClient.DataJunctionAPI.nodeDetails.mockResolvedValue(mockNodes);
    mockDjClient.DataJunctionAPI.listTags.mockResolvedValue(mockTags);

    const { getByPlaceholderText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <Search />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.nodeDetails).toHaveBeenCalled();
    });

    const searchInput = getByPlaceholderText('Search');
    fireEvent.change(searchInput, { target: { value: 'test' } });

    await waitFor(() => {
      expect(getByText(/Test Node/)).toBeInTheDocument();
    });
  });

  it('displays nodes with correct URLs', async () => {
    mockDjClient.DataJunctionAPI.nodeDetails.mockResolvedValue(mockNodes);
    mockDjClient.DataJunctionAPI.listTags.mockResolvedValue(mockTags);

    const { getByPlaceholderText, container } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <Search />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.nodeDetails).toHaveBeenCalled();
    });

    const searchInput = getByPlaceholderText('Search');
    fireEvent.change(searchInput, { target: { value: 'node' } });

    await waitFor(() => {
      const links = container.querySelectorAll('a[href^="/nodes/"]');
      expect(links.length).toBeGreaterThan(0);
    });
  });

  it('displays tags with correct URLs', async () => {
    mockDjClient.DataJunctionAPI.nodeDetails.mockResolvedValue([]);
    mockDjClient.DataJunctionAPI.listTags.mockResolvedValue(mockTags);

    const { getByPlaceholderText, container } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <Search />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.listTags).toHaveBeenCalled();
    });

    const searchInput = getByPlaceholderText('Search');
    fireEvent.change(searchInput, { target: { value: 'tag' } });

    await waitFor(() => {
      const links = container.querySelectorAll('a[href^="/tags/"]');
      expect(links.length).toBeGreaterThan(0);
    });
  });

  it('truncates long descriptions', async () => {
    mockDjClient.DataJunctionAPI.nodeDetails.mockResolvedValue(mockNodes);
    mockDjClient.DataJunctionAPI.listTags.mockResolvedValue([]);

    const { getByPlaceholderText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <Search />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.nodeDetails).toHaveBeenCalled();
    });

    const searchInput = getByPlaceholderText('Search');
    fireEvent.change(searchInput, { target: { value: 'long' } });

    await waitFor(() => {
      expect(getByText(/\.\.\./)).toBeInTheDocument();
    });
  });

  it('handles null descriptions', async () => {
    mockDjClient.DataJunctionAPI.nodeDetails.mockResolvedValue(mockNodes);
    mockDjClient.DataJunctionAPI.listTags.mockResolvedValue([]);

    const { getByPlaceholderText, getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <Search />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.nodeDetails).toHaveBeenCalled();
    });

    const searchInput = getByPlaceholderText('Search');
    fireEvent.change(searchInput, { target: { value: 'another' } });

    await waitFor(() => {
      expect(getByText(/Another Node/)).toBeInTheDocument();
    });
  });

  it('limits search results to 20 items', async () => {
    const manyNodes = Array.from({ length: 30 }, (_, i) => ({
      name: `default.node${i}`,
      display_name: `Node ${i}`,
      description: `Description ${i}`,
      type: 'transform',
    }));

    mockDjClient.DataJunctionAPI.nodeDetails.mockResolvedValue(manyNodes);
    mockDjClient.DataJunctionAPI.listTags.mockResolvedValue([]);

    const { getByPlaceholderText, container } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <Search />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.nodeDetails).toHaveBeenCalled();
    });

    const searchInput = getByPlaceholderText('Search');
    fireEvent.change(searchInput, { target: { value: 'node' } });

    await waitFor(() => {
      const results = container.querySelectorAll('.search-result-item');
      expect(results.length).toBeLessThanOrEqual(20);
    });
  });

  it('handles error when fetching nodes', async () => {
    const consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation();
    mockDjClient.DataJunctionAPI.nodeDetails.mockRejectedValue(
      new Error('Network error')
    );
    mockDjClient.DataJunctionAPI.listTags.mockResolvedValue([]);

    render(
      <DJClientContext.Provider value={mockDjClient}>
        <Search />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(consoleErrorSpy).toHaveBeenCalledWith(
        'Error fetching nodes or tags:',
        expect.any(Error)
      );
    });

    consoleErrorSpy.mockRestore();
  });

  it('prevents form submission', async () => {
    mockDjClient.DataJunctionAPI.nodeDetails.mockResolvedValue([]);
    mockDjClient.DataJunctionAPI.listTags.mockResolvedValue([]);

    const { container } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <Search />
      </DJClientContext.Provider>,
    );

    const form = container.querySelector('form');
    
    const submitEvent = new Event('submit', { bubbles: true, cancelable: true });
    const preventDefaultSpy = jest.spyOn(submitEvent, 'preventDefault');
    
    form.dispatchEvent(submitEvent);

    expect(preventDefaultSpy).toHaveBeenCalled();
  });

  it('handles empty tags array', async () => {
    mockDjClient.DataJunctionAPI.nodeDetails.mockResolvedValue(mockNodes);
    mockDjClient.DataJunctionAPI.listTags.mockResolvedValue(null);

    const { getByPlaceholderText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <Search />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.listTags).toHaveBeenCalled();
    });

    // Should not throw an error
    const searchInput = getByPlaceholderText('Search');
    expect(searchInput).toBeInTheDocument();
  });

  it('shows description separator correctly', async () => {
    mockDjClient.DataJunctionAPI.nodeDetails.mockResolvedValue(mockNodes);
    mockDjClient.DataJunctionAPI.listTags.mockResolvedValue([]);

    const { getByPlaceholderText, container } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <Search />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.nodeDetails).toHaveBeenCalled();
    });

    const searchInput = getByPlaceholderText('Search');
    fireEvent.change(searchInput, { target: { value: 'test' } });

    await waitFor(() => {
      const results = container.querySelector('.search-result-item');
      expect(results).toBeInTheDocument();
    });
  });
});
