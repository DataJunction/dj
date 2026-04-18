import React from 'react';
import { render, fireEvent, waitFor, act } from '@testing-library/react';
import Search from '../Search';
import DJClientContext from '../../providers/djclient';

const mockNodes = [
  {
    name: 'default.test_node',
    display_name: 'Test Node',
    description: 'A test node for testing',
    type: 'transform',
    kind: 'node',
  },
  {
    name: 'default.another_node',
    display_name: 'Another Node',
    description: '',
    type: 'metric',
    kind: 'node',
  },
  {
    name: 'default.long_description_node',
    display_name: 'Long Description',
    description:
      'This is a very long description that exceeds 100 characters and should be truncated to prevent display issues in the search results interface',
    type: 'dimension',
    kind: 'node',
  },
];

const mockTags = [
  {
    name: 'test_tag',
    display_name: 'Test Tag',
    description: 'A test tag',
    type: 'tag',
    tag_type: 'business',
    kind: 'tag',
  },
];

const makeClient = overrides => ({
  DataJunctionAPI: {
    globalSearch: jest
      .fn()
      .mockResolvedValue({ nodes: mockNodes, tags: mockTags }),
    ...(overrides || {}),
  },
});

const flushDebounce = async () => {
  await act(async () => {
    jest.advanceTimersByTime(300);
  });
};

describe('<Search />', () => {
  beforeEach(() => {
    jest.useFakeTimers();
    jest.clearAllMocks();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('renders the search input with the idle placeholder', () => {
    const client = makeClient();
    const { getByPlaceholderText } = render(
      <DJClientContext.Provider value={client}>
        <Search />
      </DJClientContext.Provider>,
    );
    expect(
      getByPlaceholderText('Search nodes and tags...'),
    ).toBeInTheDocument();
  });

  it('does not query the API for queries shorter than 2 characters', async () => {
    const client = makeClient();
    const { getByPlaceholderText } = render(
      <DJClientContext.Provider value={client}>
        <Search />
      </DJClientContext.Provider>,
    );
    fireEvent.change(getByPlaceholderText('Search nodes and tags...'), {
      target: { value: 'a' },
    });
    await flushDebounce();
    expect(client.DataJunctionAPI.globalSearch).not.toHaveBeenCalled();
  });

  it('calls globalSearch after the debounce interval for a non-trivial query', async () => {
    const client = makeClient();
    const { getByPlaceholderText } = render(
      <DJClientContext.Provider value={client}>
        <Search />
      </DJClientContext.Provider>,
    );
    fireEvent.change(getByPlaceholderText('Search nodes and tags...'), {
      target: { value: 'test' },
    });
    await flushDebounce();
    expect(client.DataJunctionAPI.globalSearch).toHaveBeenCalledWith(
      'test',
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
  });

  it('renders both node and tag results returned by the server', async () => {
    const client = makeClient();
    const { getByPlaceholderText, container, findByText } = render(
      <DJClientContext.Provider value={client}>
        <Search />
      </DJClientContext.Provider>,
    );
    fireEvent.change(getByPlaceholderText('Search nodes and tags...'), {
      target: { value: 'test' },
    });
    await flushDebounce();
    await findByText(/Test Node/);
    expect(
      container.querySelector('a[href="/nodes/default.test_node"]'),
    ).toBeInTheDocument();
    expect(
      container.querySelector('a[href="/tags/test_tag"]'),
    ).toBeInTheDocument();
  });

  it('truncates descriptions longer than 100 characters', async () => {
    const client = makeClient({
      globalSearch: jest
        .fn()
        .mockResolvedValue({ nodes: [mockNodes[2]], tags: [] }),
    });
    const { getByPlaceholderText, findByText } = render(
      <DJClientContext.Provider value={client}>
        <Search />
      </DJClientContext.Provider>,
    );
    fireEvent.change(getByPlaceholderText('Search nodes and tags...'), {
      target: { value: 'long' },
    });
    await flushDebounce();
    await findByText(/\.\.\./);
  });

  it('clears results when the query drops below the minimum length', async () => {
    const client = makeClient();
    const { getByPlaceholderText, container, findByText } = render(
      <DJClientContext.Provider value={client}>
        <Search />
      </DJClientContext.Provider>,
    );
    const input = getByPlaceholderText('Search nodes and tags...');
    fireEvent.change(input, { target: { value: 'test' } });
    await flushDebounce();
    await findByText(/Test Node/);
    fireEvent.change(input, { target: { value: 'x' } });
    await waitFor(() => {
      expect(container.querySelector('.search-result-item')).toBeNull();
    });
  });

  it('logs an error but does not throw when the request fails', async () => {
    const consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation();
    const client = makeClient({
      globalSearch: jest.fn().mockRejectedValue(new Error('boom')),
    });
    const { getByPlaceholderText } = render(
      <DJClientContext.Provider value={client}>
        <Search />
      </DJClientContext.Provider>,
    );
    fireEvent.change(getByPlaceholderText('Search nodes and tags...'), {
      target: { value: 'test' },
    });
    await flushDebounce();
    await waitFor(() => {
      expect(consoleErrorSpy).toHaveBeenCalledWith(
        'Search failed:',
        expect.any(Error),
      );
    });
    consoleErrorSpy.mockRestore();
  });

  it('aborts the in-flight request when a new query is typed', async () => {
    const aborts = [];
    const client = makeClient({
      globalSearch: jest.fn((q, { signal }) => {
        return new Promise((resolve, reject) => {
          signal.addEventListener('abort', () => {
            const err = new Error('aborted');
            err.name = 'AbortError';
            aborts.push(q);
            reject(err);
          });
        });
      }),
    });
    const { getByPlaceholderText } = render(
      <DJClientContext.Provider value={client}>
        <Search />
      </DJClientContext.Provider>,
    );
    const input = getByPlaceholderText('Search nodes and tags...');
    fireEvent.change(input, { target: { value: 'aaa' } });
    await flushDebounce();
    fireEvent.change(input, { target: { value: 'bbb' } });
    await flushDebounce();
    await waitFor(() => expect(aborts).toContain('aaa'));
    expect(client.DataJunctionAPI.globalSearch).toHaveBeenCalledTimes(2);
  });
});
