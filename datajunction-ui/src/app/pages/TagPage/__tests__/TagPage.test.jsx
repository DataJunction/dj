import React from 'react';
import { screen, waitFor } from '@testing-library/react';
import fetchMock from 'jest-fetch-mock';
import { render } from '../../../../setupTests';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import DJClientContext from '../../../providers/djclient';
import { TagPage } from '../index';

describe('<TagPage />', () => {
  const initializeMockDJClient = () => {
    return {
      DataJunctionAPI: {
        getTag: jest.fn(),
        listNodesForTag: jest.fn(),
      },
    };
  };

  const mockDjClient = initializeMockDJClient();

  beforeEach(() => {
    fetchMock.resetMocks();
    jest.clearAllMocks();
    window.scrollTo = jest.fn();

    mockDjClient.DataJunctionAPI.getTag.mockReturnValue({
      name: 'domains.com',
      tag_type: 'domains',
      description: 'Top-level domain .com',
    });
    mockDjClient.DataJunctionAPI.listNodesForTag.mockReturnValue([
      {
        name: 'random.node_a',
        type: 'metric',
        display_name: 'Node A',
      },
    ]);
  });

  const renderTagsPage = element => {
    return render(
      <MemoryRouter initialEntries={['/tags/:name']}>
        <Routes>
          <Route path="tags/:name" element={element} />
        </Routes>
      </MemoryRouter>,
    );
  };

  const testElement = djClient => {
    return (
      <DJClientContext.Provider value={djClient}>
        <TagPage />
      </DJClientContext.Provider>
    );
  };

  it('renders the tag page correctly', async () => {
    const element = testElement(mockDjClient);
    renderTagsPage(element);
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.getTag).toHaveBeenCalledTimes(1);
      expect(
        mockDjClient.DataJunctionAPI.listNodesForTag,
      ).toHaveBeenCalledTimes(1);
    });
    expect(screen.getByText('Nodes')).toBeInTheDocument();
    expect(screen.getByText('Node A')).toBeInTheDocument();
  }, 60000);
});
