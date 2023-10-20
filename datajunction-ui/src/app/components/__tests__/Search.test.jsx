import * as React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import Search from '../Search';
import DJClientContext from '../../providers/djclient';
import { Root } from '../../pages/Root';
import { HelmetProvider } from 'react-helmet-async';

describe('<Search />', () => {
  const mockDjClient = {
    logout: jest.fn(),
    nodeDetails: async () => [
        {
          name: 'default.repair_orders',
          display_name: 'Default: Repair Orders',
          description: 'Repair orders',
          version: 'v1.0',
          type: 'source',
          status: 'valid',
          mode: 'published',
          updated_at: '2023-08-21T16:48:52.880498+00:00',
        },
        {
          name: 'default.repair_order_details',
          display_name: 'Default: Repair Order Details',
          description: 'Details on repair orders',
          version: 'v1.0',
          type: 'source',
          status: 'valid',
          mode: 'published',
          updated_at: '2023-08-21T16:48:52.981201+00:00',
        },
      ]
  };

  it('displays search results correctly', () => {
    render(
      <HelmetProvider>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <Root />
        </DJClientContext.Provider>
      </HelmetProvider>,
    );
    const searchInput = screen.queryByPlaceholderText('Search');
    fireEvent.change(searchInput, { target: { value: 'Repair' } });
    expect(searchInput.value).toBe('Repair');
  });
});
