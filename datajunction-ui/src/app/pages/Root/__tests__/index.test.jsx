import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { Root } from '../index';
import DJClientContext from '../../../providers/djclient';
import { HelmetProvider } from 'react-helmet-async';

describe('<Root />', () => {
  const mockDjClient = {
    logout: jest.fn(),
  };

  it('renders with the correct title and navigation', async () => {
    render(
      <HelmetProvider>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <Root />
        </DJClientContext.Provider>
      </HelmetProvider>,
    );

    waitFor(() => {
      expect(document.title).toEqual('DataJunction');
      const metaDescription = document.querySelector(
        "meta[name='description']",
      );
      expect(metaDescription).toBeInTheDocument();
      expect(metaDescription.content).toBe(
        'DataJunction Metrics Platform Webapp',
      );

      expect(screen.getByText(/^DataJunction$/)).toBeInTheDocument();
      expect(screen.getByText('Explore').closest('a')).toHaveAttribute(
        'href',
        '/',
      );
      expect(screen.getByText('SQL').closest('a')).toHaveAttribute(
        'href',
        '/sql',
      );
      expect(screen.getByText('Docs').closest('a')).toHaveAttribute(
        'href',
        'https://www.datajunction.io',
      );
    });
  });

  it('renders Logout button unless REACT_DISABLE_AUTH is true', () => {
    process.env.REACT_DISABLE_AUTH = 'false';
    render(
      <HelmetProvider>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <Root />
        </DJClientContext.Provider>
      </HelmetProvider>,
    );
    expect(screen.getByText('Logout')).toBeInTheDocument();
  });

  it('calls logout and reloads window on logout button click', () => {
    process.env.REACT_DISABLE_AUTH = 'false';
    const originalLocation = window.location;
    delete window.location;
    window.location = { ...originalLocation, reload: jest.fn() };

    render(
      <HelmetProvider>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <Root />
        </DJClientContext.Provider>
      </HelmetProvider>,
    );

    screen.getByText('Logout').click();
    expect(mockDjClient.logout).toHaveBeenCalled();
    window.location = originalLocation;
  });
});
