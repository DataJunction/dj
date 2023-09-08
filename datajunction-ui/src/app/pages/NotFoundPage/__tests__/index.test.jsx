import React from 'react';
import { render } from '@testing-library/react';
import { NotFoundPage } from '../index';
import { HelmetProvider } from 'react-helmet-async';

describe('<NotFoundPage />', () => {
  it('displays the correct 404 message ', () => {
    const { getByText } = render(
      <HelmetProvider>
        <NotFoundPage />
      </HelmetProvider>,
    );

    expect(getByText('Page not found.')).toBeInTheDocument();
  });
});
