import * as React from 'react';
import { NotFoundPage } from '..';
import { MemoryRouter } from 'react-router-dom';
import { HelmetProvider } from 'react-helmet-async';
import renderer from 'react-test-renderer';

const renderPage = () =>
  renderer.create(
    <MemoryRouter>
      <HelmetProvider>
        <NotFoundPage />
      </HelmetProvider>
    </MemoryRouter>,
  );

describe('<NotFoundPage />', () => {
  it('should match snapshot', () => {
    const notFoundPage = renderPage();
    expect(notFoundPage.toJSON()).toMatchSnapshot();
  });
});
