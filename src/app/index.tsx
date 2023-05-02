/**
 * This component is the skeleton around the actual pages, and only contains
 * components that should be seen on all pages, like the logo or navigation bar.
 */

import * as React from 'react';
import { Helmet } from 'react-helmet-async';
import { BrowserRouter, Routes, Route } from 'react-router-dom';

import { GlobalStyle } from 'styles/global-styles';
import { DAGStyle } from '../styles/dag-styles';

import { NamespacePage } from './pages/NamespacePage/Loadable';
import { NodePage } from './pages/NodePage/Loadable';
import { NotFoundPage } from './pages/NotFoundPage/Loadable';
import { Root } from './pages/Root/Loadable';
import { ListNamespacesPage } from './pages/ListNamespacesPage';

export function App() {
  return (
    <BrowserRouter>
      <Helmet
        titleTemplate="DataJunction: %s"
        defaultTitle="DataJunction: A Metrics Platform"
      >
        <meta
          name="description"
          content="DataJunction serves as a semantic layer to help manage metrics"
        />
      </Helmet>

      <Routes>
        <Route
          path="/"
          element={<Root />}
          children={
            <>
              <Route path="/nodes/:name" element={<NodePage />} />
              <Route
                path="/namespaces/:namespace"
                element={<NamespacePage />}
              />
              <Route path="/namespaces/" element={<ListNamespacesPage />} />
            </>
          }
        />
        <Route path="*" element={<NotFoundPage />} />
      </Routes>
      <GlobalStyle />
      <DAGStyle />
    </BrowserRouter>
  );
}
