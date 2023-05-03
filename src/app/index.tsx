/**
 * This component is the skeleton around the actual pages, and only contains
 * components that should be seen on all pages, like the logo or navigation bar.
 */

import * as React from 'react';
import { Helmet } from 'react-helmet-async';
import { BrowserRouter, Routes, Route } from 'react-router-dom';

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
              <Route path="nodes" key="nodes">
                <Route path=":name" element={<NodePage />} />
              </Route>

              <Route path="/" element={<ListNamespacesPage />} key="index" />
              <Route path="namespaces">
                <Route
                  path=":namespace"
                  element={<NamespacePage />}
                  key="namespaces"
                />
              </Route>
            </>
          }
        />
        <Route path="*" element={<NotFoundPage />} />
      </Routes>
    </BrowserRouter>
  );
}
