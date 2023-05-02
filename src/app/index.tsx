/**
 *
 * App
 *
 * This component is the skeleton around the actual pages, and should only
 * contain code that should be seen on all pages. (e.g. navigation bar)
 */

import * as React from 'react';
import { Helmet } from 'react-helmet-async';
import { BrowserRouter, Routes, Route } from 'react-router-dom';

import { GlobalStyle } from 'styles/global-styles';
import { DAGStyle } from '../styles/dag-styles';

import { NotFoundPage } from './pages/NotFoundPage/Loadable';
import { useTranslation } from 'react-i18next';

import { Root } from './pages/Root/Loadable';
import { NodePage } from './pages/NodePage/Loadable';
import { NamespacePage } from './pages/NamespacePage/Loadable';

export function App() {
  const { i18n } = useTranslation();
  return (
    <BrowserRouter>
      <Helmet
        titleTemplate="DataJunction: %s"
        defaultTitle="DataJunction: A Metrics Platform"
        htmlAttributes={{ lang: i18n.language }}
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
            </>
            // <Route path="*" element={<NotFoundPage />} />
          }
        />
        {/*<Route path="namespaces/:namespace" element={<NamespaceInfo />} />*/}
        <Route path="*" element={<NotFoundPage />} />
      </Routes>
      <GlobalStyle />
      <DAGStyle />
    </BrowserRouter>
  );
}
