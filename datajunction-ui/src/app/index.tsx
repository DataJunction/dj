/**
 * This component is the skeleton around the actual pages, and only contains
 * components that should be seen on all pages, like the logo or navigation bar.
 */

import * as React from 'react';
import { Helmet } from 'react-helmet-async';
import { BrowserRouter, Routes, Route } from 'react-router-dom';

import { NamespacePage } from './pages/NamespacePage/Loadable';
import { NodePage } from './pages/NodePage/Loadable';
import { SQLBuilderPage } from './pages/SQLBuilderPage/Loadable';
import { NotFoundPage } from './pages/NotFoundPage/Loadable';
import { Root } from './pages/Root/Loadable';
import { ListNamespacesPage } from './pages/ListNamespacesPage';
import DJClientContext from './providers/djclient';
import { DataJunctionAPI } from './services/DJService';

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
      <DJClientContext.Provider value={{ DataJunctionAPI }}>
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
                <Route path="sql" key="sql" element={<SQLBuilderPage />} />
              </>
            }
          />
          <Route path="*" element={<NotFoundPage />} />
        </Routes>
      </DJClientContext.Provider>
    </BrowserRouter>
  );
}
