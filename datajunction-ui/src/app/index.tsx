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
import { LoginPage } from './pages/LoginPage';
import { Root } from './pages/Root/Loadable';
import DJClientContext from './providers/djclient';
import { DataJunctionAPI } from './services/DJService';
import { CookiesProvider, useCookies } from 'react-cookie';
import * as Constants from './constants';

export function App() {
  const [cookies] = useCookies([Constants.DJ_LOGGED_IN_FLAG_COOKIE]);
  return (
    <CookiesProvider>
      <BrowserRouter>
        {cookies.__djlif || process.env.REACT_DISABLE_AUTH === 'true' ? (
          <>
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

                      <Route path="/" element={<NamespacePage />} key="index" />
                      <Route path="namespaces">
                        <Route
                          path=":namespace"
                          element={<NamespacePage />}
                          key="namespaces"
                        />
                      </Route>
                      <Route
                        path="sql"
                        key="sql"
                        element={<SQLBuilderPage />}
                      />
                    </>
                  }
                />
                <Route path="*" element={<NotFoundPage />} />
              </Routes>
            </DJClientContext.Provider>
          </>
        ) : (
          <LoginPage />
        )}
      </BrowserRouter>
    </CookiesProvider>
  );
}
