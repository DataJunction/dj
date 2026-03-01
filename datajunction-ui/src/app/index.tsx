/**
 * This component is the skeleton around the actual pages, and only contains
 * components that should be seen on all pages, like the logo or navigation bar.
 */

import * as React from 'react';
import { Helmet } from 'react-helmet-async';
import { BrowserRouter, Routes, Route } from 'react-router-dom';

import { NamespacePage } from './pages/NamespacePage/Loadable';
import { MyWorkspacePage } from './pages/MyWorkspacePage/Loadable';
import { OverviewPage } from './pages/OverviewPage/Loadable';
import { SettingsPage } from './pages/SettingsPage/Loadable';
import { NotificationsPage } from './pages/NotificationsPage/Loadable';
import { NodePage } from './pages/NodePage/Loadable';
import RevisionDiff from './pages/NodePage/RevisionDiff';
import { SQLBuilderPage } from './pages/SQLBuilderPage/Loadable';
import { CubeBuilderPage } from './pages/CubeBuilderPage/Loadable';
import { QueryPlannerPage } from './pages/QueryPlannerPage/Loadable';
import { TagPage } from './pages/TagPage/Loadable';
import { AddEditNodePage } from './pages/AddEditNodePage/Loadable';
import { AddEditTagPage } from './pages/AddEditTagPage/Loadable';
import { NotFoundPage } from './pages/NotFoundPage/Loadable';
import { LoginPage } from './pages/LoginPage';
import { RegisterTablePage } from './pages/RegisterTablePage';
import { Root } from './pages/Root';
import DJClientContext from './providers/djclient';
import { UserProvider } from './providers/UserProvider';
import { DataJunctionAPI } from './services/DJService';
import { CookiesProvider, useCookies } from 'react-cookie';
import * as Constants from './constants';

export function App() {
  const [cookies] = useCookies([Constants.LOGGED_IN_FLAG_COOKIE]);
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
              <UserProvider>
                <Routes>
                  <Route
                    path="/"
                    element={<Root />}
                    children={
                      <>
                        <Route path="nodes" key="nodes">
                          <Route path=":name" element={<NodePage />} />
                          <Route
                            path=":name/edit"
                            key="edit"
                            element={<AddEditNodePage />}
                          />
                          <Route
                            path=":name/edit-cube"
                            key="edit-cube"
                            element={<CubeBuilderPage />}
                          />
                          <Route
                            path=":name/revisions/:revision"
                            element={<RevisionDiff />}
                          />
                          <Route path=":name/:tab" element={<NodePage />} />
                        </Route>

                        <Route
                          path="/"
                          element={<NamespacePage />}
                          key="index"
                        />
                        <Route path="namespaces">
                          <Route
                            path=":namespace"
                            element={<NamespacePage />}
                            key="namespaces"
                          />
                        </Route>
                        <Route
                          path="create/tag"
                          key="createtag"
                          element={<AddEditTagPage />}
                        ></Route>
                        <Route
                          path="create/source"
                          key="register"
                          element={<RegisterTablePage />}
                        ></Route>
                        <Route path="/create/cube">
                          <Route
                            path=":initialNamespace"
                            key="create"
                            element={<CubeBuilderPage />}
                          />
                          <Route
                            path=""
                            key="create"
                            element={<CubeBuilderPage />}
                          />
                        </Route>
                        <Route path="create/:nodeType">
                          <Route
                            path=":initialNamespace"
                            key="create"
                            element={<AddEditNodePage />}
                          />
                          <Route
                            path=""
                            key="create"
                            element={<AddEditNodePage />}
                          />
                        </Route>
                        <Route
                          path="sql"
                          key="sql"
                          element={<SQLBuilderPage />}
                        />
                        <Route
                          path="planner"
                          key="planner"
                          element={<QueryPlannerPage />}
                        />
                        <Route path="tags" key="tags">
                          <Route path=":name" element={<TagPage />} />
                        </Route>
                        <Route
                          path="overview"
                          key="overview"
                          element={<OverviewPage />}
                        />
                        <Route
                          path="workspace"
                          key="workspace"
                          element={<MyWorkspacePage />}
                        />
                        <Route
                          path="settings"
                          key="settings"
                          element={<SettingsPage />}
                        />
                        <Route
                          path="notifications"
                          key="notifications"
                          element={<NotificationsPage />}
                        />
                      </>
                    }
                  />
                  <Route path="*" element={<NotFoundPage />} />
                </Routes>
              </UserProvider>
            </DJClientContext.Provider>
          </>
        ) : (
          <LoginPage />
        )}
      </BrowserRouter>
    </CookiesProvider>
  );
}
