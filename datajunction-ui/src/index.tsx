/**
 * index.tsx
 *
 * Library entry. Re-exports the public surface so consumers can do
 * `import { App, ... } from 'datajunction-ui'`.
 *
 * Note: the existing internal consumer imports from deep paths
 * (`datajunction-ui/src/app/...`). Those still work because `src/` is
 * shipped as part of the package.
 */

export { App } from './app';
export { Root } from './app/pages/Root';
export { default as DJClientContext } from './app/providers/djclient';
export { UserProvider } from './app/providers/UserProvider';
export { DataJunctionAPI } from './app/services/DJService';

export { NamespacePage } from './app/pages/NamespacePage/Loadable';
export { MyWorkspacePage } from './app/pages/MyWorkspacePage/Loadable';
export { OverviewPage } from './app/pages/OverviewPage/Loadable';
export { SystemMetricsExplorerPage } from './app/pages/SystemMetricsExplorerPage/Loadable';
export { SettingsPage } from './app/pages/SettingsPage/Loadable';
export { NotificationsPage } from './app/pages/NotificationsPage/Loadable';
export { NodePage } from './app/pages/NodePage';
export { SQLBuilderPage } from './app/pages/SQLBuilderPage/Loadable';
export { CubeBuilderPage } from './app/pages/CubeBuilderPage/Loadable';
export { QueryPlannerPage } from './app/pages/QueryPlannerPage/Loadable';
export { TagPage } from './app/pages/TagPage/Loadable';
export { AddEditNodePage } from './app/pages/AddEditNodePage/Loadable';
export { AddEditTagPage } from './app/pages/AddEditTagPage/Loadable';
export { NotFoundPage } from './app/pages/NotFoundPage/Loadable';
export { RegisterTablePage } from './app/pages/RegisterTablePage';
