import { lazyLoad } from 'utils/loadable';

export const MyWorkspacePage = lazyLoad(
  () => import('./index'),
  module => module.MyWorkspacePage,
);
