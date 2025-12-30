import { lazyLoad } from 'utils/loadable';

export const QueryPlannerPage = lazyLoad(
  () => import('./index'),
  module => module.QueryPlannerPage,
);
