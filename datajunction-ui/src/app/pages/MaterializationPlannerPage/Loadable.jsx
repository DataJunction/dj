import { lazyLoad } from 'utils/loadable';

export const MaterializationPlannerPage = lazyLoad(
  () => import('./index'),
  module => module.MaterializationPlannerPage,
);

