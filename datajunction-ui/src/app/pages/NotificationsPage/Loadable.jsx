import { lazyLoad } from '../../../utils/loadable';

export const NotificationsPage = lazyLoad(
  () => import('./index'),
  module => module.NotificationsPage,
);
