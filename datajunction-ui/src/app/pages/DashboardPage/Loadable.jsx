/**
 * Asynchronously loads the component for namespaces node-viewing page
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const DashboardPage = props => {
  return lazyLoad(
    () => import('./index'),
    module => module.DashboardPage,
    {
      fallback: <div></div>,
    },
  )(props);
};
