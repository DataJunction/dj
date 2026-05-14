/**
 * Asynchronously loads the component for the System Metrics Explorer page
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const SystemMetricsExplorerPage = props => {
  return lazyLoad(
    () => import('./index'),
    module => module.SystemMetricsExplorerPage,
    {
      fallback: <div></div>,
    },
  )(props);
};
