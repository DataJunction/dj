/**
 * Asynchronously loads the component for the Overview page
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const OverviewPage = props => {
  return lazyLoad(
    () => import('./index'),
    module => module.OverviewPage,
    {
      fallback: <div></div>,
    },
  )(props);
};
