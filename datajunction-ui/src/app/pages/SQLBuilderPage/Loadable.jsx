/**
 * Asynchronously loads the component for the Node page
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const SQLBuilderPage = props => {
  return lazyLoad(
    () => import('./index'),
    module => module.SQLBuilderPage,
    {
      fallback: <div></div>,
    },
  )(props);
};
