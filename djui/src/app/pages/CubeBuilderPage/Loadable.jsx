/**
 * Asynchronously loads the component for the Node page
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const CubeBuilderPage = props => {
  return lazyLoad(
    () => import('./index'),
    module => module.CubeBuilderPage,
    {
      fallback: <div></div>,
    },
  )(props);
};
