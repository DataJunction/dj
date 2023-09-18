/**
 * Asynchronously loads the component for the Node page
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const RegisterTablePage = () => {
  return lazyLoad(
    () => import('./index'),
    module => module.RegisterTablePage,
    {
      fallback: <div></div>,
    },
  )();
};
