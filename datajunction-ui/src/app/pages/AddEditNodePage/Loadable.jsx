/**
 * Asynchronously loads the component for the Node page
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const AddEditNodePage = () => {
  return lazyLoad(
    () => import('./index'),
    module => module.AddEditNodePage,
    {
      fallback: <div></div>,
    },
  )();
};
