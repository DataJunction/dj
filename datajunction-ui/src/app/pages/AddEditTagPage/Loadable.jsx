/**
 * Asynchronously loads the component for the Node page
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const AddEditTagPage = () => {
  return lazyLoad(
    () => import('./index'),
    module => module.AddEditTagPage,
    {
      fallback: <div></div>,
    },
  )();
};
