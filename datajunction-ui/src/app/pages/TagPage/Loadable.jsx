/**
 * Asynchronously loads the component for the Node page
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const TagPage = () => {
  return lazyLoad(
    () => import('./index'),
    module => module.TagPage,
    {
      fallback: <div></div>,
    },
  )();
};
