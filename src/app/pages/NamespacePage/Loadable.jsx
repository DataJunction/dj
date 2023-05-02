/**
 * Asynchronously loads the component for namespaces node-viewing page
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const NamespacePage = lazyLoad(
  () => import('./index'),
  module => module.NamespacePage,
  {
    fallback: <div></div>,
  },
);
