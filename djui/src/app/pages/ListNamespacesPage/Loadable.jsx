/**
 * Asynchronously loads the component for namespaces node-viewing page
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const ListNamespacesPage = lazyLoad(
  () => import('./index'),
  module => module.ListNamespacesPage,
  {
    fallback: <div></div>,
  },
);
