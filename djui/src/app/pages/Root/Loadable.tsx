/**
 * Asynchronously loads the component for the root page
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const Root = lazyLoad(
  () => import('./index'),
  module => module.Root,
  {
    fallback: <></>,
  },
);
