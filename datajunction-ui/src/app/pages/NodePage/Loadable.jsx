/**
 * Asynchronously loads the component for the Node page
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const NodePage = props => {
  return lazyLoad(
    () => import('./index'),
    module => module.NodePage,
    {
      fallback: <div></div>,
    },
  )(props);
};

export const NodeRevisionPage = props => {
  return lazyLoad(
    () => import('./NodeRevision'),
    module => module.NodeRevision,
    {
      fallback: <div></div>,
    },
  )(props);
};
