/**
 * Asynchronously loads the component for the Settings page
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const SettingsPage = props => {
  return lazyLoad(
    () => import('./index'),
    module => module.SettingsPage,
    {
      fallback: <div></div>,
    },
  )(props);
};
