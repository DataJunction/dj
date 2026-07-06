/**
 * Asynchronously loads RevisionDiff.
 *
 * RevisionDiff pulls in react-syntax-highlighter (and its language/theme
 * modules). Loading it lazily keeps that large dependency out of the landing
 * bundle so it's only fetched when a revision diff is actually viewed.
 */

import * as React from 'react';
import { lazyLoad } from '../../../utils/loadable';

export const RevisionDiff = props => {
  return lazyLoad(
    () => import('./RevisionDiff'),
    module => module.default,
    {
      fallback: <div></div>,
    },
  )(props);
};
