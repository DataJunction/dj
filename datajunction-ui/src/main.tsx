/**
 * main.tsx
 *
 * App entry point. Mounts <App /> to #root.
 * (The library entry is index.tsx — it only re-exports the public surface.)
 */

import * as React from 'react';
import ReactDOM from 'react-dom/client';
import FontFaceObserver from 'fontfaceobserver';

import 'sanitize.css/sanitize.css';
import './styles/index.css';
import 'react-diff-view/style/index.css';

import { App } from './app';

import { HelmetProvider } from 'react-helmet-async';
import reportWebVitals from './reportWebVitals';

const interFontObserver = new FontFaceObserver('Inter', {});
interFontObserver.load().then(() => {
  document.body.classList.add('fontLoaded');
});

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement,
);

root.render(
  <HelmetProvider>
    <React.StrictMode>
      <App />
    </React.StrictMode>
  </HelmetProvider>,
);

reportWebVitals();
