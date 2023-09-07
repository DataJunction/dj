// react-testing-library renders your components to document.body,
// this adds jest-dom's custom assertions
import '@testing-library/jest-dom';

import 'react-app-polyfill/ie11';
import 'react-app-polyfill/stable';
import { JSDOM } from 'jsdom';
import { render as originalRender } from '@testing-library/react';

const setDom = () => {
  const dom = new JSDOM('<!doctype html><html><body></body></html>', {});

  global.window = dom.window;
  global.document = dom.window.document;
  document.createRange = () => {
    const range = new Range();
    range.getBoundingClientRect = jest.fn();
    range.getClientRects = () => {
      return {
        item: () => null,
        length: 0,
        [Symbol.iterator]: jest.fn(),
      };
    };

    return range;
  };
};

export const render = ui => {
  setDom();
  return originalRender(ui);
};
