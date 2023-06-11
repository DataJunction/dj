import * as React from 'react';
import { createRenderer } from 'react-test-renderer/shallow';

import { NamespacePage } from '../index';

const renderer = createRenderer();

describe('<NamespacePage />', () => {
  it('should render and match the snapshot', () => {
    renderer.render(<NamespacePage />);
    const renderedOutput = renderer.getRenderOutput();
    expect(renderedOutput).toMatchSnapshot();
  });
});
