import * as React from 'react';
import { createRenderer } from 'react-test-renderer/shallow';

import NamespaceHeader from '../NamespaceHeader';

const renderer = createRenderer();

describe('<NamespaceHeader />', () => {
  it('should render and match the snapshot', () => {
    renderer.render(<NamespaceHeader namespace="shared.dimensions.accounts" />);
    const renderedOutput = renderer.getRenderOutput();
    expect(renderedOutput).toMatchSnapshot();
  });
});
