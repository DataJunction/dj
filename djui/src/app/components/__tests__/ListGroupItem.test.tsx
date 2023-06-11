import * as React from 'react';
import { createRenderer } from 'react-test-renderer/shallow';

import ListGroupItem from '../ListGroupItem';

const renderer = createRenderer();

describe('<ListGroupItem />', () => {
  it('should render and match the snapshot', () => {
    renderer.render(
      <ListGroupItem label="Name" value={<span>Something</span>} />,
    );
    const renderedOutput = renderer.getRenderOutput();
    expect(renderedOutput).toMatchSnapshot();
  });
});
