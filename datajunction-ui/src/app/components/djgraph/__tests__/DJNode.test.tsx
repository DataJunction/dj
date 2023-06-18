import * as React from 'react';
import { createRenderer } from 'react-test-renderer/shallow';

import { DJNode } from '../DJNode';

const renderer = createRenderer();

describe('<DJNode />', () => {
  it('should render and match the snapshot', () => {
    renderer.render(
      <DJNode
        id="1"
        data={{
          name: 'shared.dimensions.accounts',
          column_names: ['a'],
          type: 'source',
          primary_key: ['id'],
        }}
      />,
    );
    const renderedOutput = renderer.getRenderOutput();
    expect(renderedOutput).toMatchSnapshot();
  });
});
