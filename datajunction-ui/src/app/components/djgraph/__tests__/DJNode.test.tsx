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

  it('should render with is_current true and non-metric type (collapsed=false)', () => {
    renderer.render(
      <DJNode
        id="2"
        data={{
          name: 'shared.dimensions.accounts',
          column_names: ['a', 'b'],
          type: 'dimension',
          primary_key: ['id'],
          is_current: true,
          display_name: 'Accounts',
        }}
      />,
    );
    const renderedOutput = renderer.getRenderOutput();
    expect(renderedOutput).toBeTruthy();
  });

  it('should render with metric type (collapsed=true)', () => {
    renderer.render(
      <DJNode
        id="3"
        data={{
          name: 'default.revenue',
          column_names: [],
          type: 'metric',
          primary_key: [],
          is_current: true,
          display_name: 'Revenue',
        }}
      />,
    );
    const renderedOutput = renderer.getRenderOutput();
    expect(renderedOutput).toBeTruthy();
  });
});
