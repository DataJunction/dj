import React from 'react';
import { render, fireEvent, screen, waitFor } from '@testing-library/react';
import Collapse from '../Collapse';
import { mocks } from '../../../../mocks/mockNodes';
import { ReactFlowProvider } from 'reactflow';

jest.mock('../DJNodeDimensions', () => ({
  DJNodeDimensions: jest.fn(data => <div>DJNodeDimensions content</div>),
}));

describe('<Collapse />', () => {
  const defaultProps = {
    collapsed: true,
    text: 'Dimensions',
    data: mocks.mockMetricNode,
  };

  it('renders without crashing', () => {
    render(<Collapse {...defaultProps} />);
  });

  it('renders toggle for metric type', () => {
    const { getByText } = render(
      <Collapse {...defaultProps} data={mocks.mockMetricNode} />,
    );
    const button = screen.getByText('▶ Show Dimensions');
    fireEvent.click(button);
    expect(getByText('▼ Hide Dimensions')).toBeInTheDocument();
  });

  it('toggles More/Less button text for non-metric type on click', () => {
    defaultProps.text = 'Columns';
    const { getByText } = render(
      <ReactFlowProvider>
        <Collapse
          {...defaultProps}
          data={{
            name: 'test.transform',
            type: 'transform',
            column_names: Array.from({ length: 11 }, (_, idx) => ({
              name: `column_${idx}`,
              type: 'string',
              order: idx,
            })),
            primary_key: [],
          }}
        />
      </ReactFlowProvider>,
    );
    const button = getByText('▶ More Columns');
    fireEvent.click(button);
    expect(getByText('▼ Less Columns')).toBeInTheDocument();
  });
});
