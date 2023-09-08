import React from 'react';
import { render, fireEvent, screen } from '@testing-library/react';
import Collapse from '../Collapse';

jest.mock('../DJNodeDimensions', () => ({
  DJNodeDimensions: jest.fn(data => <div>DJNodeDimensions content</div>),
}));

// jest.mock('../DJNodeColumns', () => {
//   DJNodeColumns: jest.fn(({ data, limit }) => (
//     <div>DJNodeColumns content - limit {limit}</div>
//   ));
// });

describe('<Collapse />', () => {
  const defaultProps = {
    collapsed: true,
    text: 'Test Text',
    data: {
      type: 'metric',
    },
  };

  it('renders without crashing', () => {
    render(<Collapse {...defaultProps} />);
  });

  it('renders metric type collapse button correctly', () => {
    const { getByText } = render(
      <Collapse {...defaultProps} data={{ type: 'metric' }} />,
    );
    expect(getByText('▶ Show Test Text')).toBeInTheDocument();
  });

  it('toggles metric type collapse button text on click', () => {
    const { container } = render(
      <Collapse {...defaultProps} data={{ type: 'metric' }} />,
    );
    const button = screen.getByText('▶ Show Test Text');
    fireEvent.click(button);
    expect(screen.getByText('▼ Hide Test Text')).toBeInTheDocument();
  });

  // it('renders DJNodeDimensions content for metric type', () => {
  //   const { getByText } = render(
  //     <Collapse {...defaultProps} data={{ type: 'metric' }} />,
  //   );
  //   const button = screen.getByText('▶ Show Test Text');
  //   fireEvent.click(button);
  //   expect(getByText('DJNodeDimensions content')).toBeInTheDocument();
  // });
  //
  // it('renders DJNodeColumns with correct limit for non-metric type', () => {
  //   const { getByText } = render(
  //     <Collapse {...defaultProps} data={{ type: 'non-metric' }} />,
  //   );
  //   expect(getByText('DJNodeColumns content - limit 10')).toBeInTheDocument();
  // });
  //
  // it('renders More/Less button for non-metric type with column names length > 10', () => {
  //   const { getByText } = render(
  //     <Collapse
  //       {...defaultProps}
  //       data={{ type: 'non-metric', column_names: Array(11).fill('column') }}
  //     />,
  //   );
  //   expect(getByText('▶ More Test Text')).toBeInTheDocument();
  // });
  //
  // it('toggles More/Less button text for non-metric type on click', () => {
  //   const { getByText } = render(
  //     <Collapse
  //       {...defaultProps}
  //       data={{ type: 'non-metric', column_names: Array(11).fill('column') }}
  //     />,
  //   );
  //   const button = getByText('▶ More Test Text');
  //   fireEvent.click(button);
  //   expect(getByText('▼ Less Test Text')).toBeInTheDocument();
  // });
});
