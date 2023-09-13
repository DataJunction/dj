import * as React from 'react';
import { render, screen } from '@testing-library/react';
import QueryInfo from '../QueryInfo';

describe('<QueryInfo />', () => {
  const defaultProps = {
    id: '123',
    state: 'Running',
    engine_name: 'Spark SQL',
    engine_version: '1.0',
    errors: ['Error 1', 'Error 2'],
    links: ['http://example.com', 'http://example2.com'],
    output_table: 'table1',
    scheduled: '2023-09-06',
    started: '2023-09-07',
    numRows: 1000,
  };

  it('renders without crashing', () => {
    render(<QueryInfo {...defaultProps} />);
  });

  it('displays correct query information', () => {
    render(<QueryInfo {...defaultProps} />);

    expect(screen.getByText(defaultProps.id)).toBeInTheDocument();
    expect(
      screen.getByText(
        `${defaultProps.engine_name} - ${defaultProps.engine_version}`,
      ),
    ).toBeInTheDocument();
    expect(screen.getByText(defaultProps.state)).toBeInTheDocument();
    expect(screen.getByText(defaultProps.scheduled)).toBeInTheDocument();
    expect(screen.getByText(defaultProps.started)).toBeInTheDocument();
    expect(screen.getByText(defaultProps.output_table)).toBeInTheDocument();
    expect(screen.getByText(String(defaultProps.numRows))).toBeInTheDocument();
    defaultProps.errors.forEach(error => {
      expect(screen.getByText(error)).toBeInTheDocument();
    });
    defaultProps.links.forEach(link => {
      expect(screen.getByText(link)).toHaveAttribute('href', link);
    });
  });

  it('does not render errors and links when they are not provided', () => {
    render(<QueryInfo {...defaultProps} errors={[]} links={[]} />);

    defaultProps.errors.forEach(error => {
      expect(screen.queryByText(error)).not.toBeInTheDocument();
    });
    defaultProps.links.forEach(link => {
      expect(screen.queryByText(link)).not.toBeInTheDocument();
    });
  });
});
