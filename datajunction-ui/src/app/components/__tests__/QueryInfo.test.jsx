import React from 'react';
import { render } from '@testing-library/react';
import QueryInfo from '../QueryInfo';

describe('<QueryInfo />', () => {
  const defaultProps = {
    id: 'query-123',
    state: 'completed',
    engine_name: 'spark',
    engine_version: '3.2.0',
    errors: [],
    links: [],
    output_table: 'output.table',
    scheduled: '2024-01-01 10:00:00',
    started: '2024-01-01 10:05:00',
    finished: '2024-01-01 10:15:00',
    numRows: 1000,
    isList: false,
  };

  it('renders table view when isList is false', () => {
    const { getByText, container } = render(<QueryInfo {...defaultProps} />);

    expect(getByText('Query ID')).toBeInTheDocument();
    expect(getByText('query-123')).toBeInTheDocument();
    expect(container.textContent).toContain('spark');
    expect(getByText('completed')).toBeInTheDocument();
  });

  it('renders list view when isList is true', () => {
    const { getByText } = render(<QueryInfo {...defaultProps} isList={true} />);

    expect(getByText('Query ID')).toBeInTheDocument();
    expect(getByText('State')).toBeInTheDocument();
    expect(getByText('Engine')).toBeInTheDocument();
  });

  it('displays errors in table view', () => {
    const propsWithErrors = {
      ...defaultProps,
      errors: ['Error 1', 'Error 2'],
    };

    const { getByText } = render(<QueryInfo {...propsWithErrors} />);

    expect(getByText('Error 1')).toBeInTheDocument();
    expect(getByText('Error 2')).toBeInTheDocument();
  });

  it('displays links in table view', () => {
    const propsWithLinks = {
      ...defaultProps,
      links: ['https://example.com/query1', 'https://example.com/query2'],
    };

    const { getByText } = render(<QueryInfo {...propsWithLinks} />);

    expect(getByText('https://example.com/query1')).toBeInTheDocument();
    expect(getByText('https://example.com/query2')).toBeInTheDocument();
  });

  it('renders empty state when no errors', () => {
    const { container } = render(<QueryInfo {...defaultProps} />);

    const errorCell = container.querySelector('td:nth-child(6)');
    expect(errorCell).toBeInTheDocument();
  });

  it('renders empty state when no links', () => {
    const { container } = render(<QueryInfo {...defaultProps} />);

    const linksCell = container.querySelector('td:nth-child(7)');
    expect(linksCell).toBeInTheDocument();
  });

  it('displays all query information in table view', () => {
    const { getByText } = render(<QueryInfo {...defaultProps} />);

    expect(getByText('output.table')).toBeInTheDocument();
    expect(getByText('1000')).toBeInTheDocument();
    expect(getByText('2024-01-01 10:00:00')).toBeInTheDocument();
    expect(getByText('2024-01-01 10:05:00')).toBeInTheDocument();
  });

  it('renders list view with query ID link when links present', () => {
    const propsWithLinks = {
      ...defaultProps,
      links: ['https://example.com/query'],
      isList: true,
    };

    const { container } = render(<QueryInfo {...propsWithLinks} />);

    const link = container.querySelector('a[href="https://example.com/query"]');
    expect(link).toBeInTheDocument();
    expect(link).toHaveTextContent('query-123');
  });

  it('renders list view with query ID as text when no links', () => {
    const propsNoLinks = {
      ...defaultProps,
      links: [],
      isList: true,
    };

    const { getByText } = render(<QueryInfo {...propsNoLinks} />);

    expect(getByText('query-123')).toBeInTheDocument();
  });

  it('displays errors with syntax highlighter in list view', () => {
    const propsWithErrors = {
      ...defaultProps,
      errors: ['Syntax error on line 5', 'Connection timeout'],
      isList: true,
    };

    const { getByText } = render(<QueryInfo {...propsWithErrors} />);

    expect(getByText('Logs')).toBeInTheDocument();
  });

  it('displays finished timestamp in list view', () => {
    const { getByText } = render(<QueryInfo {...defaultProps} isList={true} />);

    expect(getByText('Finished')).toBeInTheDocument();
    expect(getByText('2024-01-01 10:15:00')).toBeInTheDocument();
  });

  it('displays output table and row count in list view', () => {
    const { getByText } = render(<QueryInfo {...defaultProps} isList={true} />);

    expect(getByText('Output Table:')).toBeInTheDocument();
    expect(getByText('output.table')).toBeInTheDocument();
    expect(getByText('Rows:')).toBeInTheDocument();
    expect(getByText('1000')).toBeInTheDocument();
  });

  it('displays multiple links in list view', () => {
    const propsWithLinks = {
      ...defaultProps,
      links: ['https://link1.com', 'https://link2.com', 'https://link3.com'],
      isList: true,
    };

    const { getByText } = render(<QueryInfo {...propsWithLinks} />);

    expect(getByText('https://link1.com')).toBeInTheDocument();
    expect(getByText('https://link2.com')).toBeInTheDocument();
    expect(getByText('https://link3.com')).toBeInTheDocument();
  });

  it('renders empty logs section when no errors in list view', () => {
    const { getByText } = render(<QueryInfo {...defaultProps} isList={true} />);

    expect(getByText('Logs')).toBeInTheDocument();
  });

  it('displays engine name and version correctly', () => {
    const { container } = render(<QueryInfo {...defaultProps} />);

    const badges = container.querySelectorAll('.badge');
    const engineBadge = Array.from(badges).find(b =>
      b.textContent.includes('spark'),
    );
    expect(engineBadge).toBeTruthy();
    expect(engineBadge.textContent).toContain('3.2.0');
  });

  it('handles undefined optional props', () => {
    const minimalProps = {
      id: 'query-456',
      state: 'running',
      engine_name: 'trino',
      engine_version: '1.0',
    };

    const { getByText } = render(<QueryInfo {...minimalProps} />);

    expect(getByText('query-456')).toBeInTheDocument();
    expect(getByText('running')).toBeInTheDocument();
  });
});
