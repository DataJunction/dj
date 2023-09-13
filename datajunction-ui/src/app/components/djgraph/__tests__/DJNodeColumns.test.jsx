import React from 'react';
import { render, screen } from '@testing-library/react';
import { DJNodeColumns } from '../DJNodeColumns';
import { ReactFlowProvider } from 'reactflow';

describe('<DJNodeColumns />', () => {
  const defaultProps = {
    data: {
      name: 'TestName',
      type: 'metric',
      column_names: [
        { name: 'col1', type: 'int' },
        { name: 'col2', type: 'string' },
        { name: 'col3', type: 'float' },
      ],
      primary_key: ['col1'],
    },
    limit: 10,
  };

  const domTestingLib = require('@testing-library/dom');
  const { queryHelpers } = domTestingLib;

  const queryByAttribute = attribute =>
    queryHelpers.queryAllByAttribute.bind(null, attribute);

  function getByAttribute(container, id, attribute, ...rest) {
    const result = queryByAttribute(attribute)(container, id, ...rest);
    return result[0];
  }

  function DJNodeColumnsWithProvider(props) {
    return (
      <ReactFlowProvider>
        <DJNodeColumns {...props} />
      </ReactFlowProvider>
    );
  }

  it('renders without crashing', () => {
    render(<DJNodeColumnsWithProvider {...defaultProps} />);
  });

  it('renders columns correctly', () => {
    const { container } = render(
      <DJNodeColumnsWithProvider {...defaultProps} />,
    );

    // renders column names
    defaultProps.data.column_names.forEach(col => {
      expect(screen.getByText(col.name, { exact: false })).toBeInTheDocument();
    });

    // appends (PK) to primary key column name
    expect(screen.getByText('col1 (PK)')).toBeInTheDocument();

    // renders column type badge correctly
    defaultProps.data.column_names.forEach(col => {
      expect(screen.getByText(col.type)).toBeInTheDocument();
    });

    // renders handles correctly
    defaultProps.data.column_names.forEach(col => {
      expect(
        getByAttribute(
          container,
          defaultProps.data.name + '.' + col.name,
          'data-handleid',
        ),
      ).toBeInTheDocument();
    });
  });

  it('renders limited columns based on the limit prop', () => {
    const limitedProps = {
      ...defaultProps,
      limit: 2,
    };

    render(<DJNodeColumnsWithProvider {...limitedProps} />);
    expect(screen.queryByText('col3')).not.toBeInTheDocument();
  });
});
