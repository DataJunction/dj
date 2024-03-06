import React from 'react';
import { render, fireEvent, waitFor, screen } from '@testing-library/react';
import DJClientContext from '../../../providers/djclient';
import AddMaterializationPopover from '../AddMaterializationPopover';
import { mocks } from '../../../../mocks/mockNodes';

const mockDjClient = {
  DataJunctionAPI: {
    materialize: jest.fn(),
    materializationInfo: jest.fn(),
  },
};

describe('<AddMaterializationPopover />', () => {
  it('renders correctly and handles form submission', async () => {
    // Mock onSubmit function
    const onSubmitMock = jest.fn();
    mockDjClient.DataJunctionAPI.materialize.mockReturnValue({
      status: 201,
      json: {
        message: 'Saved!',
      },
    });
    mockDjClient.DataJunctionAPI.materializationInfo.mockReturnValue({
      status: 200,
      json: {
        job_types: [
          {
            name: 'spark_sql',
            label: 'Spark SQL',
            description: 'Spark SQL materialization job',
            allowed_node_types: ['transform', 'dimension', 'cube'],
            job_class: 'SparkSqlMaterializationJob',
          },
          {
            name: 'druid_metrics_cube',
            label: 'Druid Cube',
            description:
              'Used to materialize a cube to Druid for low-latency access to a set of metrics and dimensions. While the logical cube definition is at the level of metrics and dimensions, a materialized Druid cube will reference measures and dimensions, with rollup configured on the measures where appropriate.',
            allowed_node_types: ['cube'],
            job_class: 'DruidCubeMaterializationJob',
          },
        ],
        strategies: [
          {
            name: 'full',
            label: 'Full',
          },
          {
            name: 'snapshot',
            label: 'Snapshot',
          },
          {
            name: 'incremental_time',
            label: 'Incremental Time',
          },
          {
            name: 'view',
            label: 'View',
          },
        ],
      },
    });

    // Render the component
    const { getByText } = render(
      <DJClientContext.Provider value={mockDjClient}>
        <AddMaterializationPopover
          node={mocks.mockMetricNode}
          onSubmit={onSubmitMock}
        />
      </DJClientContext.Provider>,
    );

    // Open the popover
    fireEvent.click(getByText('+ Add Materialization'));

    // Save the materialization
    fireEvent.click(getByText('Save'));

    // Expect setAttributes to be called
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.materialize).toHaveBeenCalled();
      expect(getByText('Saved!')).toBeInTheDocument();
    });
  });
});
