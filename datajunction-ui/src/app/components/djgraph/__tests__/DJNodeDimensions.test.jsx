import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { DJNodeDimensions } from '../DJNodeDimensions';
import DJClientContext from '../../../providers/djclient';

const mockMetric = jest.fn();
describe('<DJNodeDimensions />', () => {
  const defaultProps = {
    type: 'metric',
    name: 'TestMetric',
  };
  const mockDJClient = () => {
    return {
      DataJunctionAPI: {
        metric: mockMetric,
      },
    };
  };

  const DJNodeDimensionsWithContext = (djClient, props) => {
    return (
      <DJClientContext.Provider value={djClient}>
        <DJNodeDimensions {...props} />
      </DJClientContext.Provider>
    );
  };

  beforeEach(() => {
    // Reset the mock before each test
    mockMetric.mockReset();
  });

  it('fetches dimensions for metric type', async () => {
    mockMetric.mockResolvedValueOnce({
      dimensions: [{ name: 'test.dimension' }],
    });
    const djClient = mockDJClient();

    render(
      <DJClientContext.Provider value={djClient}>
        <DJNodeDimensions {...defaultProps} />
      </DJClientContext.Provider>,
    );
    waitFor(() => {
      expect(mockMetric).toHaveBeenCalledWith(defaultProps.name);
    });
  });

  it('renders dimensions correctly after processing', async () => {
    const testDimensions = [
      {
        name: 'default.us_state.state_name',
        type: 'string',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
          'default.hard_hat.state',
        ],
      },
      {
        name: 'default.us_state.state_region',
        type: 'int',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
          'default.hard_hat.state',
        ],
      },
      {
        name: 'default.us_state.state_region_description',
        type: 'string',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
          'default.hard_hat.state',
        ],
      },
    ];
    mockMetric.mockResolvedValueOnce({ dimensions: testDimensions });
    const djClient = mockDJClient();

    const { findByText } = render(
      <DJClientContext.Provider value={djClient}>
        <DJNodeDimensions {...defaultProps} />
      </DJClientContext.Provider>,
    );

    for (const dim of testDimensions) {
      const [attribute, ...nodeName] = dim.name.split('.').reverse();
      const dimension = nodeName.reverse().join('.');
      expect(await findByText(attribute)).toBeInTheDocument();
      expect(await findByText(dimension)).toBeInTheDocument();
    }
  });

  it('does not fetch dimensions if type is not metric', () => {
    const djClient = mockDJClient();
    render(
      <DJClientContext.Provider value={djClient}>
        <DJNodeDimensions {...defaultProps} type="transform" />
      </DJClientContext.Provider>,
    );
    expect(mockMetric).not.toHaveBeenCalled();
  });

  it('handles errors gracefully', async () => {
    mockMetric.mockRejectedValueOnce(new Error('API error'));

    const djClient = mockDJClient();
    render(
      <DJClientContext.Provider value={djClient}>
        <DJNodeDimensions {...defaultProps} />
      </DJClientContext.Provider>,
    );

    expect(await mockMetric).toHaveBeenCalledWith(defaultProps.name);
  });
});
