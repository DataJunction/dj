import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import NodeColumnLineage from '../NodeLineageTab';
import DJClientContext from '../../../providers/djclient';
import { mocks } from '../../../../mocks/mockNodes';

describe('<NodeColumnLineage />', () => {
  const domTestingLib = require('@testing-library/dom');
  const { queryHelpers } = domTestingLib;

  const queryByAttribute = attribute =>
    queryHelpers.queryAllByAttribute.bind(null, attribute);

  function getByAttribute(container, id, attribute, ...rest) {
    const result = queryByAttribute(attribute)(container, id, ...rest);
    return result[0];
  }

  const mockDJClient = () => {
    return {
      DataJunctionAPI: {
        node: jest.fn(),
        metric: jest.fn(),
        node_lineage: jest.fn(name => {
          return mocks.mockNodeLineage;
        }),
      },
    };
  };

  const defaultProps = {
    djNode: mocks.mockMetricNodeJson,
  };

  it('renders and calls node_lineage with the correct node name', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.metric = name => defaultProps.djNode;
    const { container } = render(
      <DJClientContext.Provider value={djClient}>
        <NodeColumnLineage {...defaultProps} />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(djClient.DataJunctionAPI.node_lineage).toHaveBeenCalledWith(
        defaultProps.djNode.name,
      );

      // The origin node should be displayed
      expect(screen.getByText('Default: Avg Repair Price')).toBeInTheDocument();

      expect(
        getByAttribute(container, 'default.avg_repair_price', 'data-id'),
      ).toBeInTheDocument();
    });
  });
});
