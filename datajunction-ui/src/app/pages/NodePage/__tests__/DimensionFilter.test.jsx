import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import DJClientContext from '../../../providers/djclient';
import DimensionFilter from '../DimensionFilter';

// Mock DJClientContext
const mockDJClient = {
  DataJunctionAPI: {
    node: jest.fn(),
    nodeData: jest.fn(),
  },
};

const mockDimension = {
  label: 'Dimension Label [Test]',
  value: 'dimension_value',
  metadata: {
    node_name: 'test_node',
    node_display_name: 'Test Node',
  },
};

const getByTextStartsWith = (container, text) => {
  return Array.from(container.querySelectorAll('*')).find(element => {
    return element.textContent.trim().startsWith(text);
  });
};

describe('DimensionFilter', () => {
  it('fetches dimension data and renders correctly', async () => {
    // Mock node response
    const mockNodeResponse = {
      type: 'dimension',
      name: 'test_node',
      columns: [
        {
          name: 'id',
          attributes: [{ attribute_type: { name: 'primary_key' } }],
        },
        { name: 'name', attributes: [{ attribute_type: { name: 'label' } }] },
      ],
    };
    mockDJClient.DataJunctionAPI.node.mockResolvedValue(mockNodeResponse);

    // Mock node data response
    const mockNodeDataResponse = {
      results: [
        {
          columns: [{ semantic_entity: 'id' }, { semantic_entity: 'name' }],
          rows: [
            [1, 'Value 1'],
            [2, 'Value 2'],
          ],
        },
      ],
    };
    mockDJClient.DataJunctionAPI.nodeData.mockResolvedValue(
      mockNodeDataResponse,
    );

    const { container } = render(
      <DJClientContext.Provider value={mockDJClient}>
        <DimensionFilter dimension={mockDimension} onChange={jest.fn()} />
      </DJClientContext.Provider>,
    );

    // Check if the dimension label and node display name are rendered
    expect(
      getByTextStartsWith(container, 'Dimension Label'),
    ).toBeInTheDocument();
    expect(screen.getByText('Test Node')).toBeInTheDocument();
  });
});
