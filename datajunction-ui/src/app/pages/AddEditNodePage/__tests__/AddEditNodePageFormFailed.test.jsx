import React from 'react';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import fetchMock from 'jest-fetch-mock';
import userEvent from '@testing-library/user-event';
import {
  initializeMockDJClient,
  renderCreateNode,
  renderEditNode,
  testElement,
} from './index.test';
import { mocks } from '../../../../mocks/mockNodes';

describe('AddEditNodePage submission failed', () => {
  beforeEach(() => {
    fetchMock.resetMocks();
    jest.clearAllMocks();
    window.scrollTo = jest.fn();
  });

  it('for creating a node', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.createNode.mockReturnValue({
      status: 500,
      json: { message: 'Some columns in the primary key [] were not found' },
    });
    mockDjClient.DataJunctionAPI.listTags.mockReturnValue([
      { name: 'purpose', display_name: 'Purpose' },
      { name: 'intent', display_name: 'Intent' },
    ]);

    const element = testElement(mockDjClient);
    const { container } = renderCreateNode(element);

    await userEvent.type(
      screen.getByLabelText('Display Name *'),
      'Some Test Metric',
    );
    await userEvent.type(
      screen.getByLabelText('Query *'),
      'SELECT * FROM test',
    );
    await userEvent.click(screen.getByText('Create dimension'));

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.createNode).toBeCalled();
      expect(mockDjClient.DataJunctionAPI.createNode).toBeCalledWith(
        'dimension',
        'default.some_test_metric',
        'Some Test Metric',
        '',
        'SELECT * FROM test',
        'draft',
        'default',
        null,
        undefined,
        undefined,
      );
      expect(
        screen.getByText(/Some columns in the primary key \[] were not found/),
      ).toBeInTheDocument();
    });

    // After failed creation, it should return a failure message
    expect(container.getElementsByClassName('alert')).toMatchSnapshot();
  }, 60000);

  it('for editing a node', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    mockDjClient.DataJunctionAPI.patchNode.mockReturnValue({
      status: 500,
      json: { message: 'Update failed' },
    });

    mockDjClient.DataJunctionAPI.tagsNode.mockReturnValue({
      status: 404,
      json: { message: 'Some tags were not found' },
    });

    mockDjClient.DataJunctionAPI.listTags.mockReturnValue([
      { name: 'purpose', display_name: 'Purpose' },
      { name: 'intent', display_name: 'Intent' },
    ]);

    const element = testElement(mockDjClient);
    renderEditNode(element);

    await userEvent.type(screen.getByLabelText('Display Name *'), '!!!');
    await userEvent.type(screen.getByLabelText('Description'), '!!!');
    await userEvent.click(screen.getByText('Save'));
    await waitFor(async () => {
      expect(mockDjClient.DataJunctionAPI.patchNode).toBeCalledTimes(1);
      expect(mockDjClient.DataJunctionAPI.tagsNode).toBeCalled();
      expect(mockDjClient.DataJunctionAPI.tagsNode).toBeCalledWith(
        'default.num_repair_orders',
        ['purpose'],
      );
      expect(mockDjClient.DataJunctionAPI.tagsNode).toReturnWith({
        json: { message: 'Some tags were not found' },
        status: 404,
      });

      expect(
        await screen.getByText('Update failed, Some tags were not found'),
      ).toBeInTheDocument();
    });
  }, 60000);
});
