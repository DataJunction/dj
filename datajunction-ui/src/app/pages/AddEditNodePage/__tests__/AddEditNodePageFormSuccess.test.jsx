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
import { render } from '../../../../setupTests';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import DJClientContext from '../../../providers/djclient';
import { AddEditNodePage } from '../index';

describe('AddEditNodePage submission succeeded', () => {
  beforeEach(() => {
    fetchMock.resetMocks();
    jest.clearAllMocks();
    window.scrollTo = jest.fn();
  });

  it('for creating a node', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.createNode.mockReturnValue({
      status: 200,
      json: { name: 'default.some_test_metric' },
    });

    mockDjClient.DataJunctionAPI.tagsNode.mockReturnValue({
      status: 200,
      json: { message: 'Success' },
    });

    mockDjClient.DataJunctionAPI.listTags.mockReturnValue([
      { name: 'purpose', display_name: 'Purpose' },
      { name: 'intent', display_name: 'Intent' },
    ]);

    const element = testElement(mockDjClient);
    const { container } = renderCreateNode(element);

    await userEvent.type(
      screen.getByLabelText('Display Name'),
      'Some Test Metric',
    );
    await userEvent.type(screen.getByLabelText('Query'), 'SELECT * FROM test');
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
      expect(screen.getByText(/default.some_test_metric/)).toBeInTheDocument();
    });

    // After successful creation, it should return a success message
    expect(screen.getByTestId('success')).toHaveTextContent(
      'Successfully created node default.some_test_metric',
    );
  }, 60000);

  it('for editing a node', async () => {
    const mockDjClient = initializeMockDJClient();

    mockDjClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    mockDjClient.DataJunctionAPI.patchNode = jest.fn();
    mockDjClient.DataJunctionAPI.patchNode.mockReturnValue({
      status: 201,
      json: { name: 'default.num_repair_orders', type: 'metric' },
    });

    mockDjClient.DataJunctionAPI.tagsNode.mockReturnValue({
      status: 200,
      json: { message: 'Success' },
    });

    mockDjClient.DataJunctionAPI.listTags.mockReturnValue([
      { name: 'purpose', display_name: 'Purpose' },
      { name: 'intent', display_name: 'Intent' },
    ]);

    const element = testElement(mockDjClient);
    const { getByTestId } = renderEditNode(element);

    await userEvent.type(screen.getByLabelText('Display Name'), '!!!');
    await userEvent.type(screen.getByLabelText('Description'), '!!!');
    await userEvent.click(screen.getByText('Save'));

    const selectTags = getByTestId('select-tags');
    await fireEvent.keyDown(selectTags.firstChild, { key: 'ArrowDown' });
    await fireEvent.click(screen.getByText('Purpose'));

    await waitFor(async () => {
      expect(mockDjClient.DataJunctionAPI.patchNode).toBeCalledTimes(1);
      expect(mockDjClient.DataJunctionAPI.patchNode).toBeCalledWith(
        'default.num_repair_orders',
        'Default: Num Repair Orders!!!',
        'Number of repair orders!!!',
        'SELECT count(repair_order_id) default_DOT_num_repair_orders FROM default.repair_orders',
        'published',
        ['repair_order_id', 'country'],
        undefined,
        undefined,
      );
      expect(mockDjClient.DataJunctionAPI.tagsNode).toBeCalledTimes(1);
      expect(mockDjClient.DataJunctionAPI.tagsNode).toBeCalledWith(
        'default.num_repair_orders',
        [{ display_name: 'Purpose', name: 'purpose' }],
      );

      expect(
        await screen.getByDisplayValue('repair_order_id, country'),
      ).toBeInTheDocument();
      expect(
        await screen.getByText(/Successfully updated metric node/),
      ).toBeInTheDocument();
    });
  }, 1000000);
});
