import React from 'react';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import fetchMock from 'mocks/fetchMock';
import userEvent from '@testing-library/user-event';
import {
  initializeMockDJClient,
  renderCreateMetric,
  renderCreateNode,
  renderEditNode,
  renderEditTransformNode,
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
    vi.clearAllMocks();
    window.scrollTo = vi.fn();
  });

  it('for creating a dimension/transform node', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.createNode.mockReturnValue({
      status: 200,
      json: { name: 'default.some_test_dim' },
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
      screen.getByLabelText('Display Name *'),
      'Some Test Dim',
    );
    await userEvent.type(
      screen.getByLabelText('Query *'),
      'SELECT a, b, c FROM test',
    );
    await userEvent.click(screen.getByText('Create dimension'));

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.createNode).toBeCalled();
      expect(mockDjClient.DataJunctionAPI.createNode).toBeCalledWith(
        'dimension',
        'default.some_test_dim',
        'Some Test Dim',
        '',
        'SELECT a, b, c FROM test',
        'published',
        'default',
        null,
        undefined,
        undefined,
        undefined,
        null,
      );
      expect(screen.getByText(/default.some_test_dim/)).toBeInTheDocument();
    });

    // After successful creation, it should return a success message
    expect(screen.getByTestId('success')).toHaveTextContent(
      'Successfully created node default.some_test_dim',
    );
  }, 60000);

  it('for creating a metric node', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.createNode.mockReturnValue({
      status: 200,
      json: { name: 'default.some_test_metric' },
    });

    mockDjClient.DataJunctionAPI.tagsNode.mockReturnValue({
      status: 200,
      json: { message: 'Success' },
    });

    mockDjClient.DataJunctionAPI.nodesWithType
      .mockReturnValueOnce(['default.test1'])
      .mockReturnValueOnce(['default.test2'])
      .mockReturnValueOnce([]);

    mockDjClient.DataJunctionAPI.listTags.mockReturnValue([
      { name: 'purpose', display_name: 'Purpose' },
      { name: 'intent', display_name: 'Intent' },
    ]);

    mockDjClient.DataJunctionAPI.listMetricMetadata.mockReturnValue(
      mocks.metricMetadata,
    );

    mockDjClient.DataJunctionAPI.whoami.mockReturnValue({
      id: 123,
      username: 'test_user',
    });

    const element = testElement(mockDjClient);
    const { container } = renderCreateMetric(element);

    await userEvent.type(
      screen.getByLabelText('Display Name *'),
      'Some Test Metric',
    );
    const selectUpstream = screen.getByTestId('select-upstream-node');
    fireEvent.keyDown(selectUpstream.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getByText('default.repair_orders'));

    await userEvent.type(
      screen.getByLabelText('Aggregate Expression *'),
      'SUM(a)',
    );
    await userEvent.click(screen.getByText('Create metric'));

    await waitFor(
      () => {
        expect(mockDjClient.DataJunctionAPI.createNode).toBeCalled();
        expect(mockDjClient.DataJunctionAPI.createNode).toBeCalledWith(
          'metric',
          'default.some_test_metric',
          'Some Test Metric',
          '',
          'SELECT SUM(a) \n FROM default.repair_orders',
          'published',
          'default',
          null,
          undefined,
          undefined,
          undefined,
          null,
        );
        expect(
          screen.getByText(/default.some_test_metric/),
        ).toBeInTheDocument();
      },
      { timeout: 10000 },
    );

    // After successful creation, it should return a success message
    expect(screen.getByTestId('success')).toHaveTextContent(
      'Successfully created node default.some_test_metric',
    );
  }, 60000);

  it('for creating a semi-additive metric node', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.createNode.mockReturnValue({
      status: 200,
      json: { name: 'default.some_test_metric' },
    });

    mockDjClient.DataJunctionAPI.tagsNode.mockReturnValue({
      status: 200,
      json: { message: 'Success' },
    });

    mockDjClient.DataJunctionAPI.node.mockResolvedValue({
      columns: [{ name: 'v3.date.date_id[order]' }],
    });

    mockDjClient.DataJunctionAPI.listTags.mockReturnValue([
      { name: 'purpose', display_name: 'Purpose' },
      { name: 'intent', display_name: 'Intent' },
    ]);

    mockDjClient.DataJunctionAPI.listMetricMetadata.mockReturnValue(
      mocks.metricMetadata,
    );

    mockDjClient.DataJunctionAPI.whoami.mockReturnValue({
      id: 123,
      username: 'test_user',
    });

    const element = testElement(mockDjClient);
    const { getByTestId } = renderCreateMetric(element);

    await userEvent.type(
      screen.getByLabelText('Display Name *'),
      'Some Test Metric',
    );
    const selectUpstream = screen.getByTestId('select-upstream-node');
    fireEvent.keyDown(selectUpstream.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getByText('default.repair_orders'));

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.node).toHaveBeenCalledWith(
        'default.repair_orders',
      );
    });

    const selectSemiAdditiveDimension = getByTestId(
      'select-semi-additive-dimension',
    );
    fireEvent.keyDown(selectSemiAdditiveDimension.firstChild, {
      key: 'ArrowDown',
    });
    fireEvent.click(await screen.findByText('v3.date.date_id[order]'));
    fireEvent.change(screen.getByLabelText('Semi-Additive Type'), {
      target: { value: 'last_value' },
    });

    await userEvent.type(
      screen.getByLabelText('Aggregate Expression *'),
      'SUM(balance)',
    );
    await userEvent.click(screen.getByText('Create metric'));

    await waitFor(
      () => {
        expect(mockDjClient.DataJunctionAPI.createNode).toBeCalledWith(
          'metric',
          'default.some_test_metric',
          'Some Test Metric',
          '',
          'SELECT SUM(balance) \n FROM default.repair_orders',
          'published',
          'default',
          null,
          undefined,
          undefined,
          undefined,
          null,
          {
            dimension: 'v3.date.date_id[order]',
            function: 'last_value',
          },
        );
      },
      { timeout: 10000 },
    );
  }, 60000);

  it('for editing a transform or dimension node', async () => {
    const mockDjClient = initializeMockDJClient();

    mockDjClient.DataJunctionAPI.getNodeForEditing.mockReturnValue(
      mocks.mockGetTransformNode,
    );
    mockDjClient.DataJunctionAPI.patchNode = vi.fn();
    mockDjClient.DataJunctionAPI.patchNode.mockReturnValue({
      status: 201,
      json: { name: 'default.repair_order_transform', type: 'transform' },
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
    const { getByTestId } = renderEditTransformNode(element);

    await userEvent.type(screen.getByLabelText('Display Name *'), '!!!');
    await userEvent.type(screen.getByLabelText('Description'), '!!!');
    await userEvent.click(screen.getByText('Save'));

    const selectTags = getByTestId('select-tags');
    fireEvent.keyDown(selectTags.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getByText('Purpose'));

    await waitFor(async () => {
      expect(mockDjClient.DataJunctionAPI.patchNode).toBeCalledTimes(1);
      expect(mockDjClient.DataJunctionAPI.patchNode).toBeCalledWith(
        'default.repair_order_transform',
        'Default: Repair Order Transform!!!',
        'Repair order dimension!!!',
        'SELECT repair_order_id, municipality_id, hard_hat_id, dispatcher_id FROM default.repair_orders',
        'published',
        [],
        '',
        '',
        '',
        undefined,
        ['dj'],
        null,
      );
      expect(mockDjClient.DataJunctionAPI.tagsNode).toBeCalledTimes(1);
      expect(mockDjClient.DataJunctionAPI.tagsNode).toBeCalledWith(
        'default.repair_order_transform',
        [],
      );

      expect(mockDjClient.DataJunctionAPI.listMetricMetadata).toBeCalledTimes(
        0,
      );
      expect(
        await screen.getByText(/Successfully updated transform node/),
      ).toBeInTheDocument();
    });
  }, 1000000);

  it('for editing a metric node', async () => {
    const mockDjClient = initializeMockDJClient();

    mockDjClient.DataJunctionAPI.getNodeForEditing.mockReturnValue(
      mocks.mockGetMetricNode,
    );
    mockDjClient.DataJunctionAPI.patchNode = vi.fn();
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

    mockDjClient.DataJunctionAPI.whoami.mockReturnValue({
      id: 123,
      username: 'test_user',
    });

    const element = testElement(mockDjClient);
    const { getByTestId } = renderEditNode(element);

    await userEvent.type(screen.getByLabelText('Display Name *'), '!!!');
    await userEvent.type(screen.getByLabelText('Description'), '!!!');
    await userEvent.click(screen.getByText('Save'));

    const selectTags = getByTestId('select-tags');
    fireEvent.keyDown(selectTags.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getByText('Purpose'));

    await waitFor(async () => {
      expect(mockDjClient.DataJunctionAPI.patchNode).toBeCalledTimes(1);
      expect(mockDjClient.DataJunctionAPI.patchNode).toBeCalledWith(
        'default.num_repair_orders',
        'Default: Num Repair Orders!!!',
        'Number of repair orders!!!',
        'SELECT count(repair_order_id) \n FROM default.repair_orders',
        'published',
        ['repair_order_id', 'country'],
        'neutral',
        'unitless',
        5,
        [],
        ['dj'],
        { key1: 'value1', key2: 'value2' },
      );
      expect(mockDjClient.DataJunctionAPI.tagsNode).toBeCalledTimes(1);
      expect(mockDjClient.DataJunctionAPI.tagsNode).toBeCalledWith(
        'default.num_repair_orders',
        ['purpose'],
      );

      expect(mockDjClient.DataJunctionAPI.listMetricMetadata).toBeCalledTimes(
        1,
      );
      expect(
        await screen.getByText(/Successfully updated metric node/),
      ).toBeInTheDocument();
    });
  }, 1000000);

  it('for editing a semi-additive metric node', async () => {
    const mockDjClient = initializeMockDJClient();

    mockDjClient.DataJunctionAPI.getNodeForEditing.mockReturnValue({
      ...mocks.mockGetMetricNode,
      current: {
        ...mocks.mockGetMetricNode.current,
        semiAdditive: {
          dimension: 'v3.date.date_id[order]',
          function: 'last_value',
        },
      },
    });
    mockDjClient.DataJunctionAPI.patchNode = vi.fn();
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

    mockDjClient.DataJunctionAPI.whoami.mockReturnValue({
      id: 123,
      username: 'test_user',
    });

    const element = testElement(mockDjClient);
    renderEditNode(element);

    await waitFor(() => {
      expect(screen.getByLabelText('Semi-Additive Type')).toHaveValue(
        'last_value',
      );
      expect(screen.getByText('v3.date.date_id[order]')).toBeInTheDocument();
    });

    await userEvent.type(screen.getByLabelText('Description'), '!!!');
    await userEvent.click(screen.getByText('Save'));

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.patchNode).toBeCalledWith(
        'default.num_repair_orders',
        'Default: Num Repair Orders',
        'Number of repair orders!!!',
        'SELECT count(repair_order_id) \n FROM default.repair_orders',
        'published',
        ['repair_order_id', 'country'],
        'neutral',
        'unitless',
        5,
        [],
        ['dj'],
        { key1: 'value1', key2: 'value2' },
        {
          dimension: 'v3.date.date_id[order]',
          function: 'last_value',
        },
      );
    });
  }, 1000000);

  it('for clearing semi-additive fields from a metric node', async () => {
    const mockDjClient = initializeMockDJClient();

    mockDjClient.DataJunctionAPI.getNodeForEditing.mockReturnValue({
      ...mocks.mockGetMetricNode,
      current: {
        ...mocks.mockGetMetricNode.current,
        semiAdditive: {
          dimension: 'v3.date.date_id[order]',
          function: 'last_value',
        },
      },
    });
    mockDjClient.DataJunctionAPI.patchNode = vi.fn();
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

    mockDjClient.DataJunctionAPI.whoami.mockReturnValue({
      id: 123,
      username: 'test_user',
    });

    const element = testElement(mockDjClient);
    renderEditNode(element);

    await waitFor(() => {
      expect(screen.getByLabelText('Semi-Additive Type')).toHaveValue(
        'last_value',
      );
      expect(screen.getByText('v3.date.date_id[order]')).toBeInTheDocument();
    });

    const clearSemiAdditiveDimension = screen
      .getByTestId('select-semi-additive-dimension')
      .querySelector('.SemiAdditiveDimension__clear-indicator');
    expect(clearSemiAdditiveDimension).toBeInTheDocument();
    fireEvent.mouseDown(clearSemiAdditiveDimension, { button: 0 });
    fireEvent.change(screen.getByLabelText('Semi-Additive Type'), {
      target: { value: '' },
    });

    await userEvent.click(screen.getByText('Save'));

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.patchNode).toBeCalledWith(
        'default.num_repair_orders',
        'Default: Num Repair Orders',
        'Number of repair orders',
        'SELECT count(repair_order_id) \n FROM default.repair_orders',
        'published',
        ['repair_order_id', 'country'],
        'neutral',
        'unitless',
        5,
        [],
        ['dj'],
        { key1: 'value1', key2: 'value2' },
        null,
      );
    });
  }, 1000000);
});
