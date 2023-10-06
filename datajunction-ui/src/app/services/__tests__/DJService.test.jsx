import { DataJunctionAPI } from '../DJService';
import { mocks } from '../../../mocks/mockNodes';

describe('DataJunctionAPI', () => {
  let originalEventSource;

  const DJ_URL = 'http://localhost:8000';
  process.env.REACT_APP_DJ_URL = DJ_URL;

  beforeEach(() => {
    fetch.resetMocks();
    originalEventSource = global.EventSource;
    global.EventSource = jest.fn();
  });

  afterEach(() => {
    global.EventSource = originalEventSource;
  });

  it('calls whoami correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.whoami();
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/whoami/`, {
      credentials: 'include',
    });
  });

  it('calls logout correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.logout();
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/logout/`, {
      credentials: 'include',
      method: 'POST',
    });
  });

  it('calls node correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify(mocks.mockMetricNode));
    const nodeData = await DataJunctionAPI.node(mocks.mockMetricNode.name);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/${mocks.mockMetricNode.name}/`,
      {
        credentials: 'include',
      },
    );
    expect(nodeData.name).toEqual(mocks.mockMetricNode.name);
    expect(nodeData.display_name).toEqual(mocks.mockMetricNode.display_name);
    expect(nodeData.type).toEqual(mocks.mockMetricNode.type);
    expect(nodeData.primary_key).toEqual([]);
  });

  it('node with errors are handled', async () => {
    const mockNotFound = { message: 'node not found' };
    fetch.mockResponseOnce(JSON.stringify(mockNotFound));
    const nodeData = await DataJunctionAPI.node(mocks.mockMetricNode.name);
    expect(nodeData.message).toEqual(mockNotFound.message);
  });

  it('calls nodes correctly', async () => {
    const prefix = 'samplePrefix';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.nodes(prefix);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/nodes/?prefix=${prefix}`, {
      credentials: 'include',
    });
  });

  it('calls createNode correctly', async () => {
    const sampleArgs = [
      'type',
      'name',
      'display_name',
      'description',
      'query',
      'mode',
      'namespace',
      'primary_key',
    ];
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.createNode(...sampleArgs);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/nodes/${sampleArgs[0]}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        name: sampleArgs[1],
        display_name: sampleArgs[2],
        description: sampleArgs[3],
        query: sampleArgs[4],
        mode: sampleArgs[5],
        namespace: sampleArgs[6],
        primary_key: sampleArgs[7],
      }),
      credentials: 'include',
    });
  });

  it('calls patchNode correctly', async () => {
    const sampleArgs = [
      'name',
      'display_name',
      'description',
      'query',
      'mode',
      'primary_key',
    ];
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.patchNode(...sampleArgs);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/nodes/${sampleArgs[0]}`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        display_name: sampleArgs[1],
        description: sampleArgs[2],
        query: sampleArgs[3],
        mode: sampleArgs[4],
        primary_key: sampleArgs[5],
      }),
      credentials: 'include',
    });

    fetch.mockResponseOnce([
      JSON.stringify({ message: 'Update failed' }),
      { status: 200 },
    ]);
    const response = await DataJunctionAPI.patchNode(...sampleArgs);
    expect(response).toEqual({
      json: { message: 'Update failed' },
      status: 500,
    });
  });

  it('calls upstreams correctly', async () => {
    const nodeName = 'sampleNode';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.upstreams(nodeName);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/${nodeName}/upstream/`,
      {
        credentials: 'include',
      },
    );
  });

  it('calls downstreams correctly', async () => {
    const nodeName = 'sampleNode';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.downstreams(nodeName);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/${nodeName}/downstream/`,
      {
        credentials: 'include',
      },
    );
  });

  it('calls node_dag correctly', async () => {
    const nodeName = 'sampleNode';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.node_dag(nodeName);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/nodes/${nodeName}/dag/`, {
      credentials: 'include',
    });
  });

  it('calls node_lineage correctly', async () => {
    const nodeName = 'sampleNode';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.node_lineage(nodeName);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/nodes/${nodeName}/lineage/`, {
      credentials: 'include',
    });
  });

  it('calls metric correctly', async () => {
    const metricName = 'sampleMetric';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.metric(metricName);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/metrics/${metricName}/`, {
      credentials: 'include',
    });
  });

  it('calls metrics correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.metrics('');
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/metrics/`, {
      credentials: 'include',
    });
  });

  it('calls namespaces correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.namespaces();
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/namespaces/`, {
      credentials: 'include',
    });
  });

  it('calls clientCode correctly', async () => {
    const name = 'sampleName';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.clientCode(name);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/datajunction-clients/python/new_node/${name}`,
      {
        credentials: 'include',
      },
    );
  });

  it('calls cube correctly', async () => {
    const name = 'sampleCube';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.cube(name);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/cubes/${name}/`, {
      credentials: 'include',
    });
  });

  it('calls commonDimensions correctly', async () => {
    const metrics = ['metric1', 'metric2'];
    const query = metrics.map(m => `metric=${m}`).join('&');
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.commonDimensions(metrics);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/metrics/common/dimensions/?${query}`,
      {
        credentials: 'include',
      },
    );
  });

  it('calls history correctly', async () => {
    const type = 'sampleType';
    const name = 'sampleName';
    const offset = 10;
    const limit = 100;
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.history(type, name, offset, limit);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/history?node=${name}&offset=${offset}&limit=${limit}`,
      {
        credentials: 'include',
      },
    );
  });

  it('calls revisions correctly', async () => {
    const name = 'sampleName';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.revisions(name);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/nodes/${name}/revisions/`, {
      credentials: 'include',
    });
  });

  it('calls namespace correctly', async () => {
    const nmspce = 'sampleNamespace';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.namespace(nmspce);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/namespaces/${nmspce}/`, {
      credentials: 'include',
    });
  });

  it('calls sql correctly', async () => {
    const metric_name = 'sampleMetric';
    const selection = { key: 'value' };
    const params = new URLSearchParams(selection).toString();
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.sql(metric_name, selection);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/sql/${metric_name}?${params}`,
      {
        credentials: 'include',
      },
    );
  });

  it('calls nodesWithDimension correctly', async () => {
    const name = 'sampleName';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.nodesWithDimension(name);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/dimensions/${name}/nodes/`, {
      credentials: 'include',
    });
  });

  it('calls materializations correctly', async () => {
    const nodeName = 'default.sample_node';
    const mockMaterializations = [
      { name: 'materialization1', clientCode: 'from dj import DJClient' },
      { name: 'materialization2', clientCode: 'from dj import DJClient' },
    ];

    // Mock the first fetch call to return the list of materializations
    fetch.mockResponseOnce(JSON.stringify(mockMaterializations));

    // Mock the subsequent fetch calls to return clientCode for each materialization
    fetch.mockResponses(
      ...mockMaterializations.map(mat => [
        JSON.stringify('from dj import DJClient'),
      ]),
    );

    const result = await DataJunctionAPI.materializations(nodeName);

    // Check the first fetch call
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/${nodeName}/materializations/`,
      {
        credentials: 'include',
      },
    );

    // Check the subsequent fetch calls for clientCode
    mockMaterializations.forEach(mat => {
      expect(fetch).toHaveBeenCalledWith(
        `${DJ_URL}/datajunction-clients/python/add_materialization/${nodeName}/${mat.name}`,
        { credentials: 'include' },
      );
    });

    // Ensure the result contains the clientCode for each materialization
    expect(result).toEqual(mockMaterializations);
  });

  it('calls columns correctly', async () => {
    const sampleNode = {
      name: 'sampleNode',
      columns: [
        { name: 'column1', dimension: { name: 'dimension1' }, clientCode: {} },
        { name: 'column2', dimension: null },
        { name: 'column3', dimension: { name: 'dimension2' }, clientCode: {} },
      ],
    };

    // Mock the fetch calls to return clientCode for columns with a dimension
    const mockClientCodeResponses = sampleNode.columns
      .filter(col => col.dimension)
      .map(col => [JSON.stringify({ clientCode: col.clientCode })]);

    fetch.mockResponses(...mockClientCodeResponses);

    const result = await DataJunctionAPI.columns(sampleNode);

    // Check the fetch calls for clientCode for columns with a dimension
    sampleNode.columns.forEach(col => {
      if (col.dimension) {
        expect(fetch).toHaveBeenCalledWith(
          `${DJ_URL}/datajunction-clients/python/link_dimension/${sampleNode.name}/${col.name}/${col.dimension.name}`,
          { credentials: 'include' },
        );
      }
    });

    // Ensure the result contains the clientCode for columns with a dimension and leaves others unchanged
    expect(result).toEqual(sampleNode.columns);
  });

  it('calls sqls correctly', async () => {
    const metricSelection = ['metric1'];
    const dimensionSelection = ['dimension1'];
    const filters = 'sampleFilter';
    const params = new URLSearchParams();
    metricSelection.forEach(metric => params.append('metrics', metric));
    dimensionSelection.forEach(dimension =>
      params.append('dimensions', dimension),
    );
    params.append('filters', filters);
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.sqls(metricSelection, dimensionSelection, filters);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/sql/?${params}`, {
      credentials: 'include',
    });
  });

  it('calls data correctly', async () => {
    const metricSelection = ['metric1'];
    const dimensionSelection = ['dimension1'];
    const params = new URLSearchParams();
    metricSelection.forEach(metric => params.append('metrics', metric));
    dimensionSelection.forEach(dimension =>
      params.append('dimensions', dimension),
    );
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.data(metricSelection, dimensionSelection);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/data/?${params}&limit=10000`,
      {
        credentials: 'include',
      },
    );
  });

  it('calls compiledSql correctly', async () => {
    const node = 'sampleNode';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.compiledSql(node);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/sql/${node}/`, {
      credentials: 'include',
    });
  });

  it('transforms node data correctly', async () => {
    fetch.mockResolvedValueOnce({
      json: () =>
        Promise.resolve({
          message: undefined,
          columns: [
            {
              name: 'id',
              attributes: [
                {
                  attribute_type: {
                    name: 'primary_key',
                  },
                },
              ],
            },
          ],
        }),
      status: 200,
    });

    const nodeData = await DataJunctionAPI.node('sampleNodeName');
    expect(nodeData.primary_key).toEqual(['id']);
  });

  it('calls stream correctly', () => {
    const metricSelection = ['metric1', 'metric2'];
    const dimensionSelection = ['dimension1', 'dimension2'];
    const filters = 'sampleFilter';

    DataJunctionAPI.stream(metricSelection, dimensionSelection, filters);

    const params = new URLSearchParams();
    metricSelection.map(metric => params.append('metrics', metric));
    dimensionSelection.map(dimension => params.append('dimensions', dimension));
    params.append('filters', filters);

    expect(global.EventSource).toHaveBeenCalledWith(
      `${DJ_URL}/stream/?${params}&limit=10000&async_=true`,
      { withCredentials: true },
    );
  });

  it('calls dag correctly and processes response', async () => {
    const mockResponse = [
      {
        name: 'namespace.node1',
        parents: [{ name: 'parent1' }, { name: null }],
        columns: [
          {
            dimension: { name: 'dimension1' },
            name: 'col1',
            type: 'type1',
            attributes: [],
          },
          {
            dimension: null,
            name: 'col2',
            type: 'type2',
            attributes: [{ attribute_type: { name: 'primary_key' } }],
          },
        ],
        table: 'table1',
        schema_: 'schema1',
        display_name: 'DisplayName1',
        type: 'type1',
      },
    ];

    fetch.mockResponseOnce(JSON.stringify(mockResponse));
    const result = await DataJunctionAPI.dag();

    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/nodes/`, {
      credentials: 'include',
    });

    expect(result.edges).toEqual([
      {
        id: 'namespace.node1-parent1',
        target: 'namespace.node1',
        source: 'parent1',
        animated: true,
        markerEnd: { type: 'arrow' },
      },
      {
        id: 'namespace.node1-dimension1',
        target: 'namespace.node1',
        source: 'dimension1',
        draggable: true,
      },
    ]);

    expect(result.nodes).toEqual([
      {
        id: 'namespace.node1',
        type: 'DJNode',
        data: {
          label: 'schema1.table1',
          table: 'table1',
          name: 'namespace.node1',
          display_name: 'DisplayName1',
          type: 'type1',
          primary_key: ['col2'],
          column_names: [
            { name: 'col1', type: 'type1' },
            { name: 'col2', type: 'type2' },
          ],
        },
      },
    ]);

    expect(result.namespaces).toEqual([
      {
        id: 'namespace',
        type: 'DJNamespace',
        data: {
          label: 'namespace',
        },
      },
    ]);
  });

  it('calls link and unlink dimension correctly', async () => {
    const nodeName = 'default.transform1';
    const dimensionName = 'default.dimension1';
    const columnName = 'column1';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.unlinkDimension(nodeName, columnName, dimensionName);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/${nodeName}/columns/${columnName}?dimension=${dimensionName}`,
      {
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
        method: 'DELETE',
      },
    );

    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.linkDimension(nodeName, columnName, dimensionName);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/${nodeName}/columns/${columnName}?dimension=${dimensionName}`,
      {
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
        method: 'POST',
      },
    );
  });

  it('calls deactivate correctly', async () => {
    const nodeName = 'default.transform1';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.deactivate(nodeName);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/nodes/${nodeName}`, {
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      method: 'DELETE',
    });
  });

  it('calls attributes correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify(mocks.attributes));
    await DataJunctionAPI.attributes();
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/attributes`, {
      credentials: 'include',
    });
  });

  it('calls dimensions correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify(mocks.dimensions));
    await DataJunctionAPI.dimensions();
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/dimensions`, {
      credentials: 'include',
    });
  });

  it('calls setAttributes correctly', async () => {
    const nodeName = 'default.transform1';
    const attributes = ['system'];
    const columnName = 'column1';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.setAttributes(nodeName, columnName, attributes);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/${nodeName}/columns/${columnName}/attributes`,
      {
        credentials: 'include',
        body: JSON.stringify([{ namespace: 'system', name: 'system' }]),
        headers: {
          'Content-Type': 'application/json',
        },
        method: 'POST',
      },
    );
  });

  it('calls listTags correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify(mocks.tags));
    const res = await DataJunctionAPI.listTags();
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/tags`, {
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      method: 'GET',
    });
    expect(res).toEqual(mocks.tags);
  });

  it('calls getTag correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify(mocks.tags[0]));
    const res = await DataJunctionAPI.getTag('report.financials');
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/tags/report.financials`, {
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      method: 'GET',
    });
    expect(res).toEqual(mocks.tags[0]);
  });

  it('calls listNodesForTag correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify(mocks.mockLinkedNodes));
    const res = await DataJunctionAPI.listNodesForTag('report.financials');
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/tags/report.financials/nodes`,
      {
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
        method: 'GET',
      },
    );
    expect(res).toEqual(mocks.mockLinkedNodes);
  });

  it('calls tagsNode correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify(mocks.mockLinkedNodes));
    await DataJunctionAPI.tagsNode('default.num_repair_orders', [
      'report.financials',
      'report.forecasts',
    ]);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/default.num_repair_orders/tags?tag_names=report.financials&tag_names=report.forecasts`,
      {
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
        method: 'POST',
      },
    );
  });

  it('calls addTag correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify(mocks.mockLinkedNodes));
    await DataJunctionAPI.addTag(
      'report.financials',
      'Financial Reports',
      'report',
      'financial reports',
    );
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/tags`, {
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        name: 'report.financials',
        display_name: 'Financial Reports',
        tag_type: 'report',
        description: 'financial reports',
      }),
      method: 'POST',
    });
  });
});
