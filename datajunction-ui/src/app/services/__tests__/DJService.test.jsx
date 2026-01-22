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

  it('calls catalogs correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.catalogs();
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/catalogs`, {
      credentials: 'include',
    });
  });

  it('calls engines correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.engines();
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/engines`, {
      credentials: 'include',
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

  it('calls nodeDetails correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify([mocks.mockMetricNode]));
    const nodeData = await DataJunctionAPI.nodeDetails();
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/nodes/details/`, {
      credentials: 'include',
    });
    expect(nodeData).toEqual([mocks.mockMetricNode]);
  });

  it('calls validate correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify([mocks.mockMetricNode]));
    await DataJunctionAPI.validateNode(
      'metric',
      'default.num_repair_orders',
      'aa',
      'desc',
      'select 1',
    );
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/nodes/validate`, {
      credentials: 'include',
      body: JSON.stringify({
        name: 'default.num_repair_orders',
        display_name: 'aa',
        description: 'desc',
        query: 'select 1',
        type: 'metric',
        mode: 'published',
      }),

      headers: {
        'Content-Type': 'application/json',
      },
      method: 'POST',
    });
  });

  it('calls registerTable correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify([mocks.mockMetricNode]));
    await DataJunctionAPI.registerTable('default', 'xyz', 'abc');
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/register/table/default/xyz/abc`,
      {
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
        method: 'POST',
      },
    );
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

  it('calls nodesWithType correctly', async () => {
    const nodeType = 'transform';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.nodesWithType(nodeType);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/?node_type=${nodeType}`,
      {
        credentials: 'include',
      },
    );
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
      undefined, // metric_direction
      undefined, // metric_unit
      undefined, // required_dimensions
      { key: 'value' }, // custom_metadata
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
        metric_metadata: null,
        required_dimensions: undefined,
        custom_metadata: { key: 'value' },
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
      'neutral',
      '',
      null, // significant_digits
      undefined, // required_dimensions
      undefined, // owners
      { key: 'value' }, // custom_metadata
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
        metric_metadata: {
          direction: 'neutral',
          unit: '',
          significant_digits: null,
        },
        required_dimensions: undefined,
        owners: undefined,
        custom_metadata: { key: 'value' },
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

  it('calls createCube correctly', async () => {
    const sampleArgs = [
      'default.node_name',
      'Node Display Name',
      'Some readable description',
      'draft',
      ['default.num_repair_orders'],
      [
        'default.date_dim.year',
        'default.date_dim.month',
        'default.date_dim.day',
      ],
      [],
    ];
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.createCube(...sampleArgs);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/nodes/cube`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        name: sampleArgs[0],
        display_name: sampleArgs[1],
        description: sampleArgs[2],
        metrics: sampleArgs[4],
        dimensions: sampleArgs[5],
        filters: sampleArgs[6],
        mode: sampleArgs[3],
      }),
      credentials: 'include',
    });
  });

  it('calls patchCube correctly', async () => {
    const sampleArgs = [
      'default.node_name',
      'Node Display Name',
      'Some readable description',
      'draft',
      ['default.num_repair_orders'],
      [
        'default.date_dim.year',
        'default.date_dim.month',
        'default.date_dim.day',
      ],
      [],
    ];
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.patchCube(...sampleArgs);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/nodes/default.node_name`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        display_name: sampleArgs[1],
        description: sampleArgs[2],
        metrics: sampleArgs[4],
        dimensions: sampleArgs[5],
        filters: sampleArgs[6],
        mode: sampleArgs[3],
      }),
      credentials: 'include',
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

  it('calls upstreamsGQL correctly with single node', async () => {
    const nodeName = 'sampleNode';
    fetch.mockResponseOnce(
      JSON.stringify({
        data: { upstreamNodes: [{ name: 'upstream1', type: 'SOURCE' }] },
      }),
    );
    const result = await DataJunctionAPI.upstreamsGQL(nodeName);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/graphql`,
      expect.objectContaining({
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
      }),
    );
    expect(result).toEqual([{ name: 'upstream1', type: 'SOURCE' }]);
  });

  it('calls upstreamsGQL correctly with multiple nodes', async () => {
    const nodeNames = ['node1', 'node2'];
    fetch.mockResponseOnce(
      JSON.stringify({
        data: {
          upstreamNodes: [
            { name: 'upstream1', type: 'SOURCE' },
            { name: 'upstream2', type: 'TRANSFORM' },
          ],
        },
      }),
    );
    const result = await DataJunctionAPI.upstreamsGQL(nodeNames);
    expect(result).toEqual([
      { name: 'upstream1', type: 'SOURCE' },
      { name: 'upstream2', type: 'TRANSFORM' },
    ]);
  });

  it('calls downstreamsGQL correctly with single node', async () => {
    const nodeName = 'sampleNode';
    fetch.mockResponseOnce(
      JSON.stringify({
        data: { downstreamNodes: [{ name: 'downstream1', type: 'METRIC' }] },
      }),
    );
    const result = await DataJunctionAPI.downstreamsGQL(nodeName);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/graphql`,
      expect.objectContaining({
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
      }),
    );
    expect(result).toEqual([{ name: 'downstream1', type: 'METRIC' }]);
  });

  it('calls downstreamsGQL correctly with multiple nodes', async () => {
    const nodeNames = ['node1', 'node2'];
    fetch.mockResponseOnce(
      JSON.stringify({
        data: {
          downstreamNodes: [
            { name: 'downstream1', type: 'METRIC' },
            { name: 'downstream2', type: 'CUBE' },
          ],
        },
      }),
    );
    const result = await DataJunctionAPI.downstreamsGQL(nodeNames);
    expect(result).toEqual([
      { name: 'downstream1', type: 'METRIC' },
      { name: 'downstream2', type: 'CUBE' },
    ]);
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

  it('calls listMetricMetadata correctly', async () => {
    const nodeType = 'transform';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.listMetricMetadata();
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/metrics/metadata`, {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
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
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/namespaces/${nmspce}?edited_by=undefined&with_edited_by=true`,
      {
        credentials: 'include',
      },
    );
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
      { name: 'materialization1' },
      { name: 'materialization2' },
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
      `${DJ_URL}/nodes/${nodeName}/materializations?show_inactive=true&include_all_revisions=true`,
      {
        credentials: 'include',
      },
    );

    // Ensure the result contains the clientCode for each materialization
    expect(result).toEqual(mockMaterializations);
  });

  it('calls columns correctly', async () => {
    const sampleNode = {
      name: 'sampleNode',
      columns: [
        { name: 'column1', dimension: { name: 'dimension1' } },
        { name: 'column2', dimension: null },
        { name: 'column3', dimension: { name: 'dimension2' } },
      ],
    };

    // Mock the fetch calls to return clientCode for columns with a dimension
    const mockClientCodeResponses = sampleNode.columns
      .filter(col => col.dimension)
      .map(col => [JSON.stringify({ clientCode: col.clientCode })]);

    fetch.mockResponses(...mockClientCodeResponses);

    const result = await DataJunctionAPI.columns(sampleNode);
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

  it('calls streamNodeData correctly', () => {
    const nodes = ['transform1'];
    const dimensionSelection = ['dimension1', 'dimension2'];
    const filters = 'sampleFilter';

    DataJunctionAPI.streamNodeData(nodes[0], {
      dimensions: dimensionSelection,
      filters: filters,
    });

    const params = new URLSearchParams();
    params.append('dimensions', dimensionSelection, 'filters', filters);

    expect(global.EventSource).toHaveBeenCalledWith(
      `${DJ_URL}/stream/transform1?filters=sampleFilter&dimensions=dimension1&dimensions=dimension2&limit=1000&async_=true`,
      { withCredentials: true },
    );
  });

  it('calls nodeData correctly', () => {
    const nodes = ['transform1'];
    const dimensionSelection = ['dimension1', 'dimension2'];
    const filters = 'sampleFilter';
    fetch.mockResponseOnce(JSON.stringify({}));

    DataJunctionAPI.nodeData(nodes[0], {
      dimensions: dimensionSelection,
      filters: filters,
    });

    const params = new URLSearchParams();
    params.append('dimensions', dimensionSelection, 'filters', filters);

    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/data/transform1?filters=sampleFilter&dimensions=dimension1&dimensions=dimension2&limit=1000&async_=true`,
      {
        credentials: 'include',
        headers: {
          'Cache-Control': 'max-age=86400',
        },
      },
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

  it('calls add and remove complex dimension link correctly', async () => {
    const nodeName = 'default.transform1';
    const dimensionNode = 'default.dimension1';
    const joinOn = 'blah';
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.removeComplexDimensionLink(nodeName, dimensionNode);
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/nodes/${nodeName}/link`, {
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        dimension_node: dimensionNode,
        role: null,
      }),
      method: 'DELETE',
    });

    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.addComplexDimensionLink(
      nodeName,
      dimensionNode,
      joinOn,
    );
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/nodes/${nodeName}/link`, {
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        dimension_node: dimensionNode,
        join_type: null,
        join_on: joinOn,
        join_cardinality: null,
        role: null,
      }),
      method: 'POST',
    });
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

  it('calls addNamespace correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.addNamespace('test');
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/namespaces/test`, {
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      method: 'POST',
    });
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

  it('calls editTag correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.editTag(
      'report.financials',
      'Financial reports',
      'Financial Reports',
    );
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/tags/report.financials`, {
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        description: 'Financial reports',
        display_name: 'Financial Reports',
      }),
      method: 'PATCH',
    });
  });

  it('calls setPartition correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.setPartition(
      'default.hard_hat',
      'hire_date',
      'temporal',
      'yyyyMMdd',
      'day',
    );
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/default.hard_hat/columns/hire_date/partition`,
      {
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          type_: 'temporal',
          format: 'yyyyMMdd',
          granularity: 'day',
        }),
        method: 'POST',
      },
    );
  });

  it('calls materialize correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.materialize(
      'default.hard_hat',
      'spark_sql',
      'full',
      '@daily',
      {},
    );
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/default.hard_hat/materialization`,
      {
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          job: 'spark_sql',
          strategy: 'full',
          schedule: '@daily',
          config: {},
        }),
        method: 'POST',
      },
    );
  });

  it('calls runBackfill correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.runBackfill('default.hard_hat', 'spark', [
      {
        columnName: 'hire_date',
        range: ['20230101', '20230202'],
      },
    ]);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/default.hard_hat/materializations/spark/backfill`,
      {
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify([
          {
            column_name: 'hire_date',
            range: ['20230101', '20230202'],
          },
        ]),
        method: 'POST',
      },
    );
  });

  it('calls materializationInfo correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.materializationInfo();
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/materialization/info`, {
      credentials: 'include',
    });
  });

  it('calls revalidate correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.revalidate('default.hard_hat');
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/default.hard_hat/validate`,
      {
        credentials: 'include',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
      },
    );
  });

  it('calls deleteMaterialization correctly', () => {
    const nodeName = 'transform1';
    const materializationName = 'sampleMaterialization';
    fetch.mockResponseOnce(JSON.stringify({}));

    DataJunctionAPI.deleteMaterialization(nodeName, materializationName);
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/${nodeName}/materializations?materialization_name=${materializationName}`,
      {
        method: 'DELETE',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
      },
    );
  });

  it('calls listNodesForLanding correctly', () => {
    fetch.mockResponseOnce(JSON.stringify({}));

    DataJunctionAPI.listNodesForLanding(
      '',
      ['source'],
      [],
      '',
      null,
      null,
      100,
      {
        key: 'updatedAt',
        direction: 'descending',
      },
    );
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/graphql`,
      expect.objectContaining({
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
      }),
    );
  });

  it('calls getMetric correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        data: { findNodes: [{ name: 'default.num_repair_orders' }] },
      }),
    );
    await DataJunctionAPI.getMetric('default.num_repair_orders');
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/graphql`,
      expect.objectContaining({
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
      }),
    );
  });

  it('calls notebookExportCube correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.notebookExportCube('default.repairs_cube');
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/datajunction-clients/python/notebook/?cube=default.repairs_cube`,
      {
        credentials: 'include',
      },
    );
  });

  it('calls notebookExportNamespace correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({}));
    await DataJunctionAPI.notebookExportNamespace('default');
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/datajunction-clients/python/notebook/?namespace=default`,
      {
        credentials: 'include',
      },
    );
  });

  it('calls node_counts_by_type correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([
        [
          {
            value: 'cube',
            col: 'system.dj.node_type.type',
          },
          {
            value: 226,
            col: 'system.dj.number_of_nodes',
          },
        ],
        [
          {
            value: 'dimension',
            col: 'system.dj.node_type.type',
          },
          {
            value: 241,
            col: 'system.dj.number_of_nodes',
          },
        ],
        [
          {
            value: 'metric',
            col: 'system.dj.node_type.type',
          },
          {
            value: 2853,
            col: 'system.dj.number_of_nodes',
          },
        ],
        [
          {
            value: 'source',
            col: 'system.dj.node_type.type',
          },
          {
            value: 540,
            col: 'system.dj.number_of_nodes',
          },
        ],
        [
          {
            value: 'transform',
            col: 'system.dj.node_type.type',
          },
          {
            value: 663,
            col: 'system.dj.number_of_nodes',
          },
        ],
      ]),
    );
    const results = await DataJunctionAPI.system.node_counts_by_type();
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/system/data/system.dj.number_of_nodes?dimensions=system.dj.node_type.type&filters=system.dj.is_active.active_id%3Dtrue&orderby=system.dj.node_type.type`,
      {
        credentials: 'include',
      },
    );
    expect(results).toEqual([
      { name: 'cube', value: 226 },
      { name: 'dimension', value: 241 },
      { name: 'metric', value: 2853 },
      { name: 'source', value: 540 },
      { name: 'transform', value: 663 },
    ]);
  });

  it('calls node_counts_by_active correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([
        [
          {
            value: false,
            col: 'system.dj.is_active.active_id',
          },
          {
            value: 3136,
            col: 'system.dj.number_of_nodes',
          },
        ],
        [
          {
            value: true,
            col: 'system.dj.is_active.active_id',
          },
          {
            value: 4523,
            col: 'system.dj.number_of_nodes',
          },
        ],
      ]),
    );
    const results = await DataJunctionAPI.system.node_counts_by_active();
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/system/data/system.dj.number_of_nodes?dimensions=system.dj.is_active.active_id`,
      {
        credentials: 'include',
      },
    );
    expect(results).toEqual([
      { name: 'false', value: 3136 },
      { name: 'true', value: 4523 },
    ]);
  });

  it('calls node_counts_by_status correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([
        [
          {
            value: 'VALID',
            col: 'system.dj.nodes.status',
          },
          {
            value: 4333,
            col: 'system.dj.number_of_nodes',
          },
        ],
        [
          {
            value: 'INVALID',
            col: 'system.dj.nodes.status',
          },
          {
            value: 190,
            col: 'system.dj.number_of_nodes',
          },
        ],
      ]),
    );
    const results = await DataJunctionAPI.system.node_counts_by_status();
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/system/data/system.dj.number_of_nodes?dimensions=system.dj.nodes.status&filters=system.dj.is_active.active_id%3Dtrue&orderby=system.dj.nodes.status`,
      {
        credentials: 'include',
      },
    );
    expect(results).toEqual([
      { name: 'VALID', value: 4333 },
      { name: 'INVALID', value: 190 },
    ]);
  });

  it('calls nodes_without_description correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([
        [
          {
            value: 'cube',
            col: 'system.dj.node_type.type',
          },
          {
            value: 0.1,
            col: 'system.dj.node_without_description',
          },
        ],
        [
          {
            value: 'dimension',
            col: 'system.dj.node_type.type',
          },
          {
            value: 0.2,
            col: 'system.dj.node_without_description',
          },
        ],
        [
          {
            value: 'metric',
            col: 'system.dj.node_type.type',
          },
          {
            value: 0.3,
            col: 'system.dj.node_without_description',
          },
        ],
        [
          {
            value: 'source',
            col: 'system.dj.node_type.type',
          },
          {
            value: 0.4,
            col: 'system.dj.node_without_description',
          },
        ],
        [
          {
            value: 'transform',
            col: 'system.dj.node_type.type',
          },
          {
            value: 0.5,
            col: 'system.dj.node_without_description',
          },
        ],
      ]),
    );
    const results = await DataJunctionAPI.system.nodes_without_description();
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/system/data/system.dj.node_without_description?dimensions=system.dj.node_type.type&filters=system.dj.is_active.active_id%3Dtrue&orderby=system.dj.node_type.type`,
      {
        credentials: 'include',
      },
    );
    expect(results).toEqual([
      { name: 'cube', value: 0.1 },
      { name: 'dimension', value: 0.2 },
      { name: 'metric', value: 0.3 },
      { name: 'source', value: 0.4 },
      { name: 'transform', value: 0.5 },
    ]);
  });
  it('calls node_trends correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([
        [
          {
            value: 20250630,
            col: 'system.dj.nodes.created_at_week',
          },
          {
            value: 'metric',
            col: 'system.dj.node_type.type',
          },
          {
            value: 42,
            col: 'system.dj.number_of_nodes',
          },
        ],
        [
          {
            value: 20250707,
            col: 'system.dj.nodes.created_at_week',
          },
          {
            value: 'dimension',
            col: 'system.dj.node_type.type',
          },
          {
            value: 21,
            col: 'system.dj.number_of_nodes',
          },
        ],
        [
          {
            value: 20250707,
            col: 'system.dj.nodes.created_at_week',
          },
          {
            value: 'metric',
            col: 'system.dj.node_type.type',
          },
          {
            value: 9,
            col: 'system.dj.number_of_nodes',
          },
        ],
        [
          {
            value: 20250714,
            col: 'system.dj.nodes.created_at_week',
          },
          {
            value: 'metric',
            col: 'system.dj.node_type.type',
          },
          {
            value: 3,
            col: 'system.dj.number_of_nodes',
          },
        ],
        [
          {
            value: 20250714,
            col: 'system.dj.nodes.created_at_week',
          },
          {
            value: 'dimension',
            col: 'system.dj.node_type.type',
          },
          {
            value: 7,
            col: 'system.dj.number_of_nodes',
          },
        ],
      ]),
    );
    const results = await DataJunctionAPI.system.node_trends();
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/system/data/system.dj.number_of_nodes?dimensions=system.dj.nodes.created_at_week&dimensions=system.dj.node_type.type&filters=system.dj.nodes.created_at_week>=20240101&orderby=system.dj.nodes.created_at_week`,
      {
        credentials: 'include',
      },
    );
    expect(results).toEqual([
      { date: 20250630, metric: 42 },
      { date: 20250707, dimension: 21, metric: 9 },
      { date: 20250714, dimension: 7, metric: 3 },
    ]);
  });

  it('calls materialization_counts_by_type correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([
        [
          {
            value: 'cube',
            col: 'system.dj.node_type.type',
          },
          {
            value: 76,
            col: 'system.dj.number_of_materializations',
          },
        ],
        [
          {
            value: 'dimension',
            col: 'system.dj.node_type.type',
          },
          {
            value: 3,
            col: 'system.dj.number_of_materializations',
          },
        ],
        [
          {
            value: 'transform',
            col: 'system.dj.node_type.type',
          },
          {
            value: 9,
            col: 'system.dj.number_of_materializations',
          },
        ],
      ]),
    );
    const results =
      await DataJunctionAPI.system.materialization_counts_by_type();
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/system/data/system.dj.number_of_materializations?dimensions=system.dj.node_type.type&filters=system.dj.is_active.active_id%3Dtrue&orderby=system.dj.node_type.type`,
      {
        credentials: 'include',
      },
    );
    expect(results).toEqual([
      { name: 'cube', value: 76 },
      { name: 'dimension', value: 3 },
      { name: 'transform', value: 9 },
    ]);
  });

  it('calls system.dimensions correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify([]));
    const results = await DataJunctionAPI.system.dimensions();
    expect(fetch).toHaveBeenCalledWith(`${DJ_URL}/system/dimensions`, {
      credentials: 'include',
    });
  });

  it('calls availabilityStates correctly', async () => {
    const nodeName = 'default.sample_node';
    const mockAvailabilityStates = [
      {
        id: 1,
        catalog: 'test_catalog',
        schema_: 'test_schema',
        table: 'test_table',
        valid_through_ts: 1640995200,
        url: 'http://example.com/table',
        node_revision_id: 123,
        node_version: '1.0.0',
      },
    ];

    fetch.mockResponseOnce(JSON.stringify(mockAvailabilityStates));

    const result = await DataJunctionAPI.availabilityStates(nodeName);

    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/${nodeName}/availability/`,
      {
        credentials: 'include',
      },
    );
    expect(result).toEqual(mockAvailabilityStates);
  });

  it('calls refreshLatestMaterialization correctly', async () => {
    const nodeName = 'default.sample_cube';
    const mockResponse = { message: 'Materialization refreshed successfully' };

    fetch.mockResponseOnce(JSON.stringify(mockResponse));

    const result = await DataJunctionAPI.refreshLatestMaterialization(nodeName);

    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/${nodeName}?refresh_materialization=true`,
      {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({}),
        credentials: 'include',
      },
    );
    expect(result).toEqual({ status: 200, json: mockResponse });
  });

  // Reference dimension links
  it('calls addReferenceDimensionLink correctly', async () => {
    const mockResponse = { message: 'Success' };
    fetch.mockResponseOnce(JSON.stringify(mockResponse));
    const result = await DataJunctionAPI.addReferenceDimensionLink(
      'default.node1',
      'column1',
      'default.dimension1',
      'dimension_col',
    );
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/default.node1/columns/column1/link?dimension_node=default.dimension1&dimension_column=dimension_col`,
      expect.objectContaining({
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
      }),
    );
    expect(result).toEqual({ status: 200, json: mockResponse });
  });

  it('calls removeReferenceDimensionLink correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'Success' }));
    const result = await DataJunctionAPI.removeReferenceDimensionLink(
      'default.node1',
      'column1',
    );
    expect(fetch).toHaveBeenCalledWith(
      `${DJ_URL}/nodes/default.node1/columns/column1/link`,
      expect.objectContaining({
        method: 'DELETE',
      }),
    );
  });

  // Materialization
  it('calls deleteMaterialization with nodeVersion correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'Success' }));
    await DataJunctionAPI.deleteMaterialization(
      'default.node1',
      'mat1',
      'v1.0',
    );
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/nodes/default.node1/materializations?materialization_name=mat1&node_version=v1.0',
      expect.objectContaining({ method: 'DELETE' }),
    );
  });

  it('calls deleteMaterialization without nodeVersion correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'Success' }));
    await DataJunctionAPI.deleteMaterialization('default.node1', 'mat1');
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/nodes/default.node1/materializations?materialization_name=mat1',
      expect.objectContaining({ method: 'DELETE' }),
    );
  });

  // Notifications
  it('calls getNotificationPreferences correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify([]));
    await DataJunctionAPI.getNotificationPreferences({ entity_type: 'node' });
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/notifications/?entity_type=node',
      expect.objectContaining({ credentials: 'include' }),
    );
  });

  it('calls subscribeToNotifications correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ status: 'subscribed' }));
    await DataJunctionAPI.subscribeToNotifications({
      entity_type: 'node',
      entity_name: 'default.node1',
      activity_types: ['create'],
      alert_types: ['email'],
    });
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/notifications/subscribe',
      expect.objectContaining({
        method: 'POST',
        body: expect.any(String),
      }),
    );
  });

  it('calls unsubscribeFromNotifications correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ status: 'unsubscribed' }));
    await DataJunctionAPI.unsubscribeFromNotifications({
      entity_type: 'node',
      entity_name: 'default.node1',
    });
    expect(fetch).toHaveBeenCalledWith(
      expect.stringContaining('/notifications/unsubscribe'),
      expect.objectContaining({ method: 'DELETE' }),
    );
  });

  // materializeCube (lines 1323-1339)
  it('calls materializeCube correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'Success' }));
    const result = await DataJunctionAPI.materializeCube(
      'default.cube1',
      'spark_sql',
      'incremental_time',
      '0 0 * * *',
      '1 day',
    );
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/nodes/default.cube1/materialization',
      expect.objectContaining({
        method: 'POST',
        body: expect.stringContaining('lookback_window'),
      }),
    );
    expect(result).toEqual({ status: 200, json: { message: 'Success' } });
  });

  // runBackfill
  it('calls runBackfill correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'Success' }));
    const result = await DataJunctionAPI.runBackfill('default.node1', 'mat1', [
      {
        columnName: 'date',
        range: ['2023-01-01', '2023-12-31'],
        values: [],
      },
    ]);
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/nodes/default.node1/materializations/mat1/backfill',
      expect.objectContaining({
        method: 'POST',
        body: expect.stringContaining('column_name'),
      }),
    );
    expect(result).toEqual({ status: 200, json: { message: 'Success' } });
  });

  // addReferenceDimensionLink with role parameter
  it('calls addReferenceDimensionLink with role correctly', async () => {
    const mockResponse = { message: 'Success' };
    fetch.mockResponseOnce(JSON.stringify(mockResponse));
    const result = await DataJunctionAPI.addReferenceDimensionLink(
      'default.node1',
      'column1',
      'default.dimension1',
      'dimension_col',
      'birth_date',
    );
    expect(fetch).toHaveBeenCalledWith(
      expect.stringContaining('role=birth_date'),
      expect.objectContaining({
        method: 'POST',
      }),
    );
    expect(result).toEqual({ status: 200, json: mockResponse });
  });

  // Test getNotificationPreferences without params
  it('calls getNotificationPreferences without params correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify([]));
    await DataJunctionAPI.getNotificationPreferences();
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/notifications/',
      expect.objectContaining({ credentials: 'include' }),
    );
  });

  // Test listMetricMetadata
  it('calls listMetricMetadata correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify([{ name: 'metric1' }]));
    const result = await DataJunctionAPI.listMetricMetadata();
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/metrics/metadata',
      expect.objectContaining({
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      }),
    );
    expect(result).toEqual([{ name: 'metric1' }]);
  });

  // Test materializationInfo
  it('calls materializationInfo correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ info: 'test' }));
    const result = await DataJunctionAPI.materializationInfo();
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/materialization/info',
      expect.objectContaining({ credentials: 'include' }),
    );
    expect(result).toEqual({ info: 'test' });
  });

  // Test revalidate
  it('calls revalidate correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ status: 'valid' }));
    const result = await DataJunctionAPI.revalidate('default.node1');
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/nodes/default.node1/validate',
      expect.objectContaining({
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
      }),
    );
    expect(result).toEqual({ status: 'valid' });
  });

  // Test getMetric (lines 322-360) - this is a GraphQL query
  it('calls getMetric correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        data: {
          findNodes: [
            {
              name: 'system.test.metric',
              current: {
                metricMetadata: { direction: 'higher_is_better' },
              },
            },
          ],
        },
      }),
    );

    const result = await DataJunctionAPI.getMetric('system.test.metric');
    expect(result).toHaveProperty('name');
  });

  // Test system metrics APIs
  it('calls system.node_counts_by_active correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([
        [
          { col: 'system.dj.is_active.active_id', value: true },
          { col: 'system.dj.number_of_nodes', value: 100 },
        ],
      ]),
    );

    const result = await DataJunctionAPI.system.node_counts_by_active();
    expect(result).toEqual([{ name: 'true', value: 100 }]);
  });

  it('calls system.node_counts_by_type correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([
        [
          { col: 'system.dj.node_type.type', value: 'metric' },
          { col: 'system.dj.number_of_nodes', value: 50 },
        ],
      ]),
    );

    const result = await DataJunctionAPI.system.node_counts_by_type();
    expect(result).toEqual([{ name: 'metric', value: 50 }]);
  });

  it('calls system.node_counts_by_status correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([
        [
          { col: 'system.dj.nodes.status', value: 'valid' },
          { col: 'system.dj.number_of_nodes', value: 80 },
        ],
      ]),
    );

    const result = await DataJunctionAPI.system.node_counts_by_status();
    expect(result).toEqual([{ name: 'valid', value: 80 }]);
  });

  it('calls system.nodes_without_description correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([
        [
          { col: 'system.dj.node_type.type', value: 'transform' },
          { col: 'system.dj.node_without_description', value: 10 },
        ],
      ]),
    );

    const result = await DataJunctionAPI.system.nodes_without_description();
    expect(result).toEqual([{ name: 'transform', value: 10 }]);
  });

  it('calls system.materialization_counts_by_type correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([
        [
          { col: 'system.dj.node_type.type', value: 'cube' },
          { col: 'system.dj.number_of_materializations', value: 5 },
        ],
      ]),
    );

    const result =
      await DataJunctionAPI.system.materialization_counts_by_type();
    expect(result).toEqual([{ name: 'cube', value: 5 }]);
  });

  it('calls system.dimensions correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify(['dim1', 'dim2']));

    const result = await DataJunctionAPI.system.dimensions();
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/system/dimensions',
      expect.objectContaining({ credentials: 'include' }),
    );
    expect(result).toEqual(['dim1', 'dim2']);
  });

  it('calls system.node_trends correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([
        [
          { col: 'system.dj.nodes.created_at_week', value: 20240101 },
          { col: 'system.dj.node_type.type', value: 'metric' },
          { col: 'system.dj.number_of_nodes', value: 10 },
        ],
      ]),
    );

    const result = await DataJunctionAPI.system.node_trends();
    expect(result.length).toBeGreaterThan(0);
    expect(result[0]).toHaveProperty('date');
    expect(result[0]).toHaveProperty('metric');
  });

  // Test querySystemMetricSingleDimension edge cases
  it('handles missing values in querySystemMetricSingleDimension', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([
        [
          { col: 'some_dimension', value: null },
          { col: 'some_metric', value: undefined },
        ],
      ]),
    );

    const result = await DataJunctionAPI.querySystemMetricSingleDimension({
      metric: 'some_metric',
      dimension: 'some_dimension',
    });

    expect(result[0].name).toBe('unknown');
    expect(result[0].value).toBe(0);
  });

  // Test getNodeForEditing (lines 266-319)
  it('calls getNodeForEditing correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        data: {
          findNodes: [
            {
              name: 'default.node1',
              type: 'transform',
              current: {
                displayName: 'Node 1',
                description: 'Test node',
                query: 'SELECT * FROM table',
              },
            },
          ],
        },
      }),
    );

    const result = await DataJunctionAPI.getNodeForEditing('default.node1');
    expect(result).toHaveProperty('name', 'default.node1');
  });

  it('returns null when getNodeForEditing finds no nodes', async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        data: {
          findNodes: [],
        },
      }),
    );

    const result = await DataJunctionAPI.getNodeForEditing('nonexistent');
    expect(result).toBeNull();
  });

  // Test getCubeForEditing (lines 363-410)
  it('calls getCubeForEditing correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        data: {
          findNodes: [
            {
              name: 'default.cube1',
              type: 'cube',
              current: {
                displayName: 'Cube 1',
                description: 'Test cube',
                cubeMetrics: [{ name: 'metric1' }],
                cubeDimensions: [{ name: 'dim1', attribute: 'attr1' }],
              },
            },
          ],
        },
      }),
    );

    const result = await DataJunctionAPI.getCubeForEditing('default.cube1');
    expect(result).toHaveProperty('name', 'default.cube1');
    expect(result.current.cubeMetrics).toHaveLength(1);
  });

  it('returns null when getCubeForEditing finds no nodes', async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        data: {
          findNodes: [],
        },
      }),
    );

    const result = await DataJunctionAPI.getCubeForEditing('nonexistent');
    expect(result).toBeNull();
  });

  // Test logout (lines 225-230)
  it('calls logout correctly', async () => {
    fetch.mockResponseOnce('', { status: 200 });

    await DataJunctionAPI.logout();
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/logout/',
      expect.objectContaining({
        method: 'POST',
        credentials: 'include',
      }),
    );
  });

  // Test catalogs (lines 232-238)
  it('calls catalogs correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([{ name: 'catalog1' }, { name: 'catalog2' }]),
    );

    const result = await DataJunctionAPI.catalogs();
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/catalogs',
      expect.objectContaining({ credentials: 'include' }),
    );
    expect(result).toHaveLength(2);
  });

  // Test sql with array filters (lines 755-756)
  it('handles array filters in sql correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ sql: 'SELECT * FROM table' }));

    await DataJunctionAPI.sql('default.metric1', {
      dimensions: ['dim1', 'dim2'],
      filters: ['filter1', 'filter2'],
    });

    const callUrl = fetch.mock.calls[0][0];
    expect(callUrl).toContain('dimensions=dim1');
    expect(callUrl).toContain('filters=filter1');
  });

  // Test nodeData with null selection (lines 831-835)
  it('handles null selection in nodeData', async () => {
    fetch.mockResponseOnce(JSON.stringify({ data: [] }));

    await DataJunctionAPI.nodeData('default.node1', null);

    const callUrl = fetch.mock.calls[0][0];
    expect(callUrl).toContain('limit=1000');
    expect(callUrl).toContain('async_=true');
  });

  // Test streamNodeData with null selection (lines 889-893) - just test it returns EventSource
  it('handles null selection in streamNodeData', async () => {
    const eventSource = await DataJunctionAPI.streamNodeData(
      'default.node1',
      null,
    );

    expect(eventSource).toBeInstanceOf(EventSource);

    // EventSource mock might not have close method in test environment
    if (typeof eventSource.close === 'function') {
      eventSource.close();
    }
  });

  // Test setColumnDescription (lines 1027-1036)
  it('calls setColumnDescription correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'success' }));

    await DataJunctionAPI.setColumnDescription(
      'default.node1',
      'column1',
      'New description',
    );

    expect(fetch).toHaveBeenCalledWith(
      expect.stringContaining(
        '/nodes/default.node1/columns/column1/description',
      ),
      expect.objectContaining({
        method: 'PATCH',
        credentials: 'include',
      }),
    );
  });

  // Test nodeDimensions (lines 1046-1050)
  it('calls nodeDimensions correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify(['dim1', 'dim2']));

    const result = await DataJunctionAPI.nodeDimensions('default.node1');

    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/nodes/default.node1/dimensions',
      expect.objectContaining({ credentials: 'include' }),
    );
    expect(result).toEqual(['dim1', 'dim2']);
  });

  // Test users (lines 1194-1198)
  it('calls users correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify([{ username: 'user1' }, { username: 'user2' }]),
    );

    const result = await DataJunctionAPI.users();

    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/users?with_activity=true',
      expect.objectContaining({ credentials: 'include' }),
    );
    expect(result).toHaveLength(2);
  });

  // Test listCubesForPreset (lines 121-155)
  it('calls listCubesForPreset correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        data: {
          findNodes: [
            { name: 'default.cube1', current: { displayName: 'Cube 1' } },
            { name: 'default.cube2', current: { displayName: null } },
          ],
        },
      }),
    );

    const result = await DataJunctionAPI.listCubesForPreset();
    expect(fetch).toHaveBeenCalledWith(
      expect.stringContaining('/graphql'),
      expect.objectContaining({
        method: 'POST',
        credentials: 'include',
      }),
    );
    expect(result).toEqual([
      { name: 'default.cube1', display_name: 'Cube 1' },
      { name: 'default.cube2', display_name: null },
    ]);
  });

  it('handles listCubesForPreset error gracefully', async () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
    fetch.mockRejectOnce(new Error('Network error'));

    const result = await DataJunctionAPI.listCubesForPreset();
    expect(result).toEqual([]);
    consoleSpy.mockRestore();
  });

  // Test cubeForPlanner (lines 159-233)
  it('calls cubeForPlanner correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        data: {
          findNodes: [
            {
              name: 'default.cube1',
              current: {
                displayName: 'Cube 1',
                cubeMetrics: [{ name: 'metric1' }, { name: 'metric2' }],
                cubeDimensions: [{ name: 'dim1' }],
                materializations: [
                  {
                    name: 'druid_cube',
                    strategy: 'incremental_time',
                    schedule: '0 6 * * *',
                    config: {
                      lookback_window: '1 DAY',
                      druid_datasource: 'ds1',
                      preagg_tables: ['table1'],
                      workflow_urls: ['http://workflow.url'],
                      timestamp_column: 'ts',
                      timestamp_format: 'yyyy-MM-dd',
                    },
                  },
                ],
              },
            },
          ],
        },
      }),
    );

    const result = await DataJunctionAPI.cubeForPlanner('default.cube1');
    expect(result).toEqual({
      name: 'default.cube1',
      display_name: 'Cube 1',
      cube_node_metrics: ['metric1', 'metric2'],
      cube_node_dimensions: ['dim1'],
      cubeMaterialization: {
        strategy: 'incremental_time',
        schedule: '0 6 * * *',
        lookbackWindow: '1 DAY',
        druidDatasource: 'ds1',
        preaggTables: ['table1'],
        workflowUrls: ['http://workflow.url'],
        timestampColumn: 'ts',
        timestampFormat: 'yyyy-MM-dd',
      },
      availability: null,
    });
  });

  it('returns null for cubeForPlanner when cube not found', async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        data: { findNodes: [] },
      }),
    );

    const result = await DataJunctionAPI.cubeForPlanner('nonexistent');
    expect(result).toBeNull();
  });

  it('handles cubeForPlanner without druid materialization', async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        data: {
          findNodes: [
            {
              name: 'default.cube1',
              current: {
                displayName: 'Cube 1',
                cubeMetrics: [{ name: 'metric1' }],
                cubeDimensions: [],
                materializations: [],
              },
            },
          ],
        },
      }),
    );

    const result = await DataJunctionAPI.cubeForPlanner('default.cube1');
    expect(result.cubeMaterialization).toBeNull();
  });

  it('handles cubeForPlanner error gracefully', async () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
    fetch.mockRejectOnce(new Error('Network error'));

    const result = await DataJunctionAPI.cubeForPlanner('default.cube1');
    expect(result).toBeNull();
    consoleSpy.mockRestore();
  });

  // Test getNodesByNames (lines 460-493)
  it('calls getNodesByNames correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        data: {
          findNodes: [
            {
              name: 'default.node1',
              type: 'METRIC',
              current: {
                displayName: 'Node 1',
                status: 'VALID',
                mode: 'PUBLISHED',
              },
            },
          ],
        },
      }),
    );

    const result = await DataJunctionAPI.getNodesByNames(['default.node1']);
    expect(result).toHaveLength(1);
    expect(result[0].name).toBe('default.node1');
  });

  it('returns empty array for getNodesByNames with empty input', async () => {
    const result = await DataJunctionAPI.getNodesByNames([]);
    expect(result).toEqual([]);
  });

  it('returns empty array for getNodesByNames with null input', async () => {
    const result = await DataJunctionAPI.getNodesByNames(null);
    expect(result).toEqual([]);
  });

  // Test getNodeColumnsWithPartitions (lines 880-926)
  it('calls getNodeColumnsWithPartitions correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        data: {
          findNodes: [
            {
              name: 'default.node1',
              current: {
                columns: [
                  {
                    name: 'id',
                    type: 'int',
                    partition: null,
                  },
                  {
                    name: 'date_col',
                    type: 'date',
                    partition: {
                      type_: 'TEMPORAL',
                      format: 'yyyyMMdd',
                      granularity: 'day',
                    },
                  },
                ],
              },
            },
          ],
        },
      }),
    );

    const result = await DataJunctionAPI.getNodeColumnsWithPartitions(
      'default.node1',
    );
    expect(result.columns).toHaveLength(2);
    expect(result.temporalPartitions).toHaveLength(1);
    expect(result.temporalPartitions[0].name).toBe('date_col');
  });

  it('returns empty for getNodeColumnsWithPartitions when node not found', async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        data: { findNodes: [] },
      }),
    );

    const result = await DataJunctionAPI.getNodeColumnsWithPartitions(
      'nonexistent',
    );
    expect(result).toEqual({ columns: [], temporalPartitions: [] });
  });

  it('handles getNodeColumnsWithPartitions error gracefully', async () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
    fetch.mockRejectOnce(new Error('Network error'));

    const result = await DataJunctionAPI.getNodeColumnsWithPartitions(
      'default.node1',
    );
    expect(result).toEqual({ columns: [], temporalPartitions: [] });
    consoleSpy.mockRestore();
  });

  // Test measuresV3 (lines 1085-1103)
  it('calls measuresV3 correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ preaggs: [] }));
    await DataJunctionAPI.measuresV3(['metric1'], ['dim1'], 'filter=value');
    expect(fetch).toHaveBeenCalledWith(
      expect.stringContaining('/sql/measures/v3/?'),
      expect.objectContaining({ credentials: 'include' }),
    );
    const url = fetch.mock.calls[0][0];
    expect(url).toContain('metrics=metric1');
    expect(url).toContain('dimensions=dim1');
    expect(url).toContain('filters=filter');
  });

  it('calls measuresV3 without filters', async () => {
    fetch.mockResponseOnce(JSON.stringify({ preaggs: [] }));
    await DataJunctionAPI.measuresV3(['metric1'], ['dim1']);
    const url = fetch.mock.calls[0][0];
    expect(url).not.toContain('filters=');
  });

  // Test metricsV3 (lines 1106-1124)
  it('calls metricsV3 correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ sql: 'SELECT ...' }));
    await DataJunctionAPI.metricsV3(['metric1'], ['dim1'], 'filter=value');
    expect(fetch).toHaveBeenCalledWith(
      expect.stringContaining('/sql/metrics/v3/?'),
      expect.objectContaining({ credentials: 'include' }),
    );
    const url = fetch.mock.calls[0][0];
    expect(url).toContain('metrics=metric1');
    expect(url).toContain('dimensions=dim1');
    expect(url).toContain('filters=filter');
  });

  it('calls metricsV3 without filters', async () => {
    fetch.mockResponseOnce(JSON.stringify({ sql: 'SELECT ...' }));
    await DataJunctionAPI.metricsV3(['metric1'], ['dim1']);
    const url = fetch.mock.calls[0][0];
    expect(url).not.toContain('filters=');
  });

  // Test materializeCubeV2 (lines 1649-1671)
  it('calls materializeCubeV2 correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'Success' }));
    const result = await DataJunctionAPI.materializeCubeV2(
      'default.cube1',
      '0 6 * * *',
      'incremental_time',
      '1 DAY',
      true,
    );
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/cubes/default.cube1/materialize',
      expect.objectContaining({
        method: 'POST',
        body: expect.stringContaining('schedule'),
      }),
    );
    expect(result).toEqual({ status: 200, json: { message: 'Success' } });
  });

  // Test listPreaggs (lines 1845-1858)
  it('calls listPreaggs correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify([{ id: 1, name: 'preagg1' }]));
    const result = await DataJunctionAPI.listPreaggs({ node_name: 'node1' });
    expect(fetch).toHaveBeenCalledWith(
      expect.stringContaining('/preaggs/?node_name=node1'),
      expect.objectContaining({ credentials: 'include' }),
    );
    expect(result).toHaveLength(1);
  });

  // Test planPreaggs (lines 1861-1896)
  it('calls planPreaggs correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ preaggs: [] }));
    const result = await DataJunctionAPI.planPreaggs(
      ['metric1'],
      ['dim1'],
      'full',
      '0 6 * * *',
      '1 DAY',
    );
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/preaggs/plan',
      expect.objectContaining({
        method: 'POST',
        body: expect.stringContaining('metrics'),
      }),
    );
  });

  it('handles planPreaggs error', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'Error' }), {
      status: 400,
    });
    const result = await DataJunctionAPI.planPreaggs(['metric1'], ['dim1']);
    expect(result._error).toBe(true);
    expect(result._status).toBe(400);
  });

  // Test getPreagg (lines 1899-1905)
  it('calls getPreagg correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ id: 1, name: 'preagg1' }));
    const result = await DataJunctionAPI.getPreagg(1);
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/preaggs/1',
      expect.objectContaining({ credentials: 'include' }),
    );
    expect(result.id).toBe(1);
  });

  // Test materializePreagg (lines 1908-1927)
  it('calls materializePreagg correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'Success' }));
    const result = await DataJunctionAPI.materializePreagg(1);
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/preaggs/1/materialize',
      expect.objectContaining({ method: 'POST' }),
    );
  });

  it('handles materializePreagg error', async () => {
    fetch.mockResponseOnce(JSON.stringify({ detail: 'Error' }), {
      status: 500,
    });
    const result = await DataJunctionAPI.materializePreagg(1);
    expect(result._error).toBe(true);
  });

  // Test updatePreaggConfig (lines 1930-1959)
  it('calls updatePreaggConfig correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'Updated' }));
    const result = await DataJunctionAPI.updatePreaggConfig(
      1,
      'incremental_time',
      '0 6 * * *',
      '2 DAY',
    );
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/preaggs/1/config',
      expect.objectContaining({ method: 'PATCH' }),
    );
  });

  it('handles updatePreaggConfig error', async () => {
    fetch.mockResponseOnce(JSON.stringify({ detail: 'Error' }), {
      status: 400,
    });
    const result = await DataJunctionAPI.updatePreaggConfig(1, 'full');
    expect(result._error).toBe(true);
  });

  // Test deactivatePreaggWorkflow (lines 1962-1978)
  it('calls deactivatePreaggWorkflow correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'Deactivated' }));
    const result = await DataJunctionAPI.deactivatePreaggWorkflow(1);
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/preaggs/1/workflow',
      expect.objectContaining({ method: 'DELETE' }),
    );
  });

  it('handles deactivatePreaggWorkflow error', async () => {
    fetch.mockResponseOnce(JSON.stringify({ detail: 'Error' }), {
      status: 500,
    });
    const result = await DataJunctionAPI.deactivatePreaggWorkflow(1);
    expect(result._error).toBe(true);
  });

  // Test runPreaggBackfill (lines 1981-2005)
  it('calls runPreaggBackfill correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'Backfill started' }));
    const result = await DataJunctionAPI.runPreaggBackfill(
      1,
      '2024-01-01',
      '2024-12-31',
    );
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/preaggs/1/backfill',
      expect.objectContaining({
        method: 'POST',
        body: expect.stringContaining('start_date'),
      }),
    );
  });

  it('handles runPreaggBackfill error', async () => {
    fetch.mockResponseOnce(JSON.stringify({ detail: 'Error' }), {
      status: 500,
    });
    const result = await DataJunctionAPI.runPreaggBackfill(1, '2024-01-01');
    expect(result._error).toBe(true);
  });

  // Test getCubeDetails (lines 2008-2016)
  it('calls getCubeDetails correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ name: 'cube1' }));
    const result = await DataJunctionAPI.getCubeDetails('default.cube1');
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/cubes/default.cube1',
      expect.objectContaining({ credentials: 'include' }),
    );
    expect(result.status).toBe(200);
    expect(result.json).toEqual({ name: 'cube1' });
  });

  it('handles getCubeDetails error', async () => {
    fetch.mockResponseOnce('Not found', { status: 404 });
    const result = await DataJunctionAPI.getCubeDetails('nonexistent');
    expect(result.status).toBe(404);
    expect(result.json).toBeNull();
  });

  // Test getCubeWorkflowUrls (lines 2019-2044)
  it('calls getCubeWorkflowUrls correctly', async () => {
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
    fetch.mockResponseOnce(
      JSON.stringify({
        name: 'cube1',
        materializations: [
          {
            name: 'druid_cube',
            config: { workflow_urls: ['http://url1', 'http://url2'] },
          },
        ],
      }),
    );

    const result = await DataJunctionAPI.getCubeWorkflowUrls('default.cube1');
    expect(result).toEqual(['http://url1', 'http://url2']);
    consoleSpy.mockRestore();
  });

  it('returns empty array for getCubeWorkflowUrls when no materializations', async () => {
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
    fetch.mockResponseOnce(
      JSON.stringify({ name: 'cube1', materializations: [] }),
    );
    const result = await DataJunctionAPI.getCubeWorkflowUrls('default.cube1');
    expect(result).toEqual([]);
    consoleSpy.mockRestore();
  });

  // Test getCubeMaterialization (lines 2047-2070)
  it('calls getCubeMaterialization correctly', async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        materializations: [
          {
            id: 1,
            name: 'druid_cube',
            strategy: 'incremental_time',
            schedule: '0 6 * * *',
            lookback_window: '1 DAY',
            config: {
              druid_datasource: 'ds1',
              preagg_tables: ['table1'],
              workflow_urls: ['http://url'],
              timestamp_column: 'ts',
              timestamp_format: 'yyyy-MM-dd',
            },
          },
        ],
      }),
    );

    const result = await DataJunctionAPI.getCubeMaterialization(
      'default.cube1',
    );
    expect(result).toHaveProperty('strategy', 'incremental_time');
    expect(result).toHaveProperty('druidDatasource', 'ds1');
  });

  it('returns null for getCubeMaterialization when no druid_cube', async () => {
    fetch.mockResponseOnce(JSON.stringify({ materializations: [] }));
    const result = await DataJunctionAPI.getCubeMaterialization(
      'default.cube1',
    );
    expect(result).toBeNull();
  });

  // Test refreshCubeWorkflow (lines 2073-2087)
  it('calls refreshCubeWorkflow correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'Refreshed' }));
    const result = await DataJunctionAPI.refreshCubeWorkflow(
      'default.cube1',
      '0 6 * * *',
      'incremental_time',
      '1 DAY',
    );
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/cubes/default.cube1/materialize',
      expect.objectContaining({ method: 'POST' }),
    );
  });

  // Test deactivateCubeWorkflow (lines 2090-2109)
  it('calls deactivateCubeWorkflow correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'Deactivated' }));
    const result = await DataJunctionAPI.deactivateCubeWorkflow(
      'default.cube1',
    );
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/cubes/default.cube1/materialize',
      expect.objectContaining({ method: 'DELETE' }),
    );
    expect(result.status).toBe(200);
  });

  it('handles deactivateCubeWorkflow error', async () => {
    fetch.mockResponseOnce(JSON.stringify({ detail: 'Error' }), {
      status: 500,
    });
    const result = await DataJunctionAPI.deactivateCubeWorkflow(
      'default.cube1',
    );
    expect(result.status).toBe(500);
    expect(result.json.message).toBeDefined();
  });

  // Test runCubeBackfill (lines 2112-2137)
  it('calls runCubeBackfill correctly', async () => {
    fetch.mockResponseOnce(JSON.stringify({ message: 'Backfill started' }));
    const result = await DataJunctionAPI.runCubeBackfill(
      'default.cube1',
      '2024-01-01',
      '2024-12-31',
    );
    expect(fetch).toHaveBeenCalledWith(
      'http://localhost:8000/cubes/default.cube1/backfill',
      expect.objectContaining({
        method: 'POST',
        body: expect.stringContaining('start_date'),
      }),
    );
  });

  it('handles runCubeBackfill error', async () => {
    fetch.mockResponseOnce(JSON.stringify({ detail: 'Error' }), {
      status: 500,
    });
    const result = await DataJunctionAPI.runCubeBackfill(
      'default.cube1',
      '2024-01-01',
    );
    expect(result._error).toBe(true);
    expect(result.message).toBeDefined();
  });
});
