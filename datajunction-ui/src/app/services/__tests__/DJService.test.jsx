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
        dimensionNode: dimensionNode,
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
        dimensionNode: dimensionNode,
        joinType: null,
        joinOn: joinOn,
        joinCardinality: null,
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
});
