import { MarkerType } from 'reactflow';

const DJ_URL = process.env.REACT_APP_DJ_URL
  ? process.env.REACT_APP_DJ_URL
  : 'http://localhost:8000';

export const DataJunctionAPI = {
  whoami: async function () {
    return await (
      await fetch(`${DJ_URL}/whoami/`, { credentials: 'include' })
    ).json();
  },

  logout: async function () {
    await await fetch(`${DJ_URL}/logout/`, {
      credentials: 'include',
      method: 'POST',
    });
  },

  catalogs: async function () {
    return await (
      await fetch(`${DJ_URL}/catalogs`, {
        credentials: 'include',
      })
    ).json();
  },

  engines: async function () {
    return await (
      await fetch(`${DJ_URL}/engines`, {
        credentials: 'include',
      })
    ).json();
  },

  node: async function (name) {
    const data = await (
      await fetch(`${DJ_URL}/nodes/${name}/`, {
        credentials: 'include',
      })
    ).json();
    if (data.message !== undefined) {
      return data;
    }
    data.primary_key = data.columns
      .filter(col =>
        col.attributes.some(attr => attr.attribute_type.name === 'primary_key'),
      )
      .map(col => col.name);
    return data;
  },

  nodes: async function (prefix) {
    const queryParams = prefix ? `?prefix=${prefix}` : '';
    return await (
      await fetch(`${DJ_URL}/nodes/${queryParams}`, {
        credentials: 'include',
      })
    ).json();
  },

  nodeDetails: async () => {
    return await (
      await fetch(`${DJ_URL}/nodes/details/`, {
        credentials: 'include',
      })
    ).json();
  },

  createNode: async function (
    nodeType,
    name,
    display_name,
    description,
    query,
    mode,
    namespace,
    primary_key,
    metric_direction,
    metric_kind,
    metric_unit,
  ) {
    const metricMetadata =
      metric_direction || metric_kind || metric_unit
        ? {
            direction: metric_direction,
            kind: metric_kind,
            unit: metric_unit,
          }
        : null;
    const response = await fetch(`${DJ_URL}/nodes/${nodeType}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        name: name,
        display_name: display_name,
        description: description,
        query: query,
        mode: mode,
        namespace: namespace,
        primary_key: primary_key,
        metric_metadata: metricMetadata,
      }),
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
  },

  patchNode: async function (
    name,
    display_name,
    description,
    query,
    mode,
    primary_key,
    metric_direction,
    metric_kind,
    metric_unit,
  ) {
    try {
      const metricMetadata =
        metric_direction || metric_kind || metric_unit
          ? {
              direction: metric_direction,
              kind: metric_kind,
              unit: metric_unit,
            }
          : null;
      const response = await fetch(`${DJ_URL}/nodes/${name}`, {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          display_name: display_name,
          description: description,
          query: query,
          mode: mode,
          primary_key: primary_key,
          metric_metadata: metricMetadata,
        }),
        credentials: 'include',
      });
      return { status: response.status, json: await response.json() };
    } catch (error) {
      return { status: 500, json: { message: 'Update failed' } };
    }
  },

  registerTable: async function (catalog, schema, table) {
    const response = await fetch(
      `${DJ_URL}/register/table/${catalog}/${schema}/${table}`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
      },
    );
    return { status: response.status, json: await response.json() };
  },

  upstreams: async function (name) {
    return await (
      await fetch(`${DJ_URL}/nodes/${name}/upstream/`, {
        credentials: 'include',
      })
    ).json();
  },

  downstreams: async function (name) {
    return await (
      await fetch(`${DJ_URL}/nodes/${name}/downstream/`, {
        credentials: 'include',
      })
    ).json();
  },

  node_dag: async function (name) {
    return await (
      await fetch(`${DJ_URL}/nodes/${name}/dag/`, {
        credentials: 'include',
      })
    ).json();
  },

  node_lineage: async function (name) {
    return await (
      await fetch(`${DJ_URL}/nodes/${name}/lineage/`, {
        credentials: 'include',
      })
    ).json();
  },

  metric: async function (name) {
    return await (
      await fetch(`${DJ_URL}/metrics/${name}/`, {
        credentials: 'include',
      })
    ).json();
  },

  clientCode: async function (name) {
    return await (
      await fetch(`${DJ_URL}/datajunction-clients/python/new_node/${name}`, {
        credentials: 'include',
      })
    ).json();
  },

  cube: async function (name) {
    return await (
      await fetch(`${DJ_URL}/cubes/${name}/`, {
        credentials: 'include',
      })
    ).json();
  },

  metrics: async function (name) {
    return await (
      await fetch(`${DJ_URL}/metrics/`, {
        credentials: 'include',
      })
    ).json();
  },

  commonDimensions: async function (metrics) {
    const metricsQuery = '?' + metrics.map(m => `metric=${m}`).join('&');
    return await (
      await fetch(`${DJ_URL}/metrics/common/dimensions/${metricsQuery}`, {
        credentials: 'include',
      })
    ).json();
  },

  history: async function (type, name, offset, limit) {
    return await (
      await fetch(
        `${DJ_URL}/history?node=${name}&offset=${offset ? offset : 0}&limit=${
          limit ? limit : 100
        }`,
        {
          credentials: 'include',
        },
      )
    ).json();
  },

  revisions: async function (name) {
    return await (
      await fetch(`${DJ_URL}/nodes/${name}/revisions/`, {
        credentials: 'include',
      })
    ).json();
  },

  namespace: async function (nmspce) {
    return await (
      await fetch(`${DJ_URL}/namespaces/${nmspce}/`, {
        credentials: 'include',
      })
    ).json();
  },

  namespaces: async function () {
    return await (
      await fetch(`${DJ_URL}/namespaces/`, {
        credentials: 'include',
      })
    ).json();
  },

  sql: async function (metric_name, selection) {
    return await (
      await fetch(
        `${DJ_URL}/sql/${metric_name}?` + new URLSearchParams(selection),
        {
          credentials: 'include',
        },
      )
    ).json();
  },

  nodesWithDimension: async function (name) {
    return await (
      await fetch(`${DJ_URL}/dimensions/${name}/nodes/`, {
        credentials: 'include',
      })
    ).json();
  },

  materializations: async function (node) {
    const data = await (
      await fetch(`${DJ_URL}/nodes/${node}/materializations/`, {
        credentials: 'include',
      })
    ).json();

    return await Promise.all(
      data.map(async materialization => {
        materialization.clientCode = await (
          await fetch(
            `${DJ_URL}/datajunction-clients/python/add_materialization/${node}/${materialization.name}`,
            {
              credentials: 'include',
            },
          )
        ).json();
        return materialization;
      }),
    );
  },

  columns: async function (node) {
    return await Promise.all(
      node.columns.map(async col => {
        if (col.dimension) {
          col.clientCode = await (
            await fetch(
              `${DJ_URL}/datajunction-clients/python/link_dimension/${node.name}/${col.name}/${col.dimension?.name}`,
              {
                credentials: 'include',
              },
            )
          ).json();
        }
        return col;
      }),
    );
  },

  sqls: async function (metricSelection, dimensionSelection, filters) {
    const params = new URLSearchParams();
    metricSelection.map(metric => params.append('metrics', metric));
    dimensionSelection.map(dimension => params.append('dimensions', dimension));
    params.append('filters', filters);
    return await (
      await fetch(`${DJ_URL}/sql/?${params}`, {
        credentials: 'include',
      })
    ).json();
  },

  data: async function (metricSelection, dimensionSelection) {
    const params = new URLSearchParams();
    metricSelection.map(metric => params.append('metrics', metric));
    dimensionSelection.map(dimension => params.append('dimensions', dimension));
    return await (
      await fetch(`${DJ_URL}/data/?` + params + '&limit=10000', {
        credentials: 'include',
      })
    ).json();
  },

  stream: async function (metricSelection, dimensionSelection, filters) {
    const params = new URLSearchParams();
    metricSelection.map(metric => params.append('metrics', metric));
    dimensionSelection.map(dimension => params.append('dimensions', dimension));
    params.append('filters', filters);
    return new EventSource(
      `${DJ_URL}/stream/?${params}&limit=10000&async_=true`,
      {
        withCredentials: true,
      },
    );
  },

  lineage: async function (node) {},

  compiledSql: async function (node) {
    return await (
      await fetch(`${DJ_URL}/sql/${node}/`, {
        credentials: 'include',
      })
    ).json();
  },

  dag: async function (namespace = 'default') {
    const edges = [];
    const data = await (
      await fetch(`${DJ_URL}/nodes/`, {
        credentials: 'include',
      })
    ).json();

    data.forEach(obj => {
      obj.parents.forEach(parent => {
        if (parent.name) {
          edges.push({
            id: obj.name + '-' + parent.name,
            target: obj.name,
            source: parent.name,
            animated: true,
            markerEnd: {
              type: MarkerType.Arrow,
            },
          });
        }
      });

      obj.columns.forEach(col => {
        if (col.dimension) {
          edges.push({
            id: obj.name + '-' + col.dimension.name,
            target: obj.name,
            source: col.dimension.name,
            draggable: true,
          });
        }
      });
    });
    const namespaces = new Set(
      data.flatMap(node => node.name.split('.').slice(0, -1)),
    );
    const namespaceNodes = Array.from(namespaces).map(namespace => {
      return {
        id: String(namespace),
        type: 'DJNamespace',
        data: {
          label: String(namespace),
        },
      };
    });

    const nodes = data.map((node, index) => {
      const primary_key = node.columns
        .filter(col =>
          col.attributes.some(
            attr => attr.attribute_type.name === 'primary_key',
          ),
        )
        .map(col => col.name);
      const column_names = node.columns.map(col => {
        return { name: col.name, type: col.type };
      });
      return {
        id: String(node.name),
        type: 'DJNode',
        data: {
          label:
            node.table !== null
              ? String(node.schema_ + '.' + node.table)
              : String(node.name),
          table: node.table,
          name: String(node.name),
          display_name: String(node.display_name),
          type: node.type,
          primary_key: primary_key,
          column_names: column_names,
        },
      };
    });

    return { edges: edges, nodes: nodes, namespaces: namespaceNodes };
  },
  attributes: async function () {
    return await (
      await fetch(`${DJ_URL}/attributes`, {
        credentials: 'include',
      })
    ).json();
  },
  setAttributes: async function (nodeName, columnName, attributes) {
    const response = await fetch(
      `${DJ_URL}/nodes/${nodeName}/columns/${columnName}/attributes`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(
          attributes.map(attribute => {
            return {
              namespace: 'system',
              name: attribute,
            };
          }),
        ),
        credentials: 'include',
      },
    );
    return { status: response.status, json: await response.json() };
  },
  dimensions: async function () {
    return await (
      await fetch(`${DJ_URL}/dimensions`, {
        credentials: 'include',
      })
    ).json();
  },
  linkDimension: async function (nodeName, columnName, dimensionName) {
    const response = await fetch(
      `${DJ_URL}/nodes/${nodeName}/columns/${columnName}?dimension=${dimensionName}`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
      },
    );
    return { status: response.status, json: await response.json() };
  },
  unlinkDimension: async function (nodeName, columnName, dimensionName) {
    const response = await fetch(
      `${DJ_URL}/nodes/${nodeName}/columns/${columnName}?dimension=${dimensionName}`,
      {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
      },
    );
    return { status: response.status, json: await response.json() };
  },
  deactivate: async function (nodeName) {
    const response = await fetch(`${DJ_URL}/nodes/${nodeName}`, {
      method: 'DELETE',
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
  },
  addNamespace: async function (namespace) {
    const response = await fetch(`${DJ_URL}/namespaces/${namespace}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
  },
  listTags: async function () {
    const response = await fetch(`${DJ_URL}/tags`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
    });
    return await response.json();
  },
  getTag: async function (tagName) {
    const response = await fetch(`${DJ_URL}/tags/${tagName}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
    });
    return await response.json();
  },
  listNodesForTag: async function (tagName) {
    const response = await fetch(`${DJ_URL}/tags/${tagName}/nodes`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
    });
    return await response.json();
  },
  tagsNode: async function (nodeName, tagNames) {
    const url = tagNames
      .map(value => `tag_names=${encodeURIComponent(value)}`)
      .join('&');
    const response = await fetch(`${DJ_URL}/nodes/${nodeName}/tags?${url}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
  },
  addTag: async function (name, displayName, tagType, description) {
    const response = await fetch(`${DJ_URL}/tags`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        name: name,
        display_name: displayName,
        tag_type: tagType,
        description: description,
      }),
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
  },
  editTag: async function (name, description, displayName) {
    const updates = {};
    if (description) {
      updates.description = description;
    }
    if (displayName) {
      updates.display_name = displayName;
    }

    const response = await fetch(`${DJ_URL}/tags/${name}`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(updates),
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
  },
  setPartition: async function (
    nodeName,
    columnName,
    partitionType,
    format,
    granularity,
  ) {
    const body = {
      type_: partitionType,
    };
    if (format) {
      body.format = format;
    }
    if (granularity) {
      body.granularity = granularity;
    }
    const response = await fetch(
      `${DJ_URL}/nodes/${nodeName}/columns/${columnName}/partition`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
        credentials: 'include',
      },
    );
    return { status: response.status, json: await response.json() };
  },
  materialize: async function (
    nodeName,
    engineName,
    engineVersion,
    schedule,
    config,
  ) {
    const response = await fetch(
      `${DJ_URL}/nodes/${nodeName}/materialization`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          engine: {
            name: engineName,
            version: engineVersion,
          },
          schedule: schedule,
          config: JSON.parse(config),
        }),
        credentials: 'include',
      },
    );
    return { status: response.status, json: await response.json() };
  },
  runBackfill: async function (
    nodeName,
    materializationName,
    partitionColumn,
    from,
    to,
  ) {
    const response = await fetch(
      `${DJ_URL}/nodes/${nodeName}/materializations/${materializationName}/backfill`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          column_name: partitionColumn,
          range: [from, to],
        }),
        credentials: 'include',
      },
    );
    return { status: response.status, json: await response.json() };
  },
};
