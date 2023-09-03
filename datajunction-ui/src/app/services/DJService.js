import { MarkerType } from 'reactflow';

const DJ_URL = process.env.REACT_APP_DJ_URL
  ? process.env.REACT_APP_DJ_URL
  : 'http://localhost:8000';

export const DataJunctionAPI = {
  whoami: async function () {
    const data = await (
      await fetch(`${DJ_URL}/whoami/`, { credentials: 'include' })
    ).json();
    return data;
  },

  logout: async function () {
    await await fetch(`${DJ_URL}/basic/logout/`, {
      credentials: 'include',
      method: 'POST',
    });
  },

  node: async function (name) {
    const data = await (
      await fetch(`${DJ_URL}/nodes/${name}/`, {
        credentials: 'include',
      })
    ).json();
    data.primary_key = data.columns
      .filter(col =>
        col.attributes.some(attr => attr.attribute_type.name === 'primary_key'),
      )
      .map(col => col.name);
    return data;
  },

  createNode: async function (
    nodeType,
    name,
    display_name,
    description,
    query,
    mode,
    namespace,
  ) {
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
      }),
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
  },

  upstreams: async function (name) {
    const data = await (
      await fetch(`${DJ_URL}/nodes/${name}/upstream/`, {
        credentials: 'include',
      })
    ).json();
    return data;
  },

  downstreams: async function (name) {
    const data = await (
      await fetch(`${DJ_URL}/nodes/` + name + '/downstream/', {
        credentials: 'include',
      })
    ).json();
    return data;
  },

  node_dag: async function (name) {
    const data = await (
      await fetch(`${DJ_URL}/nodes/` + name + '/dag/', {
        credentials: 'include',
      })
    ).json();
    return data;
  },

  node_lineage: async function (name) {
    const data = await (
      await fetch(`${DJ_URL}/nodes/` + name + '/lineage/', {
        credentials: 'include',
      })
    ).json();
    return data;
  },

  metric: async function (name) {
    const data = await (
      await fetch(`${DJ_URL}/metrics/` + name + '/', {
        credentials: 'include',
      })
    ).json();
    return data;
  },

  clientCode: async function (name) {
    const data = await (
      await fetch(`${DJ_URL}/datajunction-clients/python/new_node/` + name, {
        credentials: 'include',
      })
    ).json();
    return data;
  },

  cube: async function (name) {
    const data = await (
      await fetch(`${DJ_URL}/cubes/` + name + '/', {
        credentials: 'include',
      })
    ).json();
    return data;
  },

  metrics: async function (name) {
    const data = await (
      await fetch(`${DJ_URL}/metrics/`, {
        credentials: 'include',
      })
    ).json();
    return data;
  },

  commonDimensions: async function (metrics) {
    const metricsQuery = '?' + metrics.map(m => `metric=${m}`).join('&');
    const data = await (
      await fetch(`${DJ_URL}/metrics/common/dimensions/` + metricsQuery, {
        credentials: 'include',
      })
    ).json();
    return data;
  },

  history: async function (type, name, offset, limit) {
    const data = await (
      await fetch(
        `${DJ_URL}/history?node=` +
          name +
          `&offset=${offset ? offset : 0}&limit=${limit ? limit : 100}`,
        {
          credentials: 'include',
        },
      )
    ).json();
    return data;
  },

  revisions: async function (name) {
    const data = await (
      await fetch(`${DJ_URL}/nodes/` + name + '/revisions/', {
        credentials: 'include',
      })
    ).json();
    return data;
  },

  namespace: async function (nmspce) {
    const data = await (
      await fetch(`${DJ_URL}/namespaces/` + nmspce + '/', {
        credentials: 'include',
      })
    ).json();
    return data;
  },

  namespaces: async function () {
    const data = await (
      await fetch(`${DJ_URL}/namespaces/`, {
        credentials: 'include',
      })
    ).json();
    return data;
  },

  sql: async function (metric_name, selection) {
    const data = await (
      await fetch(
        `${DJ_URL}/sql/` + metric_name + '?' + new URLSearchParams(selection),
        {
          credentials: 'include',
        },
      )
    ).json();
    return data;
  },

  nodesWithDimension: async function (name) {
    const data = await (
      await fetch(`${DJ_URL}/dimensions/` + name + '/nodes/', {
        credentials: 'include',
      })
    ).json();
    return data;
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
    const data = await (
      await fetch(`${DJ_URL}/data/?` + params + '&limit=10000', {
        credentials: 'include',
      })
    ).json();
    return data;
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
    const data = await (
      await fetch(`${DJ_URL}/sql/${node}/`, {
        credentials: 'include',
      })
    ).json();
    return data;
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
      // const dimensions = node.type === "metric" ? metrics.filter(metric => metric.name === node.name)[0].dimensions : [];
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
          // dimensions: dimensions,
        },
        // parentNode: [node.name.split(".").slice(-2, -1)],
        // extent: 'parent',
      };
    });

    return { edges: edges, nodes: nodes, namespaces: namespaceNodes };
  },
};
