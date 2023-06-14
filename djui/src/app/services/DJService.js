import { MarkerType } from 'reactflow';

const DJ_URL = 'http://localhost:8000'; //process.env.REACT_APP_DJ_URL;

export const DataJunctionAPI = {
  node: async function (name) {
    const data = await (await fetch(DJ_URL + '/nodes/' + name + '/')).json();
    return data;
  },

  upstreams: async function (name) {
    const data = await (
      await fetch(DJ_URL + '/nodes/' + name + '/upstream/')
    ).json();
    return data;
  },

  downstreams: async function (name) {
    const data = await (
      await fetch(DJ_URL + '/nodes/' + name + '/downstream/')
    ).json();
    return data;
  },

  metric: async function (name) {
    const data = await (await fetch(DJ_URL + '/metrics/' + name + '/')).json();
    return data;
  },

  history: async function (type, name, offset, limit) {
    const data = await (
      await fetch(
        DJ_URL +
          '/history/' +
          type +
          '/' +
          name +
          `/?offset=${offset ? offset : 0}&limit=${limit ? limit : 100}`,
      )
    ).json();
    return data;
  },

  namespace: async function (nmspce) {
    const data = await (
      await fetch(DJ_URL + '/namespaces/' + nmspce + '/')
    ).json();
    return data;
  },

  namespaces: async function () {
    const data = await (await fetch(DJ_URL + '/namespaces/')).json();
    return data;
  },

  sql: async function (metric_name, selection) {
    console.log('selection', selection);
    const data = await (
      await fetch(
        DJ_URL + '/sql/' + metric_name + '?' + new URLSearchParams(selection),
      )
    ).json();
    return data;
  },

  materializations: async function (node) {
    const data = await (
      await fetch(DJ_URL + `/nodes/${node}/materializations/`)
    ).json();
    return data;
  },

  compiledSql: async function (node) {
    const data = await (await fetch(DJ_URL + `/sql/${node}/`)).json();
    return data;
  },

  dag: async function (namespace = 'default') {
    const edges = [];
    const data = await (await fetch(DJ_URL + '/nodes/')).json();

    // const metrics = await (await fetch(DJ_URL + '/metrics/')).json();

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
