// Note: MarkerType.Arrow is just the string "arrow" - we use the literal
// to avoid importing reactflow in this service (which would bloat the main bundle)
const MARKER_TYPE_ARROW = 'arrow';

const DJ_URL = process.env.REACT_APP_DJ_URL
  ? process.env.REACT_APP_DJ_URL
  : 'http://localhost:8000';

const DJ_GQL = process.env.REACT_APP_DJ_GQL
  ? process.env.REACT_APP_DJ_GQL
  : process.env.REACT_APP_DJ_URL + '/graphql';

// Export the base URL for components that need direct access
export const getDJUrl = () => DJ_URL;

export const DataJunctionAPI = {
  listNodesForLanding: async function (
    namespace,
    nodeTypes,
    tags,
    editedBy,
    before,
    after,
    limit,
    sortConfig,
    mode,
    {
      ownedBy = null,
      statuses = null,
      missingDescription = false,
      hasMaterialization = false,
      orphanedDimension = false,
    } = {},
  ) {
    const query = `
      query ListNodes($namespace: String, $nodeTypes: [NodeType!], $tags: [String!], $editedBy: String, $mode: NodeMode, $before: String, $after: String, $limit: Int, $orderBy: NodeSortField, $ascending: Boolean, $ownedBy: String, $statuses: [NodeStatus!], $missingDescription: Boolean, $hasMaterialization: Boolean, $orphanedDimension: Boolean) {
        findNodesPaginated(
          namespace: $namespace
          nodeTypes: $nodeTypes
          tags: $tags
          editedBy: $editedBy
          mode: $mode
          limit: $limit
          before: $before
          after: $after
          orderBy: $orderBy
          ascending: $ascending
          ownedBy: $ownedBy
          statuses: $statuses
          missingDescription: $missingDescription
          hasMaterialization: $hasMaterialization
          orphanedDimension: $orphanedDimension
        ) {
          pageInfo {
            hasNextPage
            endCursor
            hasPrevPage
            startCursor
          }
          edges {
            node {
              name
              type
              currentVersion
              tags {
                name
                tagType
              }
              editedBy
              current {
                displayName
                status
                mode
                updatedAt
              }
              gitInfo {
                repo
                branch
                defaultBranch
              }
            }
          }
        }
      }
    `;
    const sortOrderMapping = {
      name: 'NAME',
      displayName: 'DISPLAY_NAME',
      type: 'TYPE',
      status: 'STATUS',
      updatedAt: 'UPDATED_AT',
    };

    return await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
          query,
          variables: {
            namespace: namespace,
            nodeTypes: nodeTypes,
            tags: tags,
            editedBy: editedBy,
            mode: mode || null,
            before: before,
            after: after,
            limit: limit,
            orderBy: sortOrderMapping[sortConfig.key],
            ascending: sortConfig.direction === 'ascending',
            ownedBy: ownedBy,
            statuses: statuses,
            missingDescription: missingDescription,
            hasMaterialization: hasMaterialization,
            orphanedDimension: orphanedDimension,
          },
        }),
      })
    ).json();
  },

  // Lightweight GraphQL query for listing cubes with display names (for preset dropdown)
  listCubesForPreset: async function () {
    const query = `
      query ListCubes {
        findNodes(nodeTypes: [CUBE]) {
          name
          current {
            displayName
          }
        }
      }
    `;

    try {
      const result = await (
        await fetch(DJ_GQL, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          credentials: 'include',
          body: JSON.stringify({ query }),
        })
      ).json();

      // Transform to simple array: [{name, display_name}]
      const nodes = result?.data?.findNodes || [];
      return nodes.map(node => ({
        name: node.name,
        display_name: node.current?.displayName || null,
      }));
    } catch (err) {
      console.error('Failed to fetch cubes via GraphQL:', err);
      return [];
    }
  },

  // Lightweight GraphQL query for planner page - only fetches fields needed
  // Much faster than REST /cubes/{name}/ which loads all columns, elements, etc.
  cubeForPlanner: async function (name) {
    const query = `
      query GetCubeForPlanner($name: String!) {
        findNodes(names: [$name]) {
          name
          current {
            displayName
            cubeMetrics {
              name
            }
            cubeDimensions {
              name
            }
            availability {
              catalog
              schema_
              table
              validThroughTs
            }
            materializations {
              name
              config
              schedule
              strategy
            }
          }
        }
      }
    `;

    try {
      const result = await (
        await fetch(DJ_GQL, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          credentials: 'include',
          body: JSON.stringify({ query, variables: { name } }),
        })
      ).json();

      if (result.errors) {
        console.error('GraphQL errors:', result.errors);
        return null;
      }

      const node = result?.data?.findNodes?.[0];
      if (!node) {
        return null;
      }

      // Transform to match the shape expected by QueryPlannerPage
      const current = node.current || {};
      const cubeMetrics = (current.cubeMetrics || []).map(m => m.name);
      const cubeDimensions = (current.cubeDimensions || []).map(d => d.name);

      // Extract druid_cube materialization if present (v3 or legacy)
      const druidMat = (current.materializations || []).find(
        m => m.name === 'druid_cube' || m.name === 'druid_cube_v3',
      );
      const cubeMaterialization = druidMat
        ? {
            strategy: druidMat.strategy,
            schedule: druidMat.schedule,
            lookbackWindow: druidMat.config?.lookback_window,
            druidDatasource: druidMat.config?.druid_datasource,
            preaggTables: druidMat.config?.preagg_tables || [],
            workflowUrls: druidMat.config?.workflow_urls || [],
            timestampColumn: druidMat.config?.timestamp_column,
            timestampFormat: druidMat.config?.timestamp_format,
          }
        : null;

      return {
        name: node.name,
        display_name: current.displayName,
        cube_node_metrics: cubeMetrics,
        cube_node_dimensions: cubeDimensions,
        cubeMaterialization, // Included so we don't need a second fetch
        availability: current.availability || null,
      };
    } catch (err) {
      console.error('Failed to fetch cube via GraphQL:', err);
      return null;
    }
  },

  whoami: async function () {
    return await (
      await fetch(`${DJ_URL}/whoami/`, { credentials: 'include' })
    ).json();
  },

  querySystemMetric: async function ({
    metric,
    dimensions = [],
    filters = [],
    orderby = [],
  }) {
    const params = new URLSearchParams();
    dimensions.forEach(d => params.append('dimensions', d));
    filters.forEach(f => params.append('filters', f));
    orderby.forEach(o => params.append('orderby', o));

    const url = `${DJ_URL}/system/data/${metric}?${params.toString()}`;
    const res = await fetch(url, { credentials: 'include' });

    if (!res.ok) {
      throw new Error(`Failed to fetch metric data ${metric}: ${res.status}`);
    }
    return await res.json();
  },

  querySystemMetricSingleDimension: async function ({
    metric,
    dimension,
    filters = [],
    orderby = [],
  }) {
    const results = await DataJunctionAPI.querySystemMetric({
      metric: metric,
      dimensions: [dimension],
      filters: filters,
      orderby: orderby,
    });
    return results.map(row => {
      return {
        name:
          row.find(entry => entry.col === dimension)?.value?.toString() ??
          'unknown',
        value: row.find(entry => entry.col === metric)?.value ?? 0,
      };
    });
  },

  system: {
    node_counts_by_active: async function () {
      return DataJunctionAPI.querySystemMetricSingleDimension({
        metric: 'system.dj.number_of_nodes',
        dimension: 'system.dj.is_active.active_id',
      });
    },
    node_counts_by_type: async function () {
      return DataJunctionAPI.querySystemMetricSingleDimension({
        metric: 'system.dj.number_of_nodes',
        dimension: 'system.dj.node_type.type',
        filters: ['system.dj.is_active.active_id=true'],
        orderby: ['system.dj.node_type.type'],
      });
    },
    node_counts_by_status: async function () {
      return DataJunctionAPI.querySystemMetricSingleDimension({
        metric: 'system.dj.number_of_nodes',
        dimension: 'system.dj.nodes.status',
        filters: ['system.dj.is_active.active_id=true'],
        orderby: ['system.dj.nodes.status'],
      });
    },
    nodes_without_description: async function () {
      return DataJunctionAPI.querySystemMetricSingleDimension({
        metric: 'system.dj.node_without_description',
        dimension: 'system.dj.node_type.type',
        filters: ['system.dj.is_active.active_id=true'],
        orderby: ['system.dj.node_type.type'],
      });
    },
    node_trends: async function () {
      const results = await (
        await fetch(
          `${DJ_URL}/system/data/system.dj.number_of_nodes?dimensions=system.dj.nodes.created_at_week&dimensions=system.dj.node_type.type&filters=system.dj.nodes.created_at_week>=20240101&orderby=system.dj.nodes.created_at_week`,
          { credentials: 'include' },
        )
      ).json();
      const byDateint = {};
      results.forEach(row => {
        const dateint = row.find(
          r => r.col === 'system.dj.nodes.created_at_week',
        )?.value;
        const nodeType = row.find(
          r => r.col === 'system.dj.node_type.type',
        )?.value;
        const count = row.find(
          r => r.col === 'system.dj.number_of_nodes',
        )?.value;
        if (!byDateint[dateint]) {
          byDateint[dateint] = { date: dateint };
        }
        byDateint[dateint][nodeType] =
          (byDateint[dateint][nodeType] || 0) + count;
      });
      return Object.entries(byDateint).map(([dateint, data]) => {
        return {
          date: dateint,
          ...data,
        };
      });
    },
    materialization_counts_by_type: async function () {
      return DataJunctionAPI.querySystemMetricSingleDimension({
        metric: 'system.dj.number_of_materializations',
        dimension: 'system.dj.node_type.type',
        filters: ['system.dj.is_active.active_id=true'],
        orderby: ['system.dj.node_type.type'],
      });
    },

    dimensions: async function () {
      return await (
        await fetch(`${DJ_URL}/system/dimensions`, {
          credentials: 'include',
        })
      ).json();
    },
  },

  logout: async function () {
    return await fetch(`${DJ_URL}/logout/`, {
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

  getNodeForEditing: async function (name) {
    const query = `
      query GetNodeForEditing($name: String!) {
        findNodes (names: [$name]) {
          name
          type
          current {
            displayName
            description
            primaryKey
            query
            parents { name type }
            isDerivedMetric
            metricMetadata {
              direction
              unit { name }
              expression
              significantDigits
              incompatibleDruidFunctions
            }
            requiredDimensions {
              name
            }
            mode
            customMetadata
          }
          tags {
            name
            displayName
          }
          owners {
            username
          }
        }
      }
    `;

    const results = await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
          query,
          variables: {
            name: name,
          },
        }),
      })
    ).json();
    if (results.data.findNodes.length === 0) {
      return null;
    }
    return results.data.findNodes[0];
  },

  // Fetch basic node info for multiple nodes by name (for Settings page)
  getNodesByNames: async function (names) {
    if (!names || names.length === 0) {
      return [];
    }
    const query = `
      query GetNodesByNames($names: [String!]) {
        findNodes(names: $names) {
          name
          type
          current {
            displayName
            status
            mode
          }
        }
      }
    `;

    const results = await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
          query,
          variables: { names },
        }),
      })
    ).json();
    return results.data?.findNodes || [];
  },

  getMetric: async function (name) {
    const query = `
      query GetMetric($name: String!) {
        findNodes (names: [$name]) {
          name
          current {
            parents { name type }
            isDerivedMetric
            metricMetadata {
              direction
              unit { name }
              expression
              significantDigits
              incompatibleDruidFunctions
            }
            requiredDimensions {
              name
            }
          }
        }
      }
    `;

    const results = await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
          query,
          variables: {
            name: name,
          },
        }),
      })
    ).json();
    return results.data.findNodes[0];
  },

  getCubeForEditing: async function (name) {
    const query = `
      query GetCubeForEditing($name: String!) {
        findNodes(names: [$name]) {
          name
          type
          owners {
            username
          }
          current {
            displayName
            description
            mode
            cubeMetrics {
              name
            }
            cubeDimensions {
              name
              attribute
              properties
            }
          }
          tags {
            name
            displayName
          }
        }
      }
    `;

    const results = await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
          query,
          variables: {
            name: name,
          },
        }),
      })
    ).json();
    if (results.data.findNodes.length === 0) {
      return null;
    }
    return results.data.findNodes[0];
  },

  nodes: async function (prefix) {
    const queryParams = prefix ? `?prefix=${prefix}` : '';
    return await (
      await fetch(`${DJ_URL}/nodes/${queryParams}`, {
        credentials: 'include',
      })
    ).json();
  },

  nodesWithType: async function (nodeType) {
    return await (
      await fetch(`${DJ_URL}/nodes/?node_type=${nodeType}`, {
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

  validateNode: async function (
    nodeType,
    name,
    display_name,
    description,
    query,
  ) {
    const response = await fetch(`${DJ_URL}/nodes/validate`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        name: name,
        display_name: display_name,
        description: description,
        query: query,
        type: nodeType,
        mode: 'published',
      }),
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
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
    metric_unit,
    required_dimensions,
    custom_metadata,
  ) {
    const metricMetadata =
      metric_direction || metric_unit
        ? {
            direction: metric_direction,
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
        required_dimensions: required_dimensions,
        custom_metadata: custom_metadata,
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
    metric_unit,
    significant_digits,
    required_dimensions,
    owners,
    custom_metadata,
  ) {
    try {
      const metricMetadata =
        metric_direction || metric_unit
          ? {
              direction: metric_direction,
              unit: metric_unit,
              significant_digits: significant_digits || null,
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
          required_dimensions: required_dimensions,
          owners: owners,
          custom_metadata: custom_metadata,
        }),
        credentials: 'include',
      });
      return { status: response.status, json: await response.json() };
    } catch (error) {
      return { status: 500, json: { message: 'Update failed' } };
    }
  },

  createCube: async function (
    name,
    display_name,
    description,
    mode,
    metrics,
    dimensions,
    filters,
  ) {
    const response = await fetch(`${DJ_URL}/nodes/cube`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        name: name,
        display_name: display_name,
        description: description,
        metrics: metrics,
        dimensions: dimensions,
        filters: filters,
        mode: mode,
      }),
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
  },

  patchCube: async function (
    name,
    display_name,
    description,
    mode,
    metrics,
    dimensions,
    filters,
    owners,
  ) {
    const url = `${DJ_URL}/nodes/${name}`;
    const response = await fetch(url, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        display_name: display_name,
        description: description,
        metrics: metrics,
        dimensions: dimensions,
        filters: filters || [],
        mode: mode,
        owners: owners,
      }),
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
  },

  refreshLatestMaterialization: async function (name) {
    const url = `${DJ_URL}/nodes/${name}?refresh_materialization=true`;
    const response = await fetch(url, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
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

  // GraphQL-based upstream/downstream queries - more efficient as they only fetch needed fields
  upstreamsGQL: async function (nodeNames) {
    const names = Array.isArray(nodeNames) ? nodeNames : [nodeNames];
    const query = `
      query GetUpstreamNodes($nodeNames: [String!]!) {
        upstreamNodes(nodeNames: $nodeNames) {
          name
          type
        }
      }
    `;
    const results = await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ query, variables: { nodeNames: names } }),
      })
    ).json();
    return results.data?.upstreamNodes || [];
  },

  downstreamsGQL: async function (nodeNames) {
    const names = Array.isArray(nodeNames) ? nodeNames : [nodeNames];
    const query = `
      query GetDownstreamNodes($nodeNames: [String!]!) {
        downstreamNodes(nodeNames: $nodeNames) {
          name
          type
        }
      }
    `;
    const results = await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ query, variables: { nodeNames: names } }),
      })
    ).json();
    return results.data?.downstreamNodes || [];
  },

  node_dag: async function (name) {
    return await (
      await fetch(`${DJ_URL}/nodes/${name}/dag/`, {
        credentials: 'include',
      })
    ).json();
  },

  // Fetch node columns with partition info via GraphQL
  // Used to check if a node has temporal partitions defined
  getNodeColumnsWithPartitions: async function (nodeName) {
    const query = `
      query GetNodeColumnsWithPartitions($name: String!) {
        findNodes(names: [$name]) {
          name
          current {
            columns {
              name
              type
              partition {
                type_
                format
                granularity
              }
            }
          }
        }
      }
    `;

    try {
      const results = await (
        await fetch(DJ_GQL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify({ query, variables: { name: nodeName } }),
        })
      ).json();

      const node = results.data?.findNodes?.[0];
      if (!node) return { columns: [], temporalPartitions: [] };

      const columns = node.current?.columns || [];
      const temporalPartitions = columns.filter(
        col => col.partition?.type_ === 'TEMPORAL',
      );

      return {
        columns,
        temporalPartitions,
      };
    } catch (err) {
      console.error('Failed to fetch node columns with partitions:', err);
      return { columns: [], temporalPartitions: [] };
    }
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

  namespace: async function (nmspce, editedBy) {
    return await (
      await fetch(
        `${DJ_URL}/namespaces/${nmspce}?edited_by=${editedBy}&with_edited_by=true`,
        {
          credentials: 'include',
        },
      )
    ).json();
  },

  namespaces: async function () {
    return await (
      await fetch(`${DJ_URL}/namespaces/`, {
        credentials: 'include',
      })
    ).json();
  },

  namespaceSources: async function (namespace) {
    return await (
      await fetch(`${DJ_URL}/namespaces/${namespace}/sources`, {
        credentials: 'include',
      })
    ).json();
  },

  namespaceSourcesBulk: async function (namespaces) {
    return await (
      await fetch(`${DJ_URL}/namespaces/sources/bulk`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ namespaces }),
        credentials: 'include',
      })
    ).json();
  },

  listDeployments: async function (namespace, limit = 5) {
    const params = new URLSearchParams();
    if (namespace) {
      params.append('namespace', namespace);
    }
    params.append('limit', limit);
    return await (
      await fetch(`${DJ_URL}/deployments?${params.toString()}`, {
        credentials: 'include',
      })
    ).json();
  },

  sql: async function (metric_name, selection) {
    const params = new URLSearchParams(selection);
    for (const [key, value] of Object.entries(selection)) {
      if (Array.isArray(value)) {
        params.delete(key);
        value.forEach(v => params.append(key, v));
      }
    }

    return await (
      await fetch(`${DJ_URL}/sql/${metric_name}?${params}`, {
        credentials: 'include',
      })
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
      await fetch(
        `${DJ_URL}/nodes/${node}/materializations?show_inactive=true&include_all_revisions=true`,
        {
          credentials: 'include',
        },
      )
    ).json();

    return data;
  },

  availabilityStates: async function (node) {
    const data = await (
      await fetch(`${DJ_URL}/nodes/${node}/availability/`, {
        credentials: 'include',
      })
    ).json();

    return data;
  },

  columns: async function (node) {
    return await Promise.all(
      node.columns.map(async col => {
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

  // V3 Measures SQL - returns pre-aggregations with components for materialization planning
  measuresV3: async function (
    metricSelection,
    dimensionSelection,
    filters = '',
  ) {
    const params = new URLSearchParams();
    metricSelection.forEach(metric => params.append('metrics', metric));
    dimensionSelection.forEach(dimension =>
      params.append('dimensions', dimension),
    );
    if (filters) {
      params.append('filters', filters);
    }
    return await (
      await fetch(`${DJ_URL}/sql/measures/v3/?${params}`, {
        credentials: 'include',
      })
    ).json();
  },

  // V3 Metrics SQL - returns final combined SQL query
  metricsV3: async function (
    metricSelection,
    dimensionSelection,
    filters = '',
    useMaterialized = true,
  ) {
    const params = new URLSearchParams();
    metricSelection.forEach(metric => params.append('metrics', metric));
    dimensionSelection.forEach(dimension =>
      params.append('dimensions', dimension),
    );
    if (filters) {
      params.append('filters', filters);
    }
    if (useMaterialized) {
      params.append('use_materialized', 'true');
      params.append('dialect', 'druid');
    } else {
      params.append('use_materialized', 'false');
      params.append('dialect', 'spark');
    }
    return await (
      await fetch(`${DJ_URL}/sql/metrics/v3/?${params}`, {
        credentials: 'include',
        params: params,
      })
    ).json();
  },

  data: async function (metricSelection, dimensionSelection, filters = []) {
    const params = new URLSearchParams();
    metricSelection.map(metric => params.append('metrics', metric));
    dimensionSelection.map(dimension => params.append('dimensions', dimension));
    if (filters && filters.length > 0) {
      filters.forEach(f => params.append('filters', f));
    }
    params.append('limit', '10000');
    const response = await fetch(`${DJ_URL}/data/?${params}`, {
      credentials: 'include',
    });
    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(
        errorData.message ||
          errorData.detail ||
          `Query failed: ${response.status}`,
      );
    }
    return await response.json();
  },

  nodeData: async function (nodeName, selection = null) {
    if (selection === null) {
      selection = {
        dimensions: [],
        filters: [],
      };
    }
    const params = new URLSearchParams(selection);
    for (const [key, value] of Object.entries(selection)) {
      if (Array.isArray(value)) {
        params.delete(key);
        value.forEach(v => params.append(key, v));
      }
    }
    params.append('limit', '1000');
    params.append('async_', 'true');

    return await (
      await fetch(`${DJ_URL}/data/${nodeName}?${params}`, {
        credentials: 'include',
        headers: { 'Cache-Control': 'max-age=86400' },
      })
    ).json();
  },

  notebookExportCube: async function (cube) {
    return await fetch(
      `${DJ_URL}/datajunction-clients/python/notebook/?cube=${cube}`,
      {
        credentials: 'include',
      },
    );
  },

  notebookExportNamespace: async function (namespace) {
    return await (
      await fetch(
        `${DJ_URL}/datajunction-clients/python/notebook/?namespace=${namespace}`,
        {
          credentials: 'include',
        },
      )
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

  streamNodeData: async function (nodeName, selection = null) {
    if (selection === null) {
      selection = {
        dimensions: [],
        filters: [],
      };
    }
    const params = new URLSearchParams(selection);
    for (const [key, value] of Object.entries(selection)) {
      if (Array.isArray(value)) {
        params.delete(key);
        value.forEach(v => params.append(key, v));
      }
    }
    params.append('limit', '1000');
    params.append('async_', 'true');

    return new EventSource(`${DJ_URL}/stream/${nodeName}?${params}`, {
      withCredentials: true,
    });
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
              type: MARKER_TYPE_ARROW,
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

  setColumnDescription: async function (nodeName, columnName, description) {
    const response = await fetch(
      `${DJ_URL}/nodes/${nodeName}/columns/${columnName}/description?description=${encodeURIComponent(
        description,
      )}`,
      {
        method: 'PATCH',
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
  nodeDimensions: async function (nodeName) {
    return await (
      await fetch(`${DJ_URL}/nodes/${nodeName}/dimensions`, {
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

  addComplexDimensionLink: async function (
    nodeName,
    dimensionNode,
    joinOn,
    joinType = null,
    joinCardinality = null,
    role = null,
    defaultValue = null,
  ) {
    const response = await fetch(`${DJ_URL}/nodes/${nodeName}/link`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        dimension_node: dimensionNode,
        join_type: joinType,
        join_on: joinOn,
        join_cardinality: joinCardinality,
        role: role,
        default_value: defaultValue,
      }),
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
  },

  removeComplexDimensionLink: async function (
    nodeName,
    dimensionNode,
    role = null,
  ) {
    const response = await fetch(`${DJ_URL}/nodes/${nodeName}/link`, {
      method: 'DELETE',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        dimension_node: dimensionNode,
        role: role,
      }),
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
  },

  addReferenceDimensionLink: async function (
    nodeName,
    nodeColumn,
    dimensionNode,
    dimensionColumn,
    role = null,
  ) {
    const url = new URL(
      `${DJ_URL}/nodes/${nodeName}/columns/${nodeColumn}/link`,
    );
    url.searchParams.append('dimension_node', dimensionNode);
    url.searchParams.append('dimension_column', dimensionColumn);
    if (role) {
      url.searchParams.append('role', role);
    }

    const response = await fetch(url.toString(), {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
  },

  removeReferenceDimensionLink: async function (nodeName, nodeColumn) {
    const response = await fetch(
      `${DJ_URL}/nodes/${nodeName}/columns/${nodeColumn}/link`,
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
  users: async function () {
    return await (
      await fetch(`${DJ_URL}/users?with_activity=true`, {
        credentials: 'include',
      })
    ).json();
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
  materialize: async function (nodeName, jobType, strategy, schedule, config) {
    const response = await fetch(
      `${DJ_URL}/nodes/${nodeName}/materialization`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          job: jobType,
          strategy: strategy,
          schedule: schedule,
          config: config,
        }),
        credentials: 'include',
      },
    );
    return { status: response.status, json: await response.json() };
  },
  materializeCube: async function (
    nodeName,
    jobType,
    strategy,
    schedule,
    lookbackWindow,
  ) {
    const response = await fetch(
      `${DJ_URL}/nodes/${nodeName}/materialization`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          job: jobType,
          strategy: strategy,
          schedule: schedule,
          lookback_window: lookbackWindow,
        }),
        credentials: 'include',
      },
    );
    return { status: response.status, json: await response.json() };
  },
  // New V2 cube materialization endpoint for Druid cubes
  materializeCubeV2: async function (
    cubeName,
    schedule,
    strategy = 'incremental_time',
    lookbackWindow = '1 DAY',
    runBackfill = true,
  ) {
    const response = await fetch(`${DJ_URL}/cubes/${cubeName}/materialize`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        schedule: schedule,
        strategy: strategy,
        lookback_window: lookbackWindow,
        run_backfill: runBackfill,
      }),
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
  },
  runBackfill: async function (nodeName, materializationName, partitionValues) {
    const response = await fetch(
      `${DJ_URL}/nodes/${nodeName}/materializations/${materializationName}/backfill`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(
          partitionValues.map(partitionValue => {
            return {
              column_name: partitionValue.columnName,
              range: partitionValue.range,
              values: partitionValue.values,
            };
          }),
        ),
        credentials: 'include',
      },
    );
    return { status: response.status, json: await response.json() };
  },
  deleteMaterialization: async function (
    nodeName,
    materializationName,
    nodeVersion = null,
  ) {
    let url = `${DJ_URL}/nodes/${nodeName}/materializations?materialization_name=${materializationName}`;
    if (nodeVersion) {
      url += `&node_version=${nodeVersion}`;
    }
    const response = await fetch(url, {
      method: 'DELETE',
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
    });
    return { status: response.status, json: await response.json() };
  },
  listMetricMetadata: async function () {
    const response = await fetch(`${DJ_URL}/metrics/metadata`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
    });
    return await response.json();
  },
  materializationInfo: async function () {
    return await (
      await fetch(`${DJ_URL}/materialization/info`, {
        credentials: 'include',
      })
    ).json();
  },
  revalidate: async function (node) {
    return await (
      await fetch(`${DJ_URL}/nodes/${node}/validate`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
      })
    ).json();
  },
  // GET /notifications/
  getNotificationPreferences: async function (params = {}) {
    const query = new URLSearchParams(params).toString();
    return await (
      await fetch(`${DJ_URL}/notifications/${query ? `?${query}` : ''}`, {
        credentials: 'include',
      })
    ).json();
  },

  // POST /notifications/subscribe
  subscribeToNotifications: async function ({
    entity_type,
    entity_name,
    activity_types,
    alert_types,
  }) {
    const response = await fetch(`${DJ_URL}/notifications/subscribe`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
      body: JSON.stringify({
        entity_type,
        entity_name,
        activity_types,
        alert_types,
      }),
    });

    return {
      status: response.status,
      json: await response.json(),
    };
  },

  // DELETE /notifications/unsubscribe
  unsubscribeFromNotifications: async function ({ entity_type, entity_name }) {
    const url = new URL(`${DJ_URL}/notifications/unsubscribe`);
    url.searchParams.append('entity_type', entity_type);
    url.searchParams.append('entity_name', entity_name);

    const response = await fetch(url.toString(), {
      method: 'DELETE',
      credentials: 'include',
    });

    return {
      status: response.status,
      json: await response.json(),
    };
  },

  // GET /history/ with only_subscribed filter
  getSubscribedHistory: async function (limit = 10) {
    return await (
      await fetch(`${DJ_URL}/history/?only_subscribed=true&limit=${limit}`, {
        credentials: 'include',
      })
    ).json();
  },

  // POST /notifications/mark-read
  markNotificationsRead: async function () {
    const response = await fetch(`${DJ_URL}/notifications/mark-read`, {
      method: 'POST',
      credentials: 'include',
    });
    return await response.json();
  },

  // Service Account APIs
  listServiceAccounts: async function () {
    const response = await fetch(`${DJ_URL}/service-accounts`, {
      credentials: 'include',
    });
    return await response.json();
  },

  createServiceAccount: async function (name) {
    const response = await fetch(`${DJ_URL}/service-accounts`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
      body: JSON.stringify({ name }),
    });
    return await response.json();
  },

  deleteServiceAccount: async function (clientId) {
    const response = await fetch(`${DJ_URL}/service-accounts/${clientId}`, {
      method: 'DELETE',
      credentials: 'include',
    });
    return await response.json();
  },

  // ===== My Workspace GraphQL Queries =====

  getWorkspaceRecentlyEdited: async function (username, limit = 10) {
    // Nodes the user has edited, ordered by last updated (excluding source nodes)
    const query = `
      query RecentlyEdited($editedBy: String!, $limit: Int!, $nodeTypes: [NodeType!]) {
        findNodesPaginated(editedBy: $editedBy, limit: $limit, nodeTypes: $nodeTypes, orderBy: UPDATED_AT, ascending: false) {
          edges {
            node {
              name
              type
              currentVersion
              current {
                displayName
                status
                mode
                updatedAt
              }
              gitInfo {
                repo
                branch
                defaultBranch
                isDefaultBranch
                parentNamespace
                gitOnly
              }
            }
          }
        }
      }
    `;
    return await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          query,
          variables: {
            editedBy: username,
            limit,
            nodeTypes: ['TRANSFORM', 'METRIC', 'DIMENSION', 'CUBE'],
          },
        }),
      })
    ).json();
  },

  getWorkspaceOwnedNodes: async function (username, limit = 10) {
    // Owned nodes ordered by UPDATED_AT (excluding source nodes)
    const query = `
      query OwnedNodes($ownedBy: String!, $limit: Int!, $nodeTypes: [NodeType!]) {
        findNodesPaginated(ownedBy: $ownedBy, limit: $limit, nodeTypes: $nodeTypes, orderBy: UPDATED_AT, ascending: false) {
          edges {
            node {
              name
              type
              currentVersion
              current {
                displayName
                status
                mode
                updatedAt
              }
              gitInfo {
                repo
                branch
                defaultBranch
                isDefaultBranch
                parentNamespace
                gitOnly
              }
            }
          }
        }
      }
    `;
    return await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          query,
          variables: {
            ownedBy: username,
            limit,
            nodeTypes: ['TRANSFORM', 'METRIC', 'DIMENSION', 'CUBE'],
          },
        }),
      })
    ).json();
  },

  getWorkspaceCollections: async function (username) {
    const query = `
      query UserCollections($createdBy: String!) {
        listCollections(createdBy: $createdBy) {
          name
          description
          nodeCount
        }
      }
    `;
    return await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          query,
          variables: { createdBy: username },
        }),
      })
    ).json();
  },

  listAllCollections: async function () {
    const query = `
      query AllCollections {
        listCollections {
          name
          description
          nodeCount
          createdBy {
            username
          }
        }
      }
    `;
    return await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ query }),
      })
    ).json();
  },

  getWorkspaceNodesMissingDescription: async function (username, limit = 5) {
    const query = `
      query NodesMissingDescription($ownedBy: String!, $limit: Int!) {
        findNodesPaginated(ownedBy: $ownedBy, missingDescription: true, limit: $limit) {
          edges {
            node {
              name
              type
              current {
                displayName
                status
              }
            }
          }
        }
      }
    `;
    return await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          query,
          variables: { ownedBy: username, limit },
        }),
      })
    ).json();
  },

  getWorkspaceInvalidNodes: async function (username, limit = 10) {
    // Nodes the user owns that have INVALID status
    const query = `
      query InvalidNodes($ownedBy: String!, $limit: Int!, $statuses: [NodeStatus!]) {
        findNodesPaginated(ownedBy: $ownedBy, statuses: $statuses, limit: $limit) {
          edges {
            node {
              name
              type
              current {
                displayName
                status
              }
            }
          }
        }
      }
    `;
    return await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          query,
          variables: {
            ownedBy: username,
            limit,
            statuses: ['INVALID'],
          },
        }),
      })
    ).json();
  },

  getWorkspaceOrphanedDimensions: async function (username, limit = 10) {
    // Dimension nodes the user owns that are not linked to by any other node
    const query = `
      query OrphanedDimensions($ownedBy: String!, $limit: Int!, $orphanedDimension: Boolean!) {
        findNodesPaginated(ownedBy: $ownedBy, orphanedDimension: $orphanedDimension, limit: $limit) {
          edges {
            node {
              name
              type
              current {
                displayName
                status
              }
            }
          }
        }
      }
    `;
    return await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          query,
          variables: {
            ownedBy: username,
            limit,
            orphanedDimension: true,
          },
        }),
      })
    ).json();
  },

  getWorkspaceDraftNodes: async function (username, limit = 50) {
    // Draft nodes the user owns (we'll filter for stale ones on frontend)
    const query = `
      query DraftNodes($ownedBy: String!, $limit: Int!, $mode: NodeMode!) {
        findNodesPaginated(ownedBy: $ownedBy, mode: $mode, limit: $limit, orderBy: UPDATED_AT, ascending: true) {
          edges {
            node {
              name
              type
              current {
                displayName
                status
                mode
                updatedAt
              }
            }
          }
        }
      }
    `;
    return await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          query,
          variables: {
            ownedBy: username,
            limit,
            mode: 'DRAFT',
          },
        }),
      })
    ).json();
  },

  getWorkspaceMaterializations: async function (username, limit = 20) {
    // Nodes the user owns that have materializations configured
    const query = `
      query MaterializedNodes($ownedBy: String!, $limit: Int!, $hasMaterialization: Boolean!) {
        findNodesPaginated(ownedBy: $ownedBy, limit: $limit, hasMaterialization: $hasMaterialization) {
          edges {
            node {
              name
              type
              current {
                displayName
                status
                materializations {
                  name
                  schedule
                  job
                }
                availability {
                  catalog
                  table
                  validThroughTs
                  maxTemporalPartition
                }
              }
            }
          }
        }
      }
    `;
    return await (
      await fetch(DJ_GQL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          query,
          variables: { ownedBy: username, limit, hasMaterialization: true },
        }),
      })
    ).json();
  },

  // =================================
  // Pre-aggregation API
  // =================================

  // List pre-aggregations with optional filters
  listPreaggs: async function (filters = {}) {
    const params = new URLSearchParams();
    if (filters.node_name) params.append('node_name', filters.node_name);
    if (filters.grain) params.append('grain', filters.grain);
    if (filters.grain_mode) params.append('grain_mode', filters.grain_mode);
    if (filters.measures) params.append('measures', filters.measures);
    if (filters.status) params.append('status', filters.status);
    if (filters.include_stale) params.append('include_stale', 'true');

    return await (
      await fetch(`${DJ_URL}/preaggs/?${params}`, {
        credentials: 'include',
      })
    ).json();
  },

  // Plan pre-aggregations for given metrics and dimensions
  planPreaggs: async function (
    metrics,
    dimensions,
    strategy = null,
    schedule = null,
    lookbackWindow = null,
  ) {
    const body = {
      metrics,
      dimensions,
    };
    if (strategy) body.strategy = strategy;
    if (schedule) body.schedule = schedule;
    if (lookbackWindow) body.lookback_window = lookbackWindow;

    const response = await fetch(`${DJ_URL}/preaggs/plan`, {
      method: 'POST',
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    const result = await response.json();
    // If there's an error, include the status for the caller to check
    if (!response.ok) {
      return {
        ...result,
        _error: true,
        _status: response.status,
        message:
          result.message || result.detail || 'Failed to plan pre-aggregations',
      };
    }
    return result;
  },

  // Get a specific pre-aggregation by ID
  getPreagg: async function (preaggId) {
    return await (
      await fetch(`${DJ_URL}/preaggs/${preaggId}`, {
        credentials: 'include',
      })
    ).json();
  },

  // Trigger materialization for a pre-aggregation
  materializePreagg: async function (preaggId) {
    const response = await fetch(`${DJ_URL}/preaggs/${preaggId}/materialize`, {
      method: 'POST',
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    const result = await response.json();
    // If there's an error, include the status for the caller to check
    if (!response.ok) {
      return {
        ...result,
        _error: true,
        _status: response.status,
        message: result.message || result.detail || 'Materialization failed',
      };
    }
    return result;
  },

  // Update a single pre-aggregation's config
  updatePreaggConfig: async function (
    preaggId,
    strategy = null,
    schedule = null,
    lookbackWindow = null,
  ) {
    const body = {};
    if (strategy) body.strategy = strategy;
    if (schedule) body.schedule = schedule;
    if (lookbackWindow) body.lookback_window = lookbackWindow;

    const response = await fetch(`${DJ_URL}/preaggs/${preaggId}/config`, {
      method: 'PATCH',
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    const result = await response.json();
    if (!response.ok) {
      return {
        ...result,
        _error: true,
        _status: response.status,
        message: result.message || result.detail || 'Failed to update config',
      };
    }
    return result;
  },

  // Deactivate (pause) a pre-aggregation's workflow
  deactivatePreaggWorkflow: async function (preaggId) {
    const response = await fetch(`${DJ_URL}/preaggs/${preaggId}/workflow`, {
      method: 'DELETE',
      credentials: 'include',
    });
    const result = await response.json();
    if (!response.ok) {
      return {
        ...result,
        _error: true,
        _status: response.status,
        message:
          result.message || result.detail || 'Failed to deactivate workflow',
      };
    }
    return result;
  },

  // Run a backfill for a pre-aggregation
  runPreaggBackfill: async function (preaggId, startDate, endDate = null) {
    const body = {
      start_date: startDate,
    };
    if (endDate) body.end_date = endDate;

    const response = await fetch(`${DJ_URL}/preaggs/${preaggId}/backfill`, {
      method: 'POST',
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    const result = await response.json();
    if (!response.ok) {
      return {
        ...result,
        _error: true,
        _status: response.status,
        message: result.message || result.detail || 'Failed to run backfill',
      };
    }
    return result;
  },

  // Bulk deactivate pre-aggregation workflows for a node
  bulkDeactivatePreaggWorkflows: async function (nodeName, staleOnly = false) {
    const params = new URLSearchParams();
    params.append('node_name', nodeName);
    if (staleOnly) params.append('stale_only', 'true');

    const response = await fetch(`${DJ_URL}/preaggs/workflows?${params}`, {
      method: 'DELETE',
      credentials: 'include',
    });
    const result = await response.json();
    if (!response.ok) {
      return {
        ...result,
        _error: true,
        _status: response.status,
        message:
          result.message ||
          result.detail ||
          'Failed to bulk deactivate workflows',
      };
    }
    return result;
  },

  // Get cube details including materializations
  getCubeDetails: async function (cubeName) {
    const response = await fetch(`${DJ_URL}/cubes/${cubeName}`, {
      credentials: 'include',
    });
    if (!response.ok) {
      return { status: response.status, json: null };
    }
    return { status: response.status, json: await response.json() };
  },

  // Extract Druid cube workflow URLs from cube materializations
  getCubeWorkflowUrls: async function (cubeName) {
    console.log('getCubeWorkflowUrls: Fetching for cube', cubeName);
    const result = await this.getCubeDetails(cubeName);
    console.log('getCubeWorkflowUrls: Cube details result', result);
    if (!result.json || !result.json.materializations) {
      console.log('getCubeWorkflowUrls: No materializations found');
      return [];
    }
    console.log(
      'getCubeWorkflowUrls: Materializations',
      result.json.materializations,
    );
    const druidMat = result.json.materializations.find(
      m => m.name === 'druid_cube',
    );
    console.log('getCubeWorkflowUrls: druid_cube materialization', druidMat);
    if (druidMat && druidMat.config && druidMat.config.workflow_urls) {
      console.log(
        'getCubeWorkflowUrls: Found URLs',
        druidMat.config.workflow_urls,
      );
      return druidMat.config.workflow_urls;
    }
    console.log('getCubeWorkflowUrls: No workflow_urls in config');
    return [];
  },

  // Get full cube materialization info (for edit/refresh/backfill)
  getCubeMaterialization: async function (cubeName) {
    const result = await this.getCubeDetails(cubeName);
    if (!result.json || !result.json.materializations) {
      return null;
    }
    const druidMat = result.json.materializations.find(
      m => m.name === 'druid_cube',
    );
    if (!druidMat) {
      return null;
    }
    // Return combined info from materialization record and config
    return {
      id: druidMat.id,
      strategy: druidMat.strategy,
      schedule: druidMat.schedule,
      lookbackWindow: druidMat.lookback_window,
      druidDatasource: druidMat.config?.druid_datasource,
      preaggTables: druidMat.config?.preagg_tables || [],
      workflowUrls: druidMat.config?.workflow_urls || [],
      timestampColumn: druidMat.config?.timestamp_column,
      timestampFormat: druidMat.config?.timestamp_format,
    };
  },

  // Refresh cube workflow (re-push to scheduler without backfill)
  refreshCubeWorkflow: async function (
    cubeName,
    schedule,
    strategy = 'incremental_time',
    lookbackWindow = '1 DAY',
  ) {
    // Re-call materialize with run_backfill=false
    return this.materializeCubeV2(
      cubeName,
      schedule,
      strategy,
      lookbackWindow,
      false, // run_backfill = false for refresh
    );
  },

  // Deactivate (pause) a cube's workflow
  deactivateCubeWorkflow: async function (cubeName) {
    const response = await fetch(`${DJ_URL}/cubes/${cubeName}/materialize`, {
      method: 'DELETE',
      credentials: 'include',
    });
    if (!response.ok) {
      const result = await response.json().catch(() => ({}));
      return {
        status: response.status,
        json: {
          ...result,
          message:
            result.message ||
            result.detail ||
            'Failed to deactivate cube workflow',
        },
      };
    }
    return { status: response.status, json: await response.json() };
  },

  // Run cube backfill (trigger ad-hoc backfill with date range)
  runCubeBackfill: async function (cubeName, startDate, endDate = null) {
    const body = {
      start_date: startDate,
    };
    if (endDate) body.end_date = endDate;

    const response = await fetch(`${DJ_URL}/cubes/${cubeName}/backfill`, {
      method: 'POST',
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    const result = await response.json();
    if (!response.ok) {
      return {
        ...result,
        _error: true,
        _status: response.status,
        message:
          result.message || result.detail || 'Failed to run cube backfill',
      };
    }
    return result;
  },

  // ============================================================
  // Git Branch Management APIs
  // ============================================================

  // Get git configuration for a namespace
  getNamespaceGitConfig: async function (namespace) {
    const response = await fetch(`${DJ_URL}/namespaces/${namespace}/git`, {
      method: 'GET',
      credentials: 'include',
    });
    if (!response.ok) {
      if (response.status === 404) return null;
      const result = await response.json().catch(() => ({}));
      throw new Error(result.message || 'Failed to get git config');
    }
    return await response.json();
  },

  // Update git configuration for a namespace
  updateNamespaceGitConfig: async function (namespace, config) {
    const response = await fetch(`${DJ_URL}/namespaces/${namespace}/git`, {
      method: 'PATCH',
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(config),
    });
    const result = await response.json();
    if (!response.ok) {
      return {
        ...result,
        _error: true,
        _status: response.status,
        message: result.message || 'Failed to update git config',
      };
    }
    return result;
  },

  // Delete git configuration for a namespace
  deleteNamespaceGitConfig: async function (namespace) {
    const response = await fetch(`${DJ_URL}/namespaces/${namespace}/git`, {
      method: 'DELETE',
      credentials: 'include',
    });
    // DELETE returns 204 No Content on success, so no JSON body
    if (!response.ok) {
      const result = await response.json().catch(() => ({}));
      return {
        ...result,
        _error: true,
        _status: response.status,
        message: result.message || 'Failed to delete git config',
      };
    }
    // Success - return empty object since 204 has no body
    return {};
  },

  // List branch namespaces for a parent namespace
  listBranches: async function (namespace) {
    const response = await fetch(`${DJ_URL}/namespaces/${namespace}/branches`, {
      method: 'GET',
      credentials: 'include',
    });
    if (!response.ok) {
      const result = await response.json().catch(() => ({}));
      throw new Error(result.message || 'Failed to list branches');
    }
    return await response.json();
  },

  // Create a new branch namespace
  createBranch: async function (namespace, branchName) {
    const response = await fetch(`${DJ_URL}/namespaces/${namespace}/branches`, {
      method: 'POST',
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ branch_name: branchName }),
    });
    const result = await response.json();
    if (!response.ok) {
      return {
        ...result,
        _error: true,
        _status: response.status,
        message: result.message || 'Failed to create branch',
      };
    }
    return result;
  },

  // Delete a branch namespace
  deleteBranch: async function (
    parentNamespace,
    branchNamespace,
    deleteGitBranch = false,
  ) {
    const url = `${DJ_URL}/namespaces/${parentNamespace}/branches/${branchNamespace}?delete_git_branch=${deleteGitBranch}`;
    const response = await fetch(url, {
      method: 'DELETE',
      credentials: 'include',
    });
    const result = await response.json();
    if (!response.ok) {
      return {
        ...result,
        _error: true,
        _status: response.status,
        message: result.message || 'Failed to delete branch',
      };
    }
    return result;
  },

  // Sync a single node to git
  syncNodeToGit: async function (nodeName, commitMessage = null) {
    const body = {};
    if (commitMessage) body.commit_message = commitMessage;

    const response = await fetch(`${DJ_URL}/nodes/${nodeName}/sync-to-git`, {
      method: 'POST',
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    const result = await response.json();
    if (!response.ok) {
      return {
        ...result,
        _error: true,
        _status: response.status,
        message: result.message || 'Failed to sync node to git',
      };
    }
    return result;
  },

  // Sync all nodes in a namespace to git
  syncNamespaceToGit: async function (namespace, commitMessage = null) {
    const body = {};
    if (commitMessage) body.commit_message = commitMessage;

    const response = await fetch(
      `${DJ_URL}/namespaces/${namespace}/sync-to-git`,
      {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      },
    );
    const result = await response.json();
    if (!response.ok) {
      return {
        ...result,
        _error: true,
        _status: response.status,
        message: result.message || 'Failed to sync namespace to git',
      };
    }
    return result;
  },

  // Get existing pull request for a branch namespace (if any)
  getPullRequest: async function (namespace) {
    const response = await fetch(
      `${DJ_URL}/namespaces/${namespace}/pull-request`,
      {
        method: 'GET',
        credentials: 'include',
      },
    );
    if (!response.ok) {
      return null;
    }
    const result = await response.json();
    return result; // null if no PR exists
  },

  // Create a pull request from a branch namespace
  createPullRequest: async function (namespace, title, body = null) {
    const payload = { title };
    if (body) payload.body = body;

    const response = await fetch(
      `${DJ_URL}/namespaces/${namespace}/pull-request`,
      {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      },
    );
    const result = await response.json();
    if (!response.ok) {
      return {
        ...result,
        _error: true,
        _status: response.status,
        message: result.message || 'Failed to create pull request',
      };
    }
    return result;
  },
};
