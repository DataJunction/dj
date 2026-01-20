/**
 * Typed GraphQL Service for DataJunction API
 *
 * This service provides type-safe GraphQL operations using generated types.
 * For REST operations, use DJService.js
 */

import type {
  NodeType,
  NodeMode,
  NodeStatus,
  NodeSortField,
  ListNodesForLandingQuery,
  ListCubesForPresetQuery,
  GetCubeForPlannerQuery,
  GetNodeForEditingQuery,
  GetNodesByNamesQuery,
  GetMetricQuery,
  GetCubeForEditingQuery,
  GetUpstreamNodesQuery,
  GetDownstreamNodesQuery,
  GetNodeColumnsWithPartitionsQuery,
} from '../../types/graphql';

// GraphQL endpoint configuration
const DJ_GQL = process.env.REACT_APP_DJ_GQL
  ? process.env.REACT_APP_DJ_GQL
  : (process.env.REACT_APP_DJ_URL || 'http://localhost:8000') + '/graphql';

// GraphQL response wrapper
interface GraphQLResponse<T> {
  data?: T;
  errors?: Array<{ message: string; path?: string[] }>;
}

// Helper type for sort configuration
export interface SortConfig {
  key: 'name' | 'displayName' | 'type' | 'status' | 'updatedAt';
  direction: 'ascending' | 'descending';
}

// Helper type for list nodes filters
export interface ListNodesFilters {
  ownedBy?: string | null;
  statuses?: NodeStatus[] | null;
  missingDescription?: boolean;
  hasMaterialization?: boolean;
  orphanedDimension?: boolean;
}

// Return type for cubeForPlanner (transformed from GraphQL response)
export interface CubeForPlannerResult {
  name: string;
  display_name: string | null | undefined;
  cube_node_metrics: string[];
  cube_node_dimensions: string[];
  cubeMaterialization: {
    strategy: string | null | undefined;
    schedule: string;
    lookbackWindow: string | undefined;
    druidDatasource: string | undefined;
    preaggTables: string[];
    workflowUrls: string[];
    timestampColumn: string | undefined;
    timestampFormat: string | undefined;
  } | null;
}

// Generic GraphQL fetch helper
async function gqlFetch<T>(
  query: string,
  variables?: Record<string, unknown>,
): Promise<GraphQLResponse<T>> {
  const response = await fetch(DJ_GQL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include',
    body: JSON.stringify({ query, variables }),
  });
  return response.json();
}

// Query strings
const LIST_NODES_FOR_LANDING = `
  query ListNodesForLanding(
    $namespace: String
    $nodeTypes: [NodeType!]
    $tags: [String!]
    $editedBy: String
    $mode: NodeMode
    $before: String
    $after: String
    $limit: Int
    $orderBy: NodeSortField!
    $ascending: Boolean!
    $ownedBy: String
    $statuses: [NodeStatus!]
    $missingDescription: Boolean!
    $hasMaterialization: Boolean!
    $orphanedDimension: Boolean!
  ) {
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
          createdBy {
            username
          }
        }
      }
    }
  }
`;

const LIST_CUBES_FOR_PRESET = `
  query ListCubesForPreset {
    findNodes(nodeTypes: [CUBE]) {
      name
      current {
        displayName
      }
    }
  }
`;

const GET_CUBE_FOR_PLANNER = `
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

const GET_NODE_FOR_EDITING = `
  query GetNodeForEditing($name: String!) {
    findNodes(names: [$name]) {
      name
      type
      current {
        displayName
        description
        primaryKey
        query
        parents { name type }
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

const GET_NODES_BY_NAMES = `
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

const GET_METRIC = `
  query GetMetric($name: String!) {
    findNodes(names: [$name]) {
      name
      current {
        parents { name }
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

const GET_CUBE_FOR_EDITING = `
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

const GET_UPSTREAM_NODES = `
  query GetUpstreamNodes($nodeNames: [String!]!) {
    upstreamNodes(nodeNames: $nodeNames) {
      name
      type
    }
  }
`;

const GET_DOWNSTREAM_NODES = `
  query GetDownstreamNodes($nodeNames: [String!]!) {
    downstreamNodes(nodeNames: $nodeNames) {
      name
      type
    }
  }
`;

const GET_NODE_COLUMNS_WITH_PARTITIONS = `
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

// Sort field mapping
const SORT_ORDER_MAPPING: Record<SortConfig['key'], NodeSortField> = {
  name: 'NAME' as NodeSortField,
  displayName: 'DISPLAY_NAME' as NodeSortField,
  type: 'TYPE' as NodeSortField,
  status: 'STATUS' as NodeSortField,
  updatedAt: 'UPDATED_AT' as NodeSortField,
};

/**
 * Typed GraphQL Service
 */
export const DJGraphQLService = {
  /**
   * List nodes with pagination and filters (for landing/namespace pages)
   */
  listNodesForLanding: async (
    namespace: string | null,
    nodeTypes: NodeType[] | null,
    tags: string[] | null,
    editedBy: string | null,
    before: string | null,
    after: string | null,
    limit: number | null,
    sortConfig: SortConfig,
    mode: NodeMode | null,
    filters: ListNodesFilters = {},
  ): Promise<GraphQLResponse<ListNodesForLandingQuery>> => {
    const {
      ownedBy = null,
      statuses = null,
      missingDescription = false,
      hasMaterialization = false,
      orphanedDimension = false,
    } = filters;

    return gqlFetch<ListNodesForLandingQuery>(LIST_NODES_FOR_LANDING, {
      namespace,
      nodeTypes,
      tags,
      editedBy,
      mode: mode || null,
      before,
      after,
      limit,
      orderBy: SORT_ORDER_MAPPING[sortConfig.key],
      ascending: sortConfig.direction === 'ascending',
      ownedBy,
      statuses,
      missingDescription,
      hasMaterialization,
      orphanedDimension,
    });
  },

  /**
   * List cubes for preset dropdown (lightweight)
   */
  listCubesForPreset: async (): Promise<
    Array<{ name: string; display_name: string | null }>
  > => {
    try {
      const result = await gqlFetch<ListCubesForPresetQuery>(
        LIST_CUBES_FOR_PRESET,
      );
      const nodes = result.data?.findNodes || [];
      return nodes.map(node => ({
        name: node.name,
        display_name: node.current?.displayName || null,
      }));
    } catch (err) {
      console.error('Failed to fetch cubes via GraphQL:', err);
      return [];
    }
  },

  /**
   * Get cube details for query planner (optimized query)
   */
  cubeForPlanner: async (
    name: string,
  ): Promise<CubeForPlannerResult | null> => {
    try {
      const result = await gqlFetch<GetCubeForPlannerQuery>(
        GET_CUBE_FOR_PLANNER,
        { name },
      );
      const node = result.data?.findNodes?.[0];
      if (!node) {
        return null;
      }

      const current = node.current;
      const cubeMetrics = (current?.cubeMetrics || []).map(m => m.name);
      const cubeDimensions = (current?.cubeDimensions || []).map(d => d.name);

      // Extract druid_cube materialization if present
      const druidMat = (current?.materializations || []).find(
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
        display_name: current?.displayName,
        cube_node_metrics: cubeMetrics,
        cube_node_dimensions: cubeDimensions,
        cubeMaterialization,
      };
    } catch (err) {
      console.error('Failed to fetch cube via GraphQL:', err);
      return null;
    }
  },

  /**
   * Get node details for editing
   */
  getNodeForEditing: async (
    name: string,
  ): Promise<GetNodeForEditingQuery['findNodes'][0] | null> => {
    const result = await gqlFetch<GetNodeForEditingQuery>(
      GET_NODE_FOR_EDITING,
      { name },
    );
    if (!result.data?.findNodes?.length) {
      return null;
    }
    return result.data.findNodes[0];
  },

  /**
   * Get multiple nodes by names (batch fetch)
   */
  getNodesByNames: async (
    names: string[],
  ): Promise<GetNodesByNamesQuery['findNodes']> => {
    if (!names || names.length === 0) {
      return [];
    }
    const result = await gqlFetch<GetNodesByNamesQuery>(GET_NODES_BY_NAMES, {
      names,
    });
    return result.data?.findNodes || [];
  },

  /**
   * Get metric details
   */
  getMetric: async (
    name: string,
  ): Promise<GetMetricQuery['findNodes'][0] | null> => {
    const result = await gqlFetch<GetMetricQuery>(GET_METRIC, { name });
    return result.data?.findNodes?.[0] || null;
  },

  /**
   * Get cube details for editing
   */
  getCubeForEditing: async (
    name: string,
  ): Promise<GetCubeForEditingQuery['findNodes'][0] | null> => {
    const result = await gqlFetch<GetCubeForEditingQuery>(
      GET_CUBE_FOR_EDITING,
      { name },
    );
    if (!result.data?.findNodes?.length) {
      return null;
    }
    return result.data.findNodes[0];
  },

  /**
   * Get upstream nodes
   */
  upstreamNodes: async (
    nodeNames: string | string[],
  ): Promise<GetUpstreamNodesQuery['upstreamNodes']> => {
    const names = Array.isArray(nodeNames) ? nodeNames : [nodeNames];
    const result = await gqlFetch<GetUpstreamNodesQuery>(GET_UPSTREAM_NODES, {
      nodeNames: names,
    });
    return result.data?.upstreamNodes || [];
  },

  /**
   * Get downstream nodes
   */
  downstreamNodes: async (
    nodeNames: string | string[],
  ): Promise<GetDownstreamNodesQuery['downstreamNodes']> => {
    const names = Array.isArray(nodeNames) ? nodeNames : [nodeNames];
    const result = await gqlFetch<GetDownstreamNodesQuery>(
      GET_DOWNSTREAM_NODES,
      { nodeNames: names },
    );
    return result.data?.downstreamNodes || [];
  },

  /**
   * Get node columns with partition info
   */
  getNodeColumnsWithPartitions: async (
    nodeName: string,
  ): Promise<{
    columns: GetNodeColumnsWithPartitionsQuery['findNodes'][0]['current']['columns'];
    temporalPartitions: GetNodeColumnsWithPartitionsQuery['findNodes'][0]['current']['columns'];
  }> => {
    try {
      const result = await gqlFetch<GetNodeColumnsWithPartitionsQuery>(
        GET_NODE_COLUMNS_WITH_PARTITIONS,
        { name: nodeName },
      );

      const node = result.data?.findNodes?.[0];
      if (!node) {
        return { columns: [], temporalPartitions: [] };
      }

      const columns = node.current?.columns || [];
      const temporalPartitions = columns.filter(
        col => col.partition?.type_ === 'TEMPORAL',
      );

      return { columns, temporalPartitions };
    } catch (err) {
      console.error('Failed to fetch node columns with partitions:', err);
      return { columns: [], temporalPartitions: [] };
    }
  },
};

// Export types for consumers
export type {
  NodeType,
  NodeMode,
  NodeStatus,
  NodeSortField,
  ListNodesForLandingQuery,
  ListCubesForPresetQuery,
  GetCubeForPlannerQuery,
  GetNodeForEditingQuery,
  GetNodesByNamesQuery,
  GetMetricQuery,
  GetCubeForEditingQuery,
  GetUpstreamNodesQuery,
  GetDownstreamNodesQuery,
  GetNodeColumnsWithPartitionsQuery,
};
