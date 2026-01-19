export type Maybe<T> = T | null;
export type InputMaybe<T> = T | null;
export type Exact<T extends { [key: string]: unknown }> = {
  [K in keyof T]: T[K];
};
export type MakeOptional<T, K extends keyof T> = Omit<T, K> & {
  [SubKey in K]?: Maybe<T[SubKey]>;
};
export type MakeMaybe<T, K extends keyof T> = Omit<T, K> & {
  [SubKey in K]: Maybe<T[SubKey]>;
};
export type MakeEmpty<
  T extends { [key: string]: unknown },
  K extends keyof T,
> = { [_ in K]?: never };
export type Incremental<T> =
  | T
  | {
      [P in keyof T]?: P extends ' $fragmentName' | '__typename' ? T[P] : never;
    };
/** All built-in and custom scalars, mapped to their actual values */
export interface Scalars {
  ID: { input: string; output: string };
  String: { input: string; output: string };
  Boolean: { input: boolean; output: boolean };
  Int: { input: number; output: number };
  Float: { input: number; output: number };
  /** Date with time (isoformat) */
  DateTime: { input: any; output: any };
  /** The `JSON` scalar type represents JSON values as specified by [ECMA-404](https://ecma-international.org/wp-content/uploads/ECMA-404_2nd_edition_december_2017.pdf). */
  JSON: { input: any; output: any };
  /** BigInt field */
  Union: { input: any; output: any };
}

export enum Aggregability {
  Full = 'FULL',
  Limited = 'LIMITED',
  None = 'NONE',
}

export interface AggregationRule {
  level?: Maybe<Array<Scalars['String']['output']>>;
  type: Aggregability;
}

export interface Attribute {
  attributeType: AttributeTypeName;
}

export interface AttributeTypeName {
  name: Scalars['String']['output'];
  namespace: Scalars['String']['output'];
}

export interface AvailabilityState {
  catalog: Scalars['String']['output'];
  categoricalPartitions?: Maybe<Array<Scalars['String']['output']>>;
  maxTemporalPartition?: Maybe<Array<Scalars['String']['output']>>;
  minTemporalPartition?: Maybe<Array<Scalars['String']['output']>>;
  partitions?: Maybe<Array<PartitionAvailability>>;
  schema_?: Maybe<Scalars['String']['output']>;
  table: Scalars['String']['output'];
  temporalPartitions?: Maybe<Array<Scalars['String']['output']>>;
  url?: Maybe<Scalars['String']['output']>;
  validThroughTs: Scalars['Int']['output'];
}

export interface Backfill {
  spec?: Maybe<Array<PartitionBackfill>>;
  urls?: Maybe<Array<Scalars['String']['output']>>;
}

export interface Catalog {
  engines?: Maybe<Array<Engine>>;
  name: Scalars['String']['output'];
}

export interface Column {
  attributes: Array<Attribute>;
  dimension?: Maybe<NodeName>;
  displayName?: Maybe<Scalars['String']['output']>;
  name: Scalars['String']['output'];
  partition?: Maybe<Partition>;
  type: Scalars['String']['output'];
}

export interface ColumnMetadata {
  name: Scalars['String']['output'];
  semanticEntity?: Maybe<SemanticEntity>;
  semanticType?: Maybe<SemanticType>;
  type: Scalars['String']['output'];
}

export interface CubeDefinition {
  cube?: InputMaybe<Scalars['String']['input']>;
  dimensions?: InputMaybe<Array<Scalars['String']['input']>>;
  filters?: InputMaybe<Array<Scalars['String']['input']>>;
  metrics?: InputMaybe<Array<Scalars['String']['input']>>;
  orderby?: InputMaybe<Array<Scalars['String']['input']>>;
}

export interface DjError {
  code: ErrorCode;
  context?: Maybe<Scalars['String']['output']>;
  message?: Maybe<Scalars['String']['output']>;
}

export interface DecomposedMetric {
  combiner: Scalars['String']['output'];
  components: Array<MetricComponent>;
  derivedExpression: Scalars['String']['output'];
  derivedQuery?: Maybe<Scalars['String']['output']>;
}

export enum Dialect {
  Clickhouse = 'CLICKHOUSE',
  Druid = 'DRUID',
  Duckdb = 'DUCKDB',
  Postgres = 'POSTGRES',
  Redshift = 'REDSHIFT',
  Snowflake = 'SNOWFLAKE',
  Spark = 'SPARK',
  Sqlite = 'SQLITE',
  Trino = 'TRINO',
}

export interface DialectInfo {
  name: Scalars['String']['output'];
  pluginClass: Scalars['String']['output'];
}

export interface DimensionAttribute {
  DimensionNode?: Maybe<Node>;
  attribute?: Maybe<Scalars['String']['output']>;
  /** The dimension node this attribute belongs to */
  dimensionNode: Node;
  name: Scalars['String']['output'];
  properties: Array<Scalars['String']['output']>;
  role?: Maybe<Scalars['String']['output']>;
  type: Scalars['String']['output'];
}

export interface DimensionLink {
  dimension: NodeName;
  foreignKeys: Scalars['JSON']['output'];
  joinCardinality?: Maybe<JoinCardinality>;
  joinSql: Scalars['String']['output'];
  joinType: JoinType;
  role?: Maybe<Scalars['String']['output']>;
}

export interface Engine {
  dialect?: Maybe<Dialect>;
  name: Scalars['String']['output'];
  uri?: Maybe<Scalars['String']['output']>;
  version: Scalars['String']['output'];
}

export interface EngineSettings {
  /** The name of the engine used by the generated SQL */
  name: Scalars['String']['input'];
  /** The version of the engine used by the generated SQL */
  version?: InputMaybe<Scalars['String']['input']>;
}

export enum ErrorCode {
  AlreadyExists = 'ALREADY_EXISTS',
  AuthenticationError = 'AUTHENTICATION_ERROR',
  CatalogNotFound = 'CATALOG_NOT_FOUND',
  CompoundBuildException = 'COMPOUND_BUILD_EXCEPTION',
  IncompleteAuthorization = 'INCOMPLETE_AUTHORIZATION',
  InvalidArgumentsToFunction = 'INVALID_ARGUMENTS_TO_FUNCTION',
  InvalidColumn = 'INVALID_COLUMN',
  InvalidColumnInFilter = 'INVALID_COLUMN_IN_FILTER',
  InvalidCube = 'INVALID_CUBE',
  InvalidDimension = 'INVALID_DIMENSION',
  InvalidDimensionJoin = 'INVALID_DIMENSION_JOIN',
  InvalidDimensionLink = 'INVALID_DIMENSION_LINK',
  InvalidFilterPattern = 'INVALID_FILTER_PATTERN',
  InvalidLoginCredentials = 'INVALID_LOGIN_CREDENTIALS',
  InvalidMetric = 'INVALID_METRIC',
  InvalidNamespace = 'INVALID_NAMESPACE',
  InvalidOrderBy = 'INVALID_ORDER_BY',
  InvalidParent = 'INVALID_PARENT',
  InvalidSqlQuery = 'INVALID_SQL_QUERY',
  InvalidValueInFilter = 'INVALID_VALUE_IN_FILTER',
  MissingColumns = 'MISSING_COLUMNS',
  MissingParameter = 'MISSING_PARAMETER',
  MissingParent = 'MISSING_PARENT',
  NodeTypeError = 'NODE_TYPE_ERROR',
  NotImplementedError = 'NOT_IMPLEMENTED_ERROR',
  OauthError = 'OAUTH_ERROR',
  QueryServiceError = 'QUERY_SERVICE_ERROR',
  TagNotFound = 'TAG_NOT_FOUND',
  TypeInference = 'TYPE_INFERENCE',
  UnauthorizedAccess = 'UNAUTHORIZED_ACCESS',
  UnknownError = 'UNKNOWN_ERROR',
  UnknownNode = 'UNKNOWN_NODE',
  UserNotFound = 'USER_NOT_FOUND',
}

export interface GeneratedSql {
  columns: Array<ColumnMetadata>;
  dialect: Dialect;
  errors: Array<DjError>;
  node: Node;
  sql: Scalars['String']['output'];
  upstreamTables: Array<Scalars['String']['output']>;
}

export enum JoinCardinality {
  ManyToMany = 'MANY_TO_MANY',
  ManyToOne = 'MANY_TO_ONE',
  OneToMany = 'ONE_TO_MANY',
  OneToOne = 'ONE_TO_ONE',
}

export enum JoinType {
  Cross = 'CROSS',
  Full = 'FULL',
  Inner = 'INNER',
  Left = 'LEFT',
  Right = 'RIGHT',
}

export interface MaterializationConfig {
  backfills: Array<Backfill>;
  config: Scalars['JSON']['output'];
  job?: Maybe<Scalars['String']['output']>;
  name?: Maybe<Scalars['String']['output']>;
  schedule: Scalars['String']['output'];
  strategy?: Maybe<Scalars['String']['output']>;
}

export interface MaterializationPlan {
  units: Array<MaterializationUnit>;
}

export interface MaterializationUnit {
  filterRefs: Array<VersionedRef>;
  filters: Array<Scalars['String']['output']>;
  grainDimensions: Array<VersionedRef>;
  measures: Array<MetricComponent>;
  upstream: VersionedRef;
}

export interface MetricComponent {
  aggregation?: Maybe<Scalars['String']['output']>;
  expression: Scalars['String']['output'];
  merge?: Maybe<Scalars['String']['output']>;
  name: Scalars['String']['output'];
  rule: AggregationRule;
}

export enum MetricDirection {
  HigherIsBetter = 'HIGHER_IS_BETTER',
  LowerIsBetter = 'LOWER_IS_BETTER',
  Neutral = 'NEUTRAL',
}

export interface MetricMetadata {
  direction?: Maybe<MetricDirection>;
  expression: Scalars['String']['output'];
  incompatibleDruidFunctions: Array<Scalars['String']['output']>;
  maxDecimalExponent?: Maybe<Scalars['Int']['output']>;
  minDecimalExponent?: Maybe<Scalars['Int']['output']>;
  significantDigits?: Maybe<Scalars['Int']['output']>;
  unit?: Maybe<Unit>;
}

export interface Node {
  createdAt: Scalars['DateTime']['output'];
  createdBy: User;
  current: NodeRevision;
  currentVersion: Scalars['String']['output'];
  deactivatedAt?: Maybe<Scalars['DateTime']['output']>;
  editedBy: Array<Scalars['String']['output']>;
  id: Scalars['Union']['output'];
  name: Scalars['String']['output'];
  owners: Array<User>;
  revisions: Array<NodeRevision>;
  tags: Array<TagBase>;
  type: NodeType;
}

export interface NodeConnection {
  edges: Array<NodeEdge>;
  pageInfo: PageInfo;
}

export interface NodeEdge {
  node: Node;
}

export enum NodeMode {
  Draft = 'DRAFT',
  Published = 'PUBLISHED',
}

export interface NodeName {
  name: Scalars['String']['output'];
}

export interface NodeNameVersion {
  currentVersion: Scalars['String']['output'];
  name: Scalars['String']['output'];
  type: Scalars['String']['output'];
}

export interface NodeRevision {
  availability?: Maybe<AvailabilityState>;
  catalog?: Maybe<Catalog>;
  columns: Array<Column>;
  cubeDimensions: Array<DimensionAttribute>;
  cubeMetrics: Array<NodeRevision>;
  customMetadata?: Maybe<Scalars['JSON']['output']>;
  description: Scalars['String']['output'];
  dimensionLinks: Array<DimensionLink>;
  displayName?: Maybe<Scalars['String']['output']>;
  extractedMeasures?: Maybe<DecomposedMetric>;
  id: Scalars['Union']['output'];
  materializations?: Maybe<Array<MaterializationConfig>>;
  metricMetadata?: Maybe<MetricMetadata>;
  mode?: Maybe<NodeMode>;
  name: Scalars['String']['output'];
  parents: Array<NodeNameVersion>;
  primaryKey: Array<Scalars['String']['output']>;
  query?: Maybe<Scalars['String']['output']>;
  requiredDimensions?: Maybe<Array<Column>>;
  schema_?: Maybe<Scalars['String']['output']>;
  status: NodeStatus;
  table?: Maybe<Scalars['String']['output']>;
  type: NodeType;
  updatedAt: Scalars['DateTime']['output'];
  version: Scalars['String']['output'];
}

export interface NodeRevisionColumnsArgs {
  attributes?: InputMaybe<Array<Scalars['String']['input']>>;
}

export enum NodeSortField {
  CreatedAt = 'CREATED_AT',
  DisplayName = 'DISPLAY_NAME',
  Mode = 'MODE',
  Name = 'NAME',
  Status = 'STATUS',
  Type = 'TYPE',
  UpdatedAt = 'UPDATED_AT',
}

export enum NodeStatus {
  Invalid = 'INVALID',
  Valid = 'VALID',
}

export enum NodeType {
  Cube = 'CUBE',
  Dimension = 'DIMENSION',
  Metric = 'METRIC',
  Source = 'SOURCE',
  Transform = 'TRANSFORM',
}

export enum OAuthProvider {
  Basic = 'BASIC',
  Github = 'GITHUB',
  Google = 'GOOGLE',
}

export interface PageInfo {
  /** When paginating forwards, the cursor to continue. */
  endCursor?: Maybe<Scalars['String']['output']>;
  /** When paginating forwards, are there more nodes? */
  hasNextPage: Scalars['Boolean']['output'];
  /** When paginating forwards, are there more nodes? */
  hasPrevPage: Scalars['Boolean']['output'];
  /** When paginating back, the cursor to continue. */
  startCursor?: Maybe<Scalars['String']['output']>;
}

export interface Partition {
  expression?: Maybe<Scalars['String']['output']>;
  format?: Maybe<Scalars['String']['output']>;
  granularity?: Maybe<Scalars['String']['output']>;
  type_: PartitionType;
}

export interface PartitionAvailability {
  maxTemporalPartition?: Maybe<Array<Scalars['String']['output']>>;
  minTemporalPartition?: Maybe<Array<Scalars['String']['output']>>;
  validThroughTs?: Maybe<Scalars['Int']['output']>;
  value: Array<Maybe<Scalars['String']['output']>>;
}

export interface PartitionBackfill {
  columnName: Scalars['String']['output'];
  range?: Maybe<Array<Scalars['String']['output']>>;
  values?: Maybe<Array<Scalars['String']['output']>>;
}

export enum PartitionType {
  Categorical = 'CATEGORICAL',
  Temporal = 'TEMPORAL',
}

export interface Query {
  /** Get common dimensions for one or more nodes */
  commonDimensions: Array<DimensionAttribute>;
  /** Find downstream nodes (optionally, of a given type) from a given node. */
  downstreamNodes: Array<Node>;
  /** Find nodes based on the search parameters. */
  findNodes: Array<Node>;
  /** Find nodes based on the search parameters with pagination */
  findNodesPaginated: NodeConnection;
  /** List available catalogs */
  listCatalogs: Array<Catalog>;
  /** List all supported SQL dialects */
  listDialects: Array<DialectInfo>;
  /** List all available engines */
  listEngines: Array<Engine>;
  /** List all DJ node tag types */
  listTagTypes: Array<Scalars['String']['output']>;
  /** Find DJ node tags based on the search parameters. */
  listTags: Array<Tag>;
  /** Get materialization plan for a list of metrics, dimensions, and filters. */
  materializationPlan: MaterializationPlan;
  /** Get measures SQL for a list of metrics, dimensions, and filters. */
  measuresSql: Array<GeneratedSql>;
  /** Find upstream nodes (optionally, of a given type) from a given node. */
  upstreamNodes: Array<Node>;
}

export interface QueryCommonDimensionsArgs {
  nodes?: InputMaybe<Array<Scalars['String']['input']>>;
}

export interface QueryDownstreamNodesArgs {
  includeDeactivated?: Scalars['Boolean']['input'];
  nodeNames: Array<Scalars['String']['input']>;
  nodeType?: InputMaybe<NodeType>;
}

export interface QueryFindNodesArgs {
  ascending?: Scalars['Boolean']['input'];
  dimensions?: InputMaybe<Array<Scalars['String']['input']>>;
  editedBy?: InputMaybe<Scalars['String']['input']>;
  fragment?: InputMaybe<Scalars['String']['input']>;
  hasMaterialization?: Scalars['Boolean']['input'];
  limit?: InputMaybe<Scalars['Int']['input']>;
  missingDescription?: Scalars['Boolean']['input'];
  missingOwner?: Scalars['Boolean']['input'];
  mode?: InputMaybe<NodeMode>;
  names?: InputMaybe<Array<Scalars['String']['input']>>;
  namespace?: InputMaybe<Scalars['String']['input']>;
  nodeTypes?: InputMaybe<Array<NodeType>>;
  orderBy?: NodeSortField;
  orphanedDimension?: Scalars['Boolean']['input'];
  ownedBy?: InputMaybe<Scalars['String']['input']>;
  statuses?: InputMaybe<Array<NodeStatus>>;
  tags?: InputMaybe<Array<Scalars['String']['input']>>;
}

export interface QueryFindNodesPaginatedArgs {
  after?: InputMaybe<Scalars['String']['input']>;
  ascending?: Scalars['Boolean']['input'];
  before?: InputMaybe<Scalars['String']['input']>;
  dimensions?: InputMaybe<Array<Scalars['String']['input']>>;
  editedBy?: InputMaybe<Scalars['String']['input']>;
  fragment?: InputMaybe<Scalars['String']['input']>;
  hasMaterialization?: Scalars['Boolean']['input'];
  limit?: InputMaybe<Scalars['Int']['input']>;
  missingDescription?: Scalars['Boolean']['input'];
  missingOwner?: Scalars['Boolean']['input'];
  mode?: InputMaybe<NodeMode>;
  names?: InputMaybe<Array<Scalars['String']['input']>>;
  namespace?: InputMaybe<Scalars['String']['input']>;
  nodeTypes?: InputMaybe<Array<NodeType>>;
  orderBy?: NodeSortField;
  orphanedDimension?: Scalars['Boolean']['input'];
  ownedBy?: InputMaybe<Scalars['String']['input']>;
  statuses?: InputMaybe<Array<NodeStatus>>;
  tags?: InputMaybe<Array<Scalars['String']['input']>>;
}

export interface QueryListTagsArgs {
  tagNames?: InputMaybe<Array<Scalars['String']['input']>>;
  tagTypes?: InputMaybe<Array<Scalars['String']['input']>>;
}

export interface QueryMaterializationPlanArgs {
  cube: CubeDefinition;
}

export interface QueryMeasuresSqlArgs {
  cube: CubeDefinition;
  engine?: InputMaybe<EngineSettings>;
  includeAllColumns?: Scalars['Boolean']['input'];
  preaggregate?: Scalars['Boolean']['input'];
  queryParameters?: InputMaybe<Scalars['JSON']['input']>;
  useMaterialized?: Scalars['Boolean']['input'];
}

export interface QueryUpstreamNodesArgs {
  includeDeactivated?: Scalars['Boolean']['input'];
  nodeNames: Array<Scalars['String']['input']>;
  nodeType?: InputMaybe<NodeType>;
}

export interface SemanticEntity {
  /** The column on the node this semantic entity is sourced from */
  column: Scalars['String']['output'];
  name: Scalars['String']['output'];
  /** The node this semantic entity is sourced from */
  node: Scalars['String']['output'];
}

export enum SemanticType {
  Dimension = 'DIMENSION',
  Measure = 'MEASURE',
  Metric = 'METRIC',
  Timestamp = 'TIMESTAMP',
}

export interface Tag {
  description?: Maybe<Scalars['String']['output']>;
  displayName?: Maybe<Scalars['String']['output']>;
  name: Scalars['String']['output'];
  /** The nodes with this tag */
  nodes: Array<Node>;
  tagMetadata?: Maybe<Scalars['JSON']['output']>;
  tagType: Scalars['String']['output'];
}

export interface TagBase {
  description?: Maybe<Scalars['String']['output']>;
  displayName?: Maybe<Scalars['String']['output']>;
  name: Scalars['String']['output'];
  tagMetadata?: Maybe<Scalars['JSON']['output']>;
  tagType: Scalars['String']['output'];
}

export interface Unit {
  abbreviation?: Maybe<Scalars['String']['output']>;
  category?: Maybe<Scalars['String']['output']>;
  label?: Maybe<Scalars['String']['output']>;
  name: Scalars['String']['output'];
}

export interface User {
  email?: Maybe<Scalars['String']['output']>;
  id: Scalars['Union']['output'];
  isAdmin: Scalars['Boolean']['output'];
  name?: Maybe<Scalars['String']['output']>;
  oauthProvider: OAuthProvider;
  username: Scalars['String']['output'];
}

export interface VersionedRef {
  name: Scalars['String']['output'];
  version: Scalars['String']['output'];
}

export type GetCubeForPlannerQueryVariables = Exact<{
  name: Scalars['String']['input'];
}>;

export type GetCubeForPlannerQuery = {
  findNodes: Array<{
    name: string;
    current: {
      displayName?: string | null;
      cubeMetrics: Array<{ name: string }>;
      cubeDimensions: Array<{ name: string }>;
      materializations?: Array<{
        name?: string | null;
        config: any;
        schedule: string;
        strategy?: string | null;
      }> | null;
    };
  }>;
};

export type GetDownstreamNodesQueryVariables = Exact<{
  nodeNames: Array<Scalars['String']['input']> | Scalars['String']['input'];
}>;

export type GetDownstreamNodesQuery = {
  downstreamNodes: Array<{ name: string; type: NodeType }>;
};

export type GetCubeForEditingQueryVariables = Exact<{
  name: Scalars['String']['input'];
}>;

export type GetCubeForEditingQuery = {
  findNodes: Array<{
    name: string;
    type: NodeType;
    owners: Array<{ username: string }>;
    current: {
      displayName?: string | null;
      description: string;
      mode?: NodeMode | null;
      cubeMetrics: Array<{ name: string }>;
      cubeDimensions: Array<{
        name: string;
        attribute?: string | null;
        properties: Array<string>;
      }>;
    };
    tags: Array<{ name: string; displayName?: string | null }>;
  }>;
};

export type GetMetricQueryVariables = Exact<{
  name: Scalars['String']['input'];
}>;

export type GetMetricQuery = {
  findNodes: Array<{
    name: string;
    current: {
      parents: Array<{ name: string }>;
      metricMetadata?: {
        direction?: MetricDirection | null;
        expression: string;
        significantDigits?: number | null;
        incompatibleDruidFunctions: Array<string>;
        unit?: { name: string } | null;
      } | null;
      requiredDimensions?: Array<{ name: string }> | null;
    };
  }>;
};

export type GetNodeColumnsWithPartitionsQueryVariables = Exact<{
  name: Scalars['String']['input'];
}>;

export type GetNodeColumnsWithPartitionsQuery = {
  findNodes: Array<{
    name: string;
    current: {
      columns: Array<{
        name: string;
        type: string;
        partition?: {
          type_: PartitionType;
          format?: string | null;
          granularity?: string | null;
        } | null;
      }>;
    };
  }>;
};

export type GetNodeForEditingQueryVariables = Exact<{
  name: Scalars['String']['input'];
}>;

export type GetNodeForEditingQuery = {
  findNodes: Array<{
    name: string;
    type: NodeType;
    current: {
      displayName?: string | null;
      description: string;
      primaryKey: Array<string>;
      query?: string | null;
      mode?: NodeMode | null;
      customMetadata?: any | null;
      parents: Array<{ name: string; type: string }>;
      metricMetadata?: {
        direction?: MetricDirection | null;
        expression: string;
        significantDigits?: number | null;
        incompatibleDruidFunctions: Array<string>;
        unit?: { name: string } | null;
      } | null;
      requiredDimensions?: Array<{ name: string }> | null;
    };
    tags: Array<{ name: string; displayName?: string | null }>;
    owners: Array<{ username: string }>;
  }>;
};

export type GetNodesByNamesQueryVariables = Exact<{
  names?: InputMaybe<
    Array<Scalars['String']['input']> | Scalars['String']['input']
  >;
}>;

export type GetNodesByNamesQuery = {
  findNodes: Array<{
    name: string;
    type: NodeType;
    current: {
      displayName?: string | null;
      status: NodeStatus;
      mode?: NodeMode | null;
    };
  }>;
};

export type ListCubesForPresetQueryVariables = Exact<{ [key: string]: never }>;

export type ListCubesForPresetQuery = {
  findNodes: Array<{ name: string; current: { displayName?: string | null } }>;
};

export type ListNodesForLandingQueryVariables = Exact<{
  namespace?: InputMaybe<Scalars['String']['input']>;
  nodeTypes?: InputMaybe<Array<NodeType> | NodeType>;
  tags?: InputMaybe<
    Array<Scalars['String']['input']> | Scalars['String']['input']
  >;
  editedBy?: InputMaybe<Scalars['String']['input']>;
  mode?: InputMaybe<NodeMode>;
  before?: InputMaybe<Scalars['String']['input']>;
  after?: InputMaybe<Scalars['String']['input']>;
  limit?: InputMaybe<Scalars['Int']['input']>;
  orderBy?: NodeSortField;
  ascending?: Scalars['Boolean']['input'];
  ownedBy?: InputMaybe<Scalars['String']['input']>;
  statuses?: InputMaybe<Array<NodeStatus> | NodeStatus>;
  missingDescription?: Scalars['Boolean']['input'];
  hasMaterialization?: Scalars['Boolean']['input'];
  orphanedDimension?: Scalars['Boolean']['input'];
}>;

export type ListNodesForLandingQuery = {
  findNodesPaginated: {
    pageInfo: {
      hasNextPage: boolean;
      endCursor?: string | null;
      hasPrevPage: boolean;
      startCursor?: string | null;
    };
    edges: Array<{
      node: {
        name: string;
        type: NodeType;
        currentVersion: string;
        editedBy: Array<string>;
        tags: Array<{ name: string; tagType: string }>;
        current: {
          displayName?: string | null;
          status: NodeStatus;
          mode?: NodeMode | null;
          updatedAt: any;
        };
        createdBy: { username: string };
      };
    }>;
  };
};

export type GetUpstreamNodesQueryVariables = Exact<{
  nodeNames: Array<Scalars['String']['input']> | Scalars['String']['input'];
}>;

export type GetUpstreamNodesQuery = {
  upstreamNodes: Array<{ name: string; type: NodeType }>;
};
