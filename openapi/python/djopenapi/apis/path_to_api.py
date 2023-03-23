import typing_extensions

from djopenapi.paths import PathValues
from djopenapi.apis.paths.catalogs_ import Catalogs
from djopenapi.apis.paths.catalogs_name_ import CatalogsName
from djopenapi.apis.paths.catalogs_name_engines_ import CatalogsNameEngines
from djopenapi.apis.paths.engines_ import Engines
from djopenapi.apis.paths.engines_name_version_ import EnginesNameVersion
from djopenapi.apis.paths.metrics_ import Metrics
from djopenapi.apis.paths.metrics_name_ import MetricsName
from djopenapi.apis.paths.metrics_common_dimensions_ import MetricsCommonDimensions
from djopenapi.apis.paths.query_sql import QuerySql
from djopenapi.apis.paths.nodes_validate_ import NodesValidate
from djopenapi.apis.paths.nodes_node_name_attributes_ import NodesNodeNameAttributes
from djopenapi.apis.paths.nodes_ import Nodes
from djopenapi.apis.paths.nodes_name_ import NodesName
from djopenapi.apis.paths.nodes_name_materialization_ import NodesNameMaterialization
from djopenapi.apis.paths.nodes_name_revisions_ import NodesNameRevisions
from djopenapi.apis.paths.nodes_source_ import NodesSource
from djopenapi.apis.paths.nodes_metric_ import NodesMetric
from djopenapi.apis.paths.nodes_dimension_ import NodesDimension
from djopenapi.apis.paths.nodes_transform_ import NodesTransform
from djopenapi.apis.paths.nodes_cube_ import NodesCube
from djopenapi.apis.paths.nodes_name_columns_column_ import NodesNameColumnsColumn
from djopenapi.apis.paths.nodes_name_tag_ import NodesNameTag
from djopenapi.apis.paths.nodes_similarity_node1_name_node2_name import NodesSimilarityNode1NameNode2Name
from djopenapi.apis.paths.nodes_name_downstream_ import NodesNameDownstream
from djopenapi.apis.paths.data_node_name_availability_ import DataNodeNameAvailability
from djopenapi.apis.paths.data_node_name_ import DataNodeName
from djopenapi.apis.paths.health_ import Health
from djopenapi.apis.paths.cubes_name_ import CubesName
from djopenapi.apis.paths.tags_ import Tags
from djopenapi.apis.paths.tags_name_ import TagsName
from djopenapi.apis.paths.tags_name_nodes_ import TagsNameNodes
from djopenapi.apis.paths.attributes_ import Attributes
from djopenapi.apis.paths.sql_node_name_ import SqlNodeName

PathToApi = typing_extensions.TypedDict(
    'PathToApi',
    {
        PathValues.CATALOGS_: Catalogs,
        PathValues.CATALOGS_NAME_: CatalogsName,
        PathValues.CATALOGS_NAME_ENGINES_: CatalogsNameEngines,
        PathValues.ENGINES_: Engines,
        PathValues.ENGINES_NAME_VERSION_: EnginesNameVersion,
        PathValues.METRICS_: Metrics,
        PathValues.METRICS_NAME_: MetricsName,
        PathValues.METRICS_COMMON_DIMENSIONS_: MetricsCommonDimensions,
        PathValues.QUERY_SQL: QuerySql,
        PathValues.NODES_VALIDATE_: NodesValidate,
        PathValues.NODES_NODE_NAME_ATTRIBUTES_: NodesNodeNameAttributes,
        PathValues.NODES_: Nodes,
        PathValues.NODES_NAME_: NodesName,
        PathValues.NODES_NAME_MATERIALIZATION_: NodesNameMaterialization,
        PathValues.NODES_NAME_REVISIONS_: NodesNameRevisions,
        PathValues.NODES_SOURCE_: NodesSource,
        PathValues.NODES_METRIC_: NodesMetric,
        PathValues.NODES_DIMENSION_: NodesDimension,
        PathValues.NODES_TRANSFORM_: NodesTransform,
        PathValues.NODES_CUBE_: NodesCube,
        PathValues.NODES_NAME_COLUMNS_COLUMN_: NodesNameColumnsColumn,
        PathValues.NODES_NAME_TAG_: NodesNameTag,
        PathValues.NODES_SIMILARITY_NODE1_NAME_NODE2_NAME: NodesSimilarityNode1NameNode2Name,
        PathValues.NODES_NAME_DOWNSTREAM_: NodesNameDownstream,
        PathValues.DATA_NODE_NAME_AVAILABILITY_: DataNodeNameAvailability,
        PathValues.DATA_NODE_NAME_: DataNodeName,
        PathValues.HEALTH_: Health,
        PathValues.CUBES_NAME_: CubesName,
        PathValues.TAGS_: Tags,
        PathValues.TAGS_NAME_: TagsName,
        PathValues.TAGS_NAME_NODES_: TagsNameNodes,
        PathValues.ATTRIBUTES_: Attributes,
        PathValues.SQL_NODE_NAME_: SqlNodeName,
    }
)

path_to_api = PathToApi(
    {
        PathValues.CATALOGS_: Catalogs,
        PathValues.CATALOGS_NAME_: CatalogsName,
        PathValues.CATALOGS_NAME_ENGINES_: CatalogsNameEngines,
        PathValues.ENGINES_: Engines,
        PathValues.ENGINES_NAME_VERSION_: EnginesNameVersion,
        PathValues.METRICS_: Metrics,
        PathValues.METRICS_NAME_: MetricsName,
        PathValues.METRICS_COMMON_DIMENSIONS_: MetricsCommonDimensions,
        PathValues.QUERY_SQL: QuerySql,
        PathValues.NODES_VALIDATE_: NodesValidate,
        PathValues.NODES_NODE_NAME_ATTRIBUTES_: NodesNodeNameAttributes,
        PathValues.NODES_: Nodes,
        PathValues.NODES_NAME_: NodesName,
        PathValues.NODES_NAME_MATERIALIZATION_: NodesNameMaterialization,
        PathValues.NODES_NAME_REVISIONS_: NodesNameRevisions,
        PathValues.NODES_SOURCE_: NodesSource,
        PathValues.NODES_METRIC_: NodesMetric,
        PathValues.NODES_DIMENSION_: NodesDimension,
        PathValues.NODES_TRANSFORM_: NodesTransform,
        PathValues.NODES_CUBE_: NodesCube,
        PathValues.NODES_NAME_COLUMNS_COLUMN_: NodesNameColumnsColumn,
        PathValues.NODES_NAME_TAG_: NodesNameTag,
        PathValues.NODES_SIMILARITY_NODE1_NAME_NODE2_NAME: NodesSimilarityNode1NameNode2Name,
        PathValues.NODES_NAME_DOWNSTREAM_: NodesNameDownstream,
        PathValues.DATA_NODE_NAME_AVAILABILITY_: DataNodeNameAvailability,
        PathValues.DATA_NODE_NAME_: DataNodeName,
        PathValues.HEALTH_: Health,
        PathValues.CUBES_NAME_: CubesName,
        PathValues.TAGS_: Tags,
        PathValues.TAGS_NAME_: TagsName,
        PathValues.TAGS_NAME_NODES_: TagsNameNodes,
        PathValues.ATTRIBUTES_: Attributes,
        PathValues.SQL_NODE_NAME_: SqlNodeName,
    }
)
