import typing_extensions

from djclient.paths import PathValues
from djclient.apis.paths.catalogs_ import Catalogs
from djclient.apis.paths.catalogs_name_ import CatalogsName
from djclient.apis.paths.catalogs_name_engines_ import CatalogsNameEngines
from djclient.apis.paths.engines_ import Engines
from djclient.apis.paths.engines_name_version_ import EnginesNameVersion
from djclient.apis.paths.metrics_ import Metrics
from djclient.apis.paths.metrics_name_ import MetricsName
from djclient.apis.paths.metrics_name_sql_ import MetricsNameSql
from djclient.apis.paths.metrics_common_dimensions_ import MetricsCommonDimensions
from djclient.apis.paths.query_sql import QuerySql
from djclient.apis.paths.nodes_validate_ import NodesValidate
from djclient.apis.paths.nodes_node_name_attributes_ import NodesNodeNameAttributes
from djclient.apis.paths.nodes_ import Nodes
from djclient.apis.paths.nodes_name_ import NodesName
from djclient.apis.paths.nodes_name_materialization_ import NodesNameMaterialization
from djclient.apis.paths.nodes_name_revisions_ import NodesNameRevisions
from djclient.apis.paths.nodes_source_ import NodesSource
from djclient.apis.paths.nodes_metric_ import NodesMetric
from djclient.apis.paths.nodes_dimension_ import NodesDimension
from djclient.apis.paths.nodes_transform_ import NodesTransform
from djclient.apis.paths.nodes_cube_ import NodesCube
from djclient.apis.paths.nodes_name_columns_column_ import NodesNameColumnsColumn
from djclient.apis.paths.nodes_name_tag_ import NodesNameTag
from djclient.apis.paths.nodes_similarity_node1_name_node2_name import NodesSimilarityNode1NameNode2Name
from djclient.apis.paths.nodes_name_downstream_ import NodesNameDownstream
from djclient.apis.paths.data_availability_node_name_ import DataAvailabilityNodeName
from djclient.apis.paths.health_ import Health
from djclient.apis.paths.cubes_name_ import CubesName
from djclient.apis.paths.tags_ import Tags
from djclient.apis.paths.tags_name_ import TagsName
from djclient.apis.paths.tags_name_nodes_ import TagsNameNodes
from djclient.apis.paths.attributes_ import Attributes

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
        PathValues.METRICS_NAME_SQL_: MetricsNameSql,
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
        PathValues.DATA_AVAILABILITY_NODE_NAME_: DataAvailabilityNodeName,
        PathValues.HEALTH_: Health,
        PathValues.CUBES_NAME_: CubesName,
        PathValues.TAGS_: Tags,
        PathValues.TAGS_NAME_: TagsName,
        PathValues.TAGS_NAME_NODES_: TagsNameNodes,
        PathValues.ATTRIBUTES_: Attributes,
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
        PathValues.METRICS_NAME_SQL_: MetricsNameSql,
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
        PathValues.DATA_AVAILABILITY_NODE_NAME_: DataAvailabilityNodeName,
        PathValues.HEALTH_: Health,
        PathValues.CUBES_NAME_: CubesName,
        PathValues.TAGS_: Tags,
        PathValues.TAGS_NAME_: TagsName,
        PathValues.TAGS_NAME_NODES_: TagsNameNodes,
        PathValues.ATTRIBUTES_: Attributes,
    }
)
