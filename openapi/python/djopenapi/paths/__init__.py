# do not import all endpoints into this module because that uses a lot of memory and stack frames
# if you need the ability to import all endpoints from this module, import them with
# from djopenapi.apis.path_to_api import path_to_api

import enum


class PathValues(str, enum.Enum):
    CATALOGS_ = "/catalogs/"
    CATALOGS_NAME_ = "/catalogs/{name}/"
    CATALOGS_NAME_ENGINES_ = "/catalogs/{name}/engines/"
    ENGINES_ = "/engines/"
    ENGINES_NAME_VERSION_ = "/engines/{name}/{version}/"
    METRICS_ = "/metrics/"
    METRICS_NAME_ = "/metrics/{name}/"
    METRICS_COMMON_DIMENSIONS_ = "/metrics/common/dimensions/"
    QUERY_SQL = "/query/{sql}"
    NODES_VALIDATE_ = "/nodes/validate/"
    NODES_NODE_NAME_ATTRIBUTES_ = "/nodes/{node_name}/attributes/"
    NODES_ = "/nodes/"
    NODES_NAME_ = "/nodes/{name}/"
    NODES_NAME_MATERIALIZATION_ = "/nodes/{name}/materialization/"
    NODES_NAME_REVISIONS_ = "/nodes/{name}/revisions/"
    NODES_SOURCE_ = "/nodes/source/"
    NODES_METRIC_ = "/nodes/metric/"
    NODES_DIMENSION_ = "/nodes/dimension/"
    NODES_TRANSFORM_ = "/nodes/transform/"
    NODES_CUBE_ = "/nodes/cube/"
    NODES_NAME_COLUMNS_COLUMN_ = "/nodes/{name}/columns/{column}/"
    NODES_NAME_TAG_ = "/nodes/{name}/tag/"
    NODES_SIMILARITY_NODE1_NAME_NODE2_NAME = "/nodes/similarity/{node1_name}/{node2_name}"
    NODES_NAME_DOWNSTREAM_ = "/nodes/{name}/downstream/"
    DATA_NODE_NAME_AVAILABILITY_ = "/data/{node_name}/availability/"
    DATA_NODE_NAME_ = "/data/{node_name}/"
    HEALTH_ = "/health/"
    CUBES_NAME_ = "/cubes/{name}/"
    TAGS_ = "/tags/"
    TAGS_NAME_ = "/tags/{name}/"
    TAGS_NAME_NODES_ = "/tags/{name}/nodes/"
    ATTRIBUTES_ = "/attributes/"
    SQL_NODE_NAME_ = "/sql/{node_name}/"
