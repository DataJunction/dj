# do not import all endpoints into this module because that uses a lot of memory and stack frames
# if you need the ability to import all endpoints from this module, import them with
# from djclient.apis.path_to_api import path_to_api

import enum


class PathValues(str, enum.Enum):
    CATALOGS_ = "/catalogs/"
    CATALOGS_NAME_ = "/catalogs/{name}/"
    CATALOGS_NAME_ENGINES_ = "/catalogs/{name}/engines/"
    DATABASES_ = "/databases/"
    ENGINES_ = "/engines/"
    ENGINES_NAME_VERSION_ = "/engines/{name}/{version}/"
    METRICS_ = "/metrics/"
    METRICS_NAME_ = "/metrics/{name}/"
    METRICS_NAME_SQL_ = "/metrics/{name}/sql/"
    QUERY_VALIDATE = "/query/validate"
    NODES_VALIDATE_ = "/nodes/validate/"
    NODES_ = "/nodes/"
    NODES_NAME_ = "/nodes/{name}/"
    NODES_NAME_MATERIALIZATION_ = "/nodes/{name}/materialization/"
    NODES_NAME_REVISIONS_ = "/nodes/{name}/revisions/"
    NODES_NAME_COLUMNS_COLUMN_ = "/nodes/{name}/columns/{column}/"
    NODES_NAME_TABLE_ = "/nodes/{name}/table/"
    NODES_SIMILARITY_NODE1_NAME_NODE2_NAME = "/nodes/similarity/{node1_name}/{node2_name}"
    NODES_NAME_DOWNSTREAM_ = "/nodes/{name}/downstream/"
    DATA_AVAILABILITY_NODE_NAME_ = "/data/availability/{node_name}/"
    HEALTH_ = "/health/"
    CUBES_NAME_ = "/cubes/{name}/"
    GRAPHQL = "/graphql"
