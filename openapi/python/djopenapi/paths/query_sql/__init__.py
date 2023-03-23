# do not import all endpoints into this module because that uses a lot of memory and stack frames
# if you need the ability to import all endpoints from this module, import them with
# from djopenapi.paths.query_sql import Api

from djopenapi.paths import PathValues

path = PathValues.QUERY_SQL