import logging

from datajunction import DJBuilder, Project
from datajunction.exceptions import DJClientException
from datajunction._internal import RequestsSessionWithEndpoint

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

session = RequestsSessionWithEndpoint(endpoint="http://dj:8000")
session.post("/basic/login/", data={"username": "dj", "password": "dj"})

dj = DJBuilder(requests_session=session)

tables = [
    "node",
    "noderevision",
    "users",
    "materialization",
    "node_owners",
    "availabilitystate",
    "backfill",
    "collection",
    "dimensionlink",
]

for table in tables:
    try:
        logger.info("Registering table: %s", table)
        dj.register_table("dj_metadata", "public", table)
    except DJClientException as exc:
        if "already exists" in str(exc):
            logger.info("Already exists: %s", table)
        else:
            logger.error("Error registering tables: %s", exc)
logger.info("Finished registering DJ system metadata tables")

logger.info("Loading DJ system nodes...")
project = Project.load("nodes")
logger.info("Finished loading DJ system nodes.")

logger.info("Compiling DJ system nodes...")
compiled_project = project.compile()
logger.info("Finished compiling DJ system nodes.")

logger.info("Deploying DJ system nodes...")
compiled_project.deploy(client=dj)
logger.info("Finished deploying DJ system nodes.")
