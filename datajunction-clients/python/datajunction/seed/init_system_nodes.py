import logging

from datajunction import DJBuilder, DeploymentService
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

logger.info("Deploying DJ system nodes...")
# Deployment is handled server-side: DeploymentService reconstructs a deployment
# spec from the YAML files under `nodes/` and POSTs it to the `/deployments`
# orchestrator. The explicit namespace (matching the project's `prefix:` in
# dj.yaml) marks this as the bootstrap/system-seed case, which skips git config.
service = DeploymentService(client=dj)
service.push("nodes", namespace="system.dj")
logger.info("Finished deploying DJ system nodes.")
