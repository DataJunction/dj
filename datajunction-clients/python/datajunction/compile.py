"""Deprecated: client-side compilation has moved server-side.

Client-side YAML compilation (the old ``Project`` / ``CompiledProject`` flow)
has been removed. YAML deployment is now handled entirely server-side: use
:class:`datajunction.DeploymentService` (or the ``dj push`` / ``dj pull`` CLI),
which reconstructs a deployment spec from local YAML files and POSTs it to the
server's ``/deployments`` orchestrator.
"""

from datajunction.exceptions import DJClientException

_MESSAGE = (
    "datajunction.Project / client-side YAML compilation has been removed. "
    "Deployment is now handled server-side — use datajunction.DeploymentService "
    "(or the `dj push` CLI)."
)


class Project:  # pylint: disable=too-few-public-methods
    """
    Deprecated. Client-side project compilation has been removed; deployment is
    now handled server-side via :class:`datajunction.DeploymentService`.
    """

    def __init__(self, *args, **kwargs):
        raise DJClientException(_MESSAGE)

    @classmethod
    def load(cls, *args, **kwargs):
        """Deprecated. Use ``datajunction.DeploymentService`` instead."""
        raise DJClientException(_MESSAGE)

    @classmethod
    def load_current(cls, *args, **kwargs):
        """Deprecated. Use ``datajunction.DeploymentService`` instead."""
        raise DJClientException(_MESSAGE)
