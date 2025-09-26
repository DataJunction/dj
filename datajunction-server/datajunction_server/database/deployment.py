from uuid import UUID, uuid4
from sqlalchemy import String, Enum, JSON, DateTime
from datetime import datetime
from datajunction_server.models.deployment import (
    DeploymentResult,
    DeploymentSpec,
    DeploymentStatus,
)
from sqlalchemy import JSON, DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy_utils import UUIDType
from datajunction_server.typing import UTCDatetime
from datajunction_server.database.user import User
from datetime import datetime, timezone
from functools import partial
from datajunction_server.database.base import Base


class Deployment(Base):
    __tablename__ = "deployments"

    uuid: Mapped[UUID] = mapped_column(UUIDType(), default=uuid4, primary_key=True)

    namespace: Mapped[str] = mapped_column(String)
    status: Mapped[DeploymentStatus] = mapped_column(
        Enum(DeploymentStatus),
        default=DeploymentStatus.PENDING,
    )
    spec: Mapped[dict] = mapped_column(JSON, default={})
    results: Mapped[dict] = mapped_column(JSON, default={})

    created_by_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    created_by: Mapped[User] = relationship("User", lazy="selectin")

    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.now, timezone.utc),
    )
    updated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    @property
    def deployment_spec(self) -> DeploymentSpec:
        return DeploymentSpec(**self.spec)  # pragma: no cover

    @deployment_spec.setter
    def deployment_spec(self, value: DeploymentSpec):
        self.spec = value.model_dump()  # pragma: no cover

    @property
    def deployment_results(self) -> list[DeploymentResult]:
        return [DeploymentResult(**result) for result in self.results]

    @deployment_results.setter
    def deployment_results(self, value: list[DeploymentResult]):
        self.results = [result.model_dump() for result in value]  # pragma: no cover
