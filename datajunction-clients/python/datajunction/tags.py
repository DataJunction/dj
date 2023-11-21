"""
Models related to tags
"""
# pylint: disable=protected-access
from typing import Dict, Optional

from pydantic import BaseModel

from datajunction._internal import ClientEntity
from datajunction.exceptions import DJClientException
from datajunction.models import UpdateTag


class TagInfo(BaseModel):
    """
    Metadata about a tag
    """

    name: str
    description: str = ""
    tag_type: str
    tag_metadata: Optional[Dict] = None


class Tag(TagInfo, ClientEntity):
    """
    Node tags
    """

    def _update(self) -> "Tag":
        """
        Update the tag for fields that have changed
        """
        update_tag = UpdateTag(
            description=self.description,
            tag_metadata=self.tag_metadata,
        )
        return self.dj_client._update_tag(self.name, update_tag)

    def save(self) -> dict:
        """
        Saves the tag to DJ, whether it existed before or not.
        """
        existing_tag = self.dj_client._get_tag(tag_name=self.name)
        if "name" in existing_tag:
            # update
            response = self._update()
            if not response.ok:  # pragma: no cover
                raise DJClientException(
                    f"Error updating tag `{self.name}`: {response.text}",
                )
            self.refresh()
        else:
            # create
            response = self.dj_client._create_tag(
                tag=self,
            )  # pragma: no cover
            if not response.ok:  # pragma: no cover
                raise DJClientException(
                    f"Error creating new tag `{self.name}`: {response.text}",
                )
        return response.json()

    def refresh(self):
        """
        Refreshes a tag with its latest version from the database.
        """
        refreshed_tag = self.dj_client._get_tag(self.name)
        for key, value in refreshed_tag.items():
            if hasattr(self, key):
                setattr(self, key, value)
        return self
