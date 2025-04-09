"""
Models related to tags
"""

# pylint: disable=protected-access
from dataclasses import dataclass
from typing import Dict, Optional

from datajunction._internal import ClientEntity
from datajunction.exceptions import DJClientException
from datajunction.models import UpdateTag


@dataclass
class Tag(ClientEntity):
    """
    Node tag.
    """

    name: str
    tag_type: str
    description: Optional[str] = None
    display_name: Optional[str] = None
    tag_metadata: Optional[Dict] = None

    def to_dict(self) -> dict:
        """
        Convert the tag to a dictionary. We need to make this method because
        the default asdict() method from dataclasses does not handle nested dataclasses.
        """
        return {
            "name": self.name,
            "tag_type": self.tag_type,
            "description": self.description,
            "display_name": self.display_name,
            "tag_metadata": self.tag_metadata,
        }

    def _update(self) -> dict:
        """
        Update the tag for fields that have changed
        """
        update_tag = UpdateTag(
            description=self.description,
            tag_metadata=self.tag_metadata,
        )
        response = self.dj_client._update_tag(self.name, update_tag)
        if not response.status_code < 400:  # pragma: no cover
            raise DJClientException(
                f"Error updating tag `{self.name}`: {response.text}",
            )
        return response.json()

    def save(self) -> dict:
        """
        Saves the tag to DJ, whether it existed before or not.
        """
        existing_tag = self.dj_client._get_tag(tag_name=self.name)
        if "name" in existing_tag:
            # update
            response_json = self._update()
            self.refresh()
        else:
            # create
            response = self.dj_client._create_tag(
                tag=self,
            )  # pragma: no cover
            if not response.status_code < 400:  # pragma: no cover
                raise DJClientException(
                    f"Error creating new tag `{self.name}`: {response.text}",
                )
            response_json = response.json()
        return response_json

    def refresh(self):
        """
        Refreshes a tag with its latest version from the database.
        """
        refreshed_tag = self.dj_client._get_tag(self.name)
        for key, value in refreshed_tag.items():  # pragma: no cover
            if hasattr(self, key):
                setattr(self, key, value)
        return self
