from typing import Dict, List

import pystac_client

from harmonize_ds.utils import Utils

from .base import Source


class STAC(Source):
    """Implementation of a STAC API client.

    This class provides an interface to interact with a STAC server.

    Args:
        source_id (str): Identifier of the STAC source.
        url (str): Base URL of the STAC server.
    """

    def __init__(self, source_id: str, url: str) -> None:
        """Initialize the STAC client."""
        super().__init__(source_id, url)
        self.service = pystac_client.Client.open(self.url)

    @property
    def collections(self) -> List[Dict[str, str]]:
        """Retrieve the list of available collections.

        Returns:
            List[Dict[str, str]]: A list of dictionaries, each containing the source ID and collection name.
        """
        return [
            {"id": self.id, "collection": collection.id}
            for collection in self.service.get_collections()
        ]

    def get_type(self) -> str:
        """Return the data source type."""
        return "STAC"

    def describe(self, collection_id: str) -> Dict:
        """Retrieve metadata for a specific collection.

        Args:
            collection_id (str): The ID of the collection.

        Returns:
            Dict: Collection metadata in dictionary format.

        Raises:
            ValueError: If the collection is not found.
        """
        collection = self.service.get_collection(collection_id)
        if collection is None:
            raise ValueError(f"Collection '{collection_id}' not found.")
        return collection.to_dict()
