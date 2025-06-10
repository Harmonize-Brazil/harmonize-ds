#
# This file is part of Python Client Library for the Harmonize Datasources.
# Copyright (C) 2025 INPE.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/gpl-3.0.html>.
#

"""Python Client Library for the Harmonize Datasources."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class Source(ABC):
    """Abstract class to represent a Data Source."""

    def __init__(self, source_id: str, url: str) -> None:
        """Initialize a DataSource.

        Args:
            source_id (str): Identifier of the data source.
            url (str): Base URL of the data source.
        """
        self._source_id = source_id
        self._url = url

    @property
    def id(self) -> str:
        """Return the data source identifier."""
        return self._source_id

    @property
    def url(self) -> str:
        """Return the base URL of the data source."""
        return self._url

    @abstractmethod
    def get_type(self) -> str:
        """Return the data source type."""
        pass

    @abstractmethod
    def describe(self, collection_id: str) -> Dict:
        """Return metadata of the given collection.

        Args:
            collection_id (str): The ID of the collection to describe.

        Returns:
            Dict: A dictionary with the collection metadata.
        """
        pass

    def get(
        self,
        collection_id: str,
        filter: Optional[Dict[str, Any]] = None,
        srid: int = 4326,
    ) -> Any:
        """Generic method to get data from a collection.

        Args:
            collection_id (str): Collection identifier.
            filter (Dict[str, Any], optional): Filters for data search.
            bbox (List[float], optional): BBOX [min_x, min_y, max_x, max_y].
            time (str, optional): Time filter.

        Returns:
            Any: The result of the request, depending on the source type.
        """
        raise NotImplementedError("get() method must be implemented.")
