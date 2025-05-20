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

import importlib.resources
from typing import List, Optional

import yaml

from harmonize_ds.manager.datasource_factory import DataSourceFactory
from harmonize_ds.sources.base import Source


class DataSourceManager:
    """Manages data sources loaded from the configuration file."""

    def __init__(self, config_path: str = "config/config.yaml") -> None:
        """Initializes the manager and loads the YAML data sources.

        Args:
            config_path (str): Path to the YAML configuration file.
        """
        self._datasources: List[Source] = []
        self._config_path = config_path
        self.load_all()

    def load_all(self) -> None:
        """Loads data sources from the configuration file."""
        try:
            with (
                importlib.resources.files("harmonize_ds")
                .joinpath("config/config.yaml")
                .open("r") as file
            ):
                config = yaml.safe_load(file)
        except FileNotFoundError:
            raise RuntimeError("Configuration file config.yaml not found")
        except yaml.YAMLError as e:
            raise RuntimeError(f"Error loading YAML: {e}")

        self._datasources = [
            DataSourceFactory.make(source["type"], source["id"], source["host"])
            for source in config.get("sources", [])
        ]

    def get_datasources(self) -> List[Source]:
        """Returns all loaded data sources."""
        return self._datasources

    def get_datasource_by_id(self, id: str) -> Optional[Source]:
        """Returns a data source by ID or None if not found."""
        for ds in self._datasources:
            if ds._source_id == id:
                return ds
        return None

    def __repr__(self) -> str:
        """Instance representation for debugging."""
        return (f"<DataSourceManager {len(self._datasources)} loaded data sources>")