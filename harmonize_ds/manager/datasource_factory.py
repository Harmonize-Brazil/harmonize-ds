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

from typing import Dict, Type

from harmonize_ds.sources.base import Source
from harmonize_ds.sources.wcs import WCS
from harmonize_ds.sources.wfs import WFS


class DataSourceFactory:
    """Class DataSourceFactory."""

    _factories: Dict[str, Type[Source]] = {}

    @classmethod
    def register(cls, name: str, factory: Type[Source]) -> None:
        """Register a new data source at the factory."""
        cls._factories[name] = factory

    @classmethod
    def make(cls, ds_type: str, ds_id: str, host: str) -> Source:
        """Creates an instance of the registered data source."""
        try:
            factory = cls._factories[ds_type]
        except KeyError as exc:
            raise ValueError(f"Datasource '{ds_type}' not registered {exc}.")
        return factory(ds_id, host)


DataSourceFactory.register("WCS", WCS)
DataSourceFactory.register("WFS", WFS)
