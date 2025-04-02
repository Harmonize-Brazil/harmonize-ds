#
# This file is part of Python Client Library for the Harmonize Datasources.
# Copyright (C) 2022 INPE.
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
"""Python Client Library for Harmonize Datasources."""
from typing import Any, Dict, Iterable, List, Optional

import geopandas as gpd
import pandas as pd

from harmonize_ds.manager.datasource_manager import DataSourceManager
from harmonize_ds.sources.base import Source


class CollectionClient:
    """Representa uma coleção específica dentro de uma fonte de dados."""

    def __init__(self, datasource: Source, collection_id: str):
        self._datasource = datasource
        self._collection_id = collection_id
        self._metadata = self._datasource.describe(self._collection_id)

    def describe(self) -> Dict[str, str]:
        """Retorna os metadados da coleção."""
        return self._metadata

    @property
    def title(self) -> Optional[str]:
        """Título da coleção, se disponível."""
        return self._metadata.get("title")

    @property
    def abstract(self) -> Optional[str]:
        """Resumo da coleção, se disponível."""
        return self._metadata.get("abstract")

    def get(self, filter: Optional[Dict[str, Any]] = None):
        """Obtém os dados da coleção como DataFrame, delegando para a fonte."""
        return self._datasource.get_dataset(self._collection_id, filter=filter)

    def __repr__(self) -> str:
        """Representação textual da coleção."""
        return f"<CollectionClient title={self.title}, source_id={self._datasource._source_id }, collection_id={self._collection_id}>"


class HARMONIZEDS:
    manager = DataSourceManager()

    @classmethod
    def list_collections(cls) -> List[str]:
        """Retorna uma lista de coleções de todas as fontes de dados."""
        collections = []
        for datasource in cls.manager.get_datasources():
            collections.extend(datasource.collections)
        return collections

    @classmethod
    def collections(cls) -> List[str]:
        """Retorna uma lista de coleções que pode ser iterada diretamente."""
        return cls.list_collections()  # Retorna diretamente uma lista

    @classmethod
    def get_collection(cls, id: str, collection_id: str) -> CollectionClient:
        """Retorna um CollectionClient para a coleção especificada."""
        datasource = cls.manager.get_datasource_by_id(id)
        if datasource is None:
            raise ValueError(f"Fonte de dados com ID '{id}' não encontrada.")

        return CollectionClient(datasource, collection_id)

    @classmethod
    def describe(cls, id: str, collection_id: str) -> dict:
        """Descrição detalhada de uma coleção específica."""
        datasource = cls.manager.get_datasource_by_id(id)
        if datasource is None:
            raise ValueError(f"Fonte de dados com ID '{id}' não encontrada.")

        return datasource.describe(collection_id)

    @staticmethod
    def save_feature(
        filename: str,
        gdf: gpd.geodataframe.GeoDataFrame,
        driver: str = "ESRI Shapefile",
    ):
        """Save dataset data to file.

        Args:
            filename (str): The path or filename.
            gdf (geodataframe): geodataframe to save.
            driver (str): Drive (type) of output file.
        """
        gdf.to_file(filename, encoding="utf-8", driver=driver)

    def __repr__(self):
        """Representação em string do serviço Harmonized DS."""
        return f"<HARMONIZEDS(access_token=None, sources={len(self.manager.get_datasources())})>"
