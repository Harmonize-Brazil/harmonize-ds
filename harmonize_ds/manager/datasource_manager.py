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

import yaml
from typing import List, Dict, Optional
import importlib.resources
from harmonize_ds.manager.datasource_factory import DataSourceFactory
from harmonize_ds.sources.base import Source


class DataSourceManager:
    """Gerencia as fontes de dados carregadas a partir do arquivo de configuração."""

    def __init__(self, config_path: str = "config/config.yaml") -> None:
        """Inicializa o gerenciador e carrega as fontes de dados do YAML.

        Args:
            config_path (str): Caminho para o arquivo de configuração YAML.
        """
        self._datasources: List[Source] = []
        self._config_path = config_path
        self.load_all()

    def load_all(self) -> None:
        """Carrega as fontes de dados a partir do arquivo de configuração (YAML)."""
        try:
            with importlib.resources.files('harmonize_ds').joinpath('config/config.yaml').open('r') as file:
                config = yaml.safe_load(file)
        except FileNotFoundError:
            raise RuntimeError("Arquivo de configuração config.yaml não encontrado.")
        except yaml.YAMLError as e:
            raise RuntimeError(f"Erro ao carregar YAML: {e}")

        self._datasources = [
            DataSourceFactory.make(source["type"], source["id"], source["host"])
            for source in config.get("sources", [])
        ]

    def get_datasources(self) -> List[Source]:
        """Retorna todas as fontes de dados carregadas."""
        return self._datasources

    def get_datasource_by_id(self, id: str) -> Optional[Source]:
        """Retorna uma fonte de dados pelo ID ou None se não encontrada."""
        for ds in self._datasources:
            if ds._source_id == id:
                return ds
        return None  # Retorna None ao invés de lançar exceção diretamente

    def __repr__(self) -> str:
        """Representação da instância para depuração."""
        return f"<DataSourceManager {len(self._datasources)} fontes de dados carregadas>"

