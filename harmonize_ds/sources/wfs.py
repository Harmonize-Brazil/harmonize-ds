import gzip
import json
from io import BytesIO
from time import sleep

import geopandas as gpd
import httpx
import pyproj
from lxml import etree
from rich.console import Console
from rich.progress import (BarColumn, DownloadColumn, Progress, TextColumn,
                           TimeElapsedColumn, TimeRemainingColumn,
                           TransferSpeedColumn)
from shapely.geometry import (LineString, MultiPoint, MultiPolygon, Point,
                              Polygon)

from .base import Source
from ..utils import Utils

console = Console()


from typing import Any, Dict, List, Optional, Tuple, Union
from xml.dom import minidom



WFS_FORMATS = {
    "shp": "shape-zip",
    "kml": "kml",
    "csv": "csv",
    "json": "application/json",
}


class WFS(Source):
    """A class that describes a WFS."""

    def __init__(self, source_id: str, url: str) -> None:
        """Create a WFS client attached to the given host address.

        Args:
            source_id (str): Dataset identifier.
            url (str):The base URL of the WFS service.
        """
        super().__init__(source_id=source_id, url=url)
        self._base_path = "wfs?service=wfs&version=2.0.0"

    def get_type(self) -> str:
        """Retorna o tipo da fonte de dados.

        Returns:
            str: Tipo da fonte de dados ("WFS").
        """
        return "WFS"

    def _get(self, url: str) -> str:
        """Exec GET request with httpx."""
        try:
            response = httpx.get(url, timeout=30.0)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(
                f"Error HTTP {exc.response.status_code}: {exc.response.text}"
            ) from exc
        except httpx.RequestError as exc:
            raise RuntimeError(f"Connection Error With WFS : {exc}") from exc

        return response.text

    def list_features(self) -> Dict[str, List[str]]:
        """Return the list of features available from the WFS."""
        url = f"{self._url}/{self._base_path}&request=GetCapabilities&outputFormat=application/json"
        doc = Utils._get(url)

        xmldoc = minidom.parseString(doc)
        itemlist = xmldoc.getElementsByTagName("FeatureType")

        features = {"features": []}
        for s in itemlist:
            features["features"].append(s.childNodes[0].firstChild.nodeValue)

        return features

    @property
    def collections(self) -> List[Dict[str, str]]:
        """Obtém a lista de camadas disponíveis no serviço WFS.

        Returns:
            List[Dict[str, str]]: Lista de dicionários com identificador e nome da coleção.
        """
        return [
            {"id": self._source_id, "collection": layer}
            for layer in self.list_features()["features"]
        ]

    def describe_feature(self, ft_name: str):

        url = f"{self._url}/{self._base_path}&request=DescribeFeatureType&typeName={ft_name}&outputFormat=application/json"
        doc = self._get(url)
        js = json.loads(doc)

        if not js.get("featureTypes"):
            raise ValueError("No featureTypes found in response.")

        return js

    def _extract_epsg_code(srs: str) -> int | None:
        if srs and "EPSG" in srs:
            try:
                return int(srs.split(":")[-1])
            except ValueError:
                return None
        return None

    def capabilites(self, ft_name: str):
        url = f"{self._url}/{self._base_path}&request=GetCapabilities&outputFormat=application/json"
        doc = self._get(url)
        tree = etree.fromstring(doc.encode())
        ns = {
            "wfs": "http://www.opengis.net/wfs/2.0",
            "ows": "http://www.opengis.net/ows/1.1",  # versão correta para OWS
        }

        xpath_expr = f".//wfs:FeatureType[wfs:Name='{ft_name}']"
        feature_el = tree.find(xpath_expr, namespaces=ns)

        if feature_el is None:
            return None

        return {
            "name": ft_name,
            "title": feature_el.findtext("wfs:Title", namespaces=ns),
            "abstract": feature_el.findtext("wfs:Abstract", namespaces=ns),
            "srs": feature_el.findtext("wfs:DefaultCRS", namespaces=ns),
            "bbox": {
                "lower": feature_el.findtext(
                    "ows:WGS84BoundingBox/ows:LowerCorner", namespaces=ns
                ),
                "upper": feature_el.findtext(
                    "ows:WGS84BoundingBox/ows:UpperCorner", namespaces=ns
                ),
            },
        }

    def describe(self, ft_name: str) -> Dict[str, Any]:
        """Return metadata about a specific feature type."""
        if not ft_name:
            raise ValueError("Missing feature name.")

        js = self.describe_feature(ft_name)
        capabilites = self.capabilites(ft_name)

        ft_info = js["featureTypes"][0]
        feature = {
            "name": ft_info["typeName"],
            "namespace": js.get("targetPrefix", ""),
            "full_name": f"{js.get('targetPrefix', '')}:{ft_info['typeName']}",
            "attributes": [],
            "title": capabilites["title"],
            "abstract": capabilites["abstract"],
            "bbox": capabilites["bbox"],
            "ft_name": ft_name,
        }

        supported_geometries = {"gml:MultiPolygon", "gml:Point", "gml:Polygon"}

        for prop in ft_info.get("properties", []):
            attr = {
                "name": prop["name"],
                "localtype": prop.get("localType"),
                "type": prop.get("type"),
            }
            feature["attributes"].append(attr)
            if prop.get("type") in supported_geometries:
                feature["geometry"] = attr

        return feature

    def build_cql_filter(self, filter: dict) -> str:

        clauses = []

        # CQL para data
        if "date" in filter:
            date_value = filter["date"]
            if "/" in date_value:
                start, end = date_value.split("/")
                clause = f"date BETWEEN '{start}' AND '{end}'"
            else:
                clause = f"date = '{date_value}'"
            clauses.append(clause)

        return " AND ".join(clauses)

    def get_dataset(
        self,
        ft_name: str,
        max_features: Optional[int] = None,
        bbox: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None,
        output: str = "json",
        page_size: int = 1000,
        epsg: int = 4326,
    ) -> gpd.GeoDataFrame:
        """Return features from a specific feature type with pagination and progress bar."""
        if not ft_name:
            raise ValueError("Missing feature name.")

        output_format = WFS_FORMATS.get(output.lower(), "application/json")
        base_url = (
            f"{self._url}/{self._base_path}&request=GetFeature&typeName={ft_name}"
            f"&outputFormat={output_format}&srsName=EPSG:{epsg}"
        )

        if bbox:
            base_url += f"&bbox={bbox}"

        # Gerar a parte do filtro se for fornecido
        if filter:
            cql_filter = self.build_cql_filter(filter)
            if cql_filter:
                base_url += f"&cql_filter={cql_filter}"

        all_features = []
        start_index = 0
        total_received = 0
        total_features = max_features if max_features else float("inf")

        # Barra de progresso simples com rich
        with console.status("[bold green]Iniciando downloads...") as status:
            while True:
                page_url = f"{base_url}&startIndex={start_index}&count={page_size}"
                status.update(
                    f"[bold cyan]Download data {start_index}..."
                )  # Atualiza a descrição com o índice
                sleep(1)  # Simula o download de cada página

                doc = self._get(page_url)

                try:
                    data = json.loads(doc)
                except Exception as e:
                    raise RuntimeError(f"Falha ao decodificar JSON: {e}")

                features = data.get("features", [])
                received = len(features)

                if not features:
                    console.log("[yellow]⚠ Encerrando...")
                    break

                all_features.extend(features)
                total_received += received

                if max_features and total_received >= max_features:
                    console.log("[green]✔ Limite máximo de features atingido.")
                    break

                start_index += received

            if max_features:
                all_features = all_features[:max_features]

            fc = dict()

            fc["features"] = []

            for item in all_features:
                if item["geometry"]["type"] == "Point":
                    feature = {
                        "geom": Point(
                            item["geometry"]["coordinates"][0],
                            item["geometry"]["coordinates"][1],
                        )
                    }
                elif item["geometry"]["type"] == "MultiPoint":
                    points = []
                    for point in item["geometry"]["coordinates"]:
                        points += [Point(point)]
                    feature = {"geom": MultiPoint(points)}

                elif item["geometry"]["type"] == "LineString":
                    feature = {"geom": LineString(item["geometry"]["coordinates"])}

                elif item["geometry"]["type"] == "MultiPolygon":
                    polygons = []
                    for polygon in item["geometry"]["coordinates"]:
                        polygons += [Polygon(lr) for lr in polygon]
                    feature = {"geom": MultiPolygon(polygons)}

                elif item["geometry"]["type"] == "Polygon":
                    feature = {"geom": Polygon(item["geometry"]["coordinates"][0])}

                else:
                    raise Exception("Unsupported geometry type.")

                if "bbox" in item["properties"]:
                    del item["properties"]["bbox"]

                feature.update(item["properties"])

                fc["features"].append(feature)

            fc["crs"] = data["crs"]

            console.log(
                f"[bold green]✅ Total de features recebidas: {len(all_features)}"
            )

            df_obs = gpd.GeoDataFrame.from_dict(fc["features"])


            df_dataset_data = df_obs.set_geometry(col="geom", crs=f"EPSG:{epsg}")

            return df_dataset_data
