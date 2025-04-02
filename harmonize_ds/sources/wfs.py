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
        doc = Utils._get(url)
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
        doc = Utils._get(url)
        tree = etree.fromstring(doc.encode())
        ns = {
            "wfs": "http://www.opengis.net/wfs/2.0",
            "ows": "http://www.opengis.net/ows/1.1",
        }

        xpath_expr = f".//wfs:FeatureType[wfs:Name='{ft_name}']"
        feature_el = tree.find(xpath_expr, namespaces=ns)

        if feature_el is None:
            raise ValueError("Feature not found in capabilities") 

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

    
    def get(
        self,
        collection_id: str,
        filter: Optional[Dict[str, Any]] = None,
        srid: int = 4326,
    ) -> gpd.GeoDataFrame:
        """Return features from a specific feature type with pagination and progress bar."""
        if not collection_id:
            raise ValueError("Missing collection_id.")

        output_format = "application/json"

        base_url = (
            f"{self._url}/{self._base_path}&request=GetFeature&typeName={collection_id}"
            f"&outputFormat={output_format}&srsName=EPSG:{srid}"
        )

        if filter:
            if "bbox" in filter:
                bbox = ",".join(map(str, filter["bbox"]))
                base_url += f"&bbox={bbox}"

            if "date" in filter:
                clauses = []
                date_value = filter["date"]
                if "/" in date_value:
                    start, end = date_value.split("/")
                    clause = f"date BETWEEN '{start}' AND '{end}'"
                else:
                    clause = f"date = '{date_value}'"
                clauses.append(clause)

                cql_filter = " AND ".join(clauses)

                base_url += f"&cql_filter={cql_filter}"
        all_features = []
        start_index = 0
        total_received = 0
        page_size = 1000

        with console.status("[bold green]Starting downloads...") as status:
            while True:
                page_url = f"{base_url}&startIndex={start_index}&count={page_size}"
                status.update(
                    f"[bold cyan]Download data {start_index}..."
                )
                sleep(1)

                doc = Utils._get(page_url)

                try:
                    data = json.loads(doc)
                except Exception as e:
                    raise RuntimeError(f"Error in JSON: {e}")

                features = data.get("features", [])
                received = len(features)

                if not features:
                    console.log("[yellow]⚠ Finishing...")
                    break

                all_features.extend(features)
                total_received += received

                start_index += received

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
                f"[bold green]✅ Total features received: {len(all_features)}"
            )

            df_obs = gpd.GeoDataFrame.from_dict(fc["features"])


            df_dataset_data = df_obs.set_geometry(col="geom", crs=f"EPSG:{srid}")

            return df_dataset_data
