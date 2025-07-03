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

import gzip
import json
from io import BytesIO
from time import sleep
from typing import Any, Dict, List, Optional
from xml.dom import minidom

import geopandas as gpd
import httpx
import pyproj
from lxml import etree
from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from shapely.geometry import LineString, MultiPoint, MultiPolygon, Point, Polygon

from ..utils import Utils
from .base import Source

WFS_FORMATS = {
    "shp": "shape-zip",
    "kml": "kml",
    "csv": "csv",
    "json": "application/json",
}

console = Console()


class WFS(Source):
    """A class that describes a WFS.

    Attributes:
        source_id (str): Data source identifier.
        url (str): URL of the WFS service.
    """

    def __init__(self, source_id: str, url: str) -> None:
        """Create a WFS client attached to the given host address.

        Args:
            source_id (str): Dataset identifier.
            url (str):The base URL of the WFS service.
        """
        super().__init__(source_id=source_id, url=url)
        self._base_path = "wfs?service=wfs&version=2.0.0"

    def get_type(self) -> str:
        """Returns the data source type.

        Returns:
        str: Data source type ("WFS").
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
        """Gets the list of layers available in the WFS service.

        Returns:
        List[Dict[str, str]]: List of dictionaries with identifier and collection name.
        """
        return [
            {"id": self._source_id, "collection": layer}
            for layer in self.list_features()["features"]
        ]

    def describe_feature(self, ft_name: str):
        """Describe Feature."""
        url = f"{self._url}/{self._base_path}&request=DescribeFeatureType&typeName={ft_name}&outputFormat=application/json"
        doc = Utils._get(url)
        js = json.loads(doc)

        if not js.get("featureTypes"):
            raise ValueError("No featureTypes found in response.")

        return js

    def capabilites(self, ft_name: str):
        """Get capabilites function."""
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

    def describe(self, collection_id: str) -> Dict[str, Any]:
        """Return metadata about a specific feature type."""
        if not collection_id:
            raise ValueError("Missing feature name.")

        js = self.describe_feature(collection_id)
        capabilites = self.capabilites(collection_id)

        ft_info = js["featureTypes"][0]
        feature = {
            "name": ft_info["typeName"],
            "namespace": js.get("targetPrefix", ""),
            "full_name": f"{js.get('targetPrefix', '')}:{ft_info['typeName']}",
            "attributes": [],
            "title": capabilites["title"],
            "abstract": capabilites["abstract"],
            "bbox": capabilites["bbox"],
            "ft_name": collection_id,
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
        geometry_column: str = "geom",
    ) -> gpd.GeoDataFrame:
        """Return features from a specific feature type with pagination and progress bar."""
        if not collection_id:
            raise ValueError("Missing collection_id.")

        output_format = "application/json"

        base_url = (
            f"{self._url}/{self._base_path}&request=GetFeature&typeName={collection_id}"
            f"&outputFormat={output_format}&srsName=EPSG:{srid}"
        )
        clauses = []

        if filter:
            if "bbox" in filter:
                minx, miny, maxx, maxy = filter["bbox"]

                clauses.append(
                    f"BBOX({geometry_column}, {minx}, {miny}, {maxx}, {maxy}, 'EPSG:{srid}')"
                )

            if "date" in filter:
                date_value = filter["date"]
                if "/" in date_value:
                    start, end = date_value.split("/")
                    clauses.append(f"date BETWEEN '{start}' AND '{end}'")
                else:
                    clauses.append(f"date = '{date_value}'")

            if clauses:
                cql_filter = " AND ".join(clauses)
                base_url += f"&cql_filter={cql_filter}"

        all_features = []
        start_index = 0
        total_received = 0
        page_size = 1000

        with console.status("[bold green]Starting downloads...") as status:
            while True:
                page_url = f"{base_url}&startIndex={start_index}&count={page_size}"
                status.update(f"[bold cyan]Download data {start_index}...")
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

            if not all_features:
                return gpd.GeoDataFrame()

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

            console.log(f"[bold green]✅ Total features received: {len(all_features)}")

            df_obs = gpd.GeoDataFrame.from_dict(fc["features"])

            df_dataset_data = df_obs.set_geometry(col="geom", crs=f"EPSG:{srid}")

            return df_dataset_data
