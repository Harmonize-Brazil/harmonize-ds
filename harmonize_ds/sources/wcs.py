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

import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

from lxml import etree

from ..utils import Utils
from .base import Source


class WCS(Source):
    """Python client for accessing GeoServer WCS services.

    Attributes:
        source_id (str): Data source identifier.
        url (str): URL of the WFS service.
    """

    def __init__(self, source_id: str, url: str) -> None:
        """Initializes the WCS client with the specified URL.

        Args:
            source_id (str): Unique identifier of the data source.
            url (str): URL of the WFS service.
        """
        super().__init__(source_id=source_id, url=url)
        self._base_path = "wcs?service=wcs&version=1.1.1"

    @property
    def collections(self) -> List[Dict[str, str]]:
        """Gets the list of layers available in the WFS service.

        Returns:
        List[Dict[str, str]]: List of dictionaries with identifier and collection name.
        """
        return [
            {"id": self._source_id, "collection": layer} for layer in self.list_image()
        ]

    def get_type(self) -> str:
        """Returns the data source type.

        Returns:
            str: Data source type ("WCS").
        """
        return "WCS"

    def list_image(self) -> List[str]:
        """Return the list of all available images in the service."""
        url = f"{self._url}/{self._base_path}&request=GetCapabilities&outputFormat=application/json"

        try:
            doc = Utils._get(url)
            xmldoc = etree.fromstring(doc.encode("utf-8"))
        except etree.XMLSyntaxError as e:
            print(f"Error parsing XML: {e}")
            return []

        namespaces = {
            "wcs": "http://www.opengis.net/wcs/2.0",
            "ows": "http://www.opengis.net/ows/2.0",
        }
        itemlist = xmldoc.findall(".//wcs:CoverageSummary", namespaces)

        available_images: List[str] = []
        for coverage_summary in itemlist:
            coverage_id = coverage_summary.find("wcs:CoverageId", namespaces)
            if coverage_id is not None and coverage_id.text:
                available_images.append(coverage_id.text)

        return available_images

    def get(
        self,
        collection_id: str,
        filter: Optional[Dict[str, Any]] = None,
        srid: int = 4326,
    ) -> str:
        """Build the URL to get an image (coverage) from the server based on a GetCoverage request."""
        base_url = f"{self._url}?service=WCS&version=2.0.1&request=GetCoverage&coverageId={collection_id}"
        base_url += f"&crs=EPSG:{srid}"

        if filter:
            if "bbox" in filter:
                bbox = ",".join(map(str, filter["bbox"]))
                base_url += f"&bbox={bbox}"
            if "width" in filter and "height" in filter:
                base_url += f"&width={filter['width']}&height={filter['height']}"
            if "time" in filter:
                base_url += f"&time={filter['time']}"
            if "format" in filter:
                base_url += f"&format={filter['format']}"

        return base_url

    def describe(self, collection_id: str) -> Dict:
        """Gets the schema of a specific collection.

        Args:
            collection_id (str): Collection identifier.

        Returns:
            Dict: Collection schema.
        """
        infos = self.getcapabilities(collection_id)
        coverage = self.describe_coverage(collection_id)

        metadata = {
            "title": infos.get("title"),
            "abstract": infos.get("abstract"),
            "name": infos.get("id"),
            "keywords": infos.get("keywords", []),
            "wgs84_bbox": infos.get("boundingBoxWGS84"),
            "timelimits": coverage.get("timelimits"),
            "timepositions": coverage.get("timepositions"),
            "supportedCRS": coverage.get("supportedCRS"),
        }

        return metadata

    def _extract_bbox(self, coverage_summary, ns):
        """Extract BBOX."""
        bbox_el = coverage_summary.find("ows:WGS84BoundingBox", ns)
        if bbox_el is not None:
            lower = bbox_el.findtext("ows:LowerCorner", default="", namespaces=ns)
            upper = bbox_el.findtext("ows:UpperCorner", default="", namespaces=ns)
            return {
                "lower": [float(x) for x in lower.split()],
                "upper": [float(x) for x in upper.split()],
            }
        return None

    def getcapabilities(self, collection_id: str) -> Dict[str, Any]:
        """Get Capabilities."""
        url = f"{self._url}/{self._base_path}&request=GetCapabilities"

        response = Utils._get(url)

        root = ET.fromstring(response)
        ns = {
            "wcs": "http://www.opengis.net/wcs/1.1.1",
            "ows": "http://www.opengis.net/ows/1.1",
        }

        for coverage_summary in root.findall(".//wcs:CoverageSummary", ns):
            coverage_id = coverage_summary.findtext(
                "wcs:Identifier", default="", namespaces=ns
            )

            if coverage_id == collection_id:
                return {
                    "id": coverage_id,
                    "title": coverage_summary.findtext(
                        "ows:Title", default="", namespaces=ns
                    ),
                    "abstract": coverage_summary.findtext(
                        "ows:Abstract", default="", namespaces=ns
                    ),
                    "keywords": [
                        k.text
                        for k in coverage_summary.findall(
                            "ows:Keywords/ows:Keyword", ns
                        )
                    ],
                    "boundingBoxWGS84": self._extract_bbox(coverage_summary, ns),
                }

        raise ValueError(f"Coverage '{collection_id}' not found in GetCapabilities.")

    def describe_coverage(self, collection_id: str) -> Dict[str, Any]:
        """Describe Coverage."""
        url = f"{self._url}/{self._base_path}&request=DescribeCoverage&identifiers={collection_id}"

        response = Utils._get(url)

        root = ET.fromstring(response)
        ns = {
            "wcs": "http://www.opengis.net/wcs/1.1.1",
            "ows": "http://www.opengis.net/ows/1.1",
        }

        coverage_el = root.find(".//wcs:CoverageDescription", ns)
        if coverage_el is None:
            raise ValueError(
                "CoverageDescription not found in DescribeCoverage response"
            )

        supported_crs = [el.text for el in root.findall(".//wcs:SupportedCRS", ns)]
        timepositions = [
            el.text for el in root.findall(".//wcs:TemporalDomain/wcs:timePosition", ns)
        ]

        timelimits = (
            (min(timepositions), max(timepositions)) if timepositions else (None, None)
        )

        dic = {
            "supportedCRS": supported_crs,
            "timepositions": timepositions,
            "timelimits": timelimits,
        }

        return dic
