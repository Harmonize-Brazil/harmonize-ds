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

"""Command line interface for the Harmonize Datasources."""

import json

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

from .harmonize import HARMONIZEDS

# pylint: disable=too-few-public-methods
class Config:
    """A simple decorator class for command line options."""

    def __init__(self):
        """Initialize of Config decorator."""
        self.service = None


pass_config = click.make_pass_decorator(Config, ensure=True)

console = Console()


@click.group()
@click.version_option()
@pass_config
def cli(config): # pylint: disable=unused-argument
    """HARMONIZE DataSources Client on command line."""
    config.service = HARMONIZEDS


@cli.command()
@click.option("-v", "--verbose", is_flag=True, default=False)
@pass_config
def collections(config: Config, verbose):
    """Return the list of available collections."""
    if verbose:
        console.print(
            "[black]\tRetrieving the list of available collections...[/black]"
        )

    table = Table(
        title="Available Collections", show_header=True, header_style="bold magenta"
    )
    table.add_column("Id", style="green", no_wrap=True)
    table.add_column("Collection", style="green", no_wrap=True)

    for ds in config.service.collections():
        table.add_row(ds["id"], ds["collection"])

    panel = Panel(
        table,
        title="[bold green]Harmonize Datasources[/bold green]",
        expand=False,
        border_style="bright_blue",
    )

    console.print(panel)

    console.print("[black]\tFinished![/black]")


@cli.command()
@click.option("-v", "--verbose", is_flag=True, default=False)
@click.option(
    "-c", "--collection_id", required=True, type=str, help="The collection identifier"
)
@click.option("-i", "--id", required=True, type=str, help="The datasource id")
@click.option("-v", "--time", is_flag=True, default=False)
@pass_config
def describe(config: Config, verbose, collection_id, id, time):
    """Describe a collection by its source ID and collection ID."""
    if verbose:
        console.print(
            "[black]\tGet Collection Describe...[/black]"
        )
    collection = config.service.get_collection(
        id=id, collection_id=collection_id
    ).describe()

    if not collection:
        console.print(f"[bold red]Collection '{collection_id}' not found.[/bold red]")
        return

    general_table = Table.grid(padding=(0, 1))
    general_table.add_column(style="bold cyan", no_wrap=True)
    general_table.add_column()
    general_table.add_row("Title", str(collection.get("title", "")))
    general_table.add_row("Abstract", str(collection.get("abstract", "")))
    general_table.add_row("Name", str(collection.get("name", "")))
    general_table.add_row(
        "Keywords", ", ".join(map(str, collection.get("keywords", [])))
    )
    general_table.add_row(
        "Default CRS",
        ", ".join(map(str, collection.get("supportedCRS") or [])),
    )

    # BBOX
    bbox = collection.get("wgs84_bbox", {})
    if isinstance(bbox, dict):
        lower = bbox.get("lower", "")
        upper = bbox.get("upper", "")
    elif isinstance(bbox, (list, tuple)) and len(bbox) == 4:
        lower = f"{bbox[0]} {bbox[1]}"
        upper = f"{bbox[2]} {bbox[3]}"
    else:
        lower = upper = [0, 0]

    bbox_table = Table(
        title="Bounding Box (WGS 84)", show_header=True, header_style="bold magenta"
    )
    bbox_table.add_column("Corner", justify="center")
    bbox_table.add_column("Longitude", justify="right")
    bbox_table.add_column("Latitude", justify="right")

    bbox_table.add_row("Lower", f"{lower[0]:.6f}", f"{lower[1]:.6f}")
    bbox_table.add_row("Upper", f"{upper[0]:.6f}", f"{upper[1]:.6f}")

    schema_tree = Tree("[bold magenta]Schema")
    schema = collection.get("schema", {})
    if schema:
        for key, value in schema.items():
            schema_tree.add(f"[green]{key}[/green]: [white]{value}[/white]")
    else:
        schema_tree.add("[dim]No schema available[/dim]")

    geometry = collection.get("geometry", {})
    geom_tree = Tree("[bold magenta]Geometry")
    if geometry:
        for k, v in geometry.items():
            geom_tree.add(f"[green]{k}[/green]: [white]{v}[/white]")
    else:
        geom_tree.add("[dim]No geometry information[/dim]")

    time_tree = Tree("[bold magenta]Time Information")
    if "timelimits" in collection:
        time_tree.add(f"[green]Time Limits[/green]: {collection['timelimits']}")

    if time:
        if "timepositions" in collection:
            time_tree.add(
                f"[green]Time Positions[/green]: {collection['timepositions']}"
            )

    console.print(
        Panel(general_table, title="[bold green]General Info", border_style="blue")
    )
    console.print(
        Panel(bbox_table, title="[bold green]Bounding Box", border_style="blue")
    )
    console.print(schema_tree)
    console.print(geom_tree)
    if "timelimits" in collection or "timepositions" in collection:
        console.print(time_tree)

    console.print("[black]\tFinished![/black]")


@cli.command()
@click.option(
    "-c", "--collection_id", required=True, type=str, help="The collection identifier"
)
@click.option("-i", "--id", required=True, type=str, help="The datasource id")
@click.option(
    "--filename",
    type=click.STRING,
    required=True,
    help="File path or file handle to write to",
)
@click.option(
    "--filter",
    default=None,
    type=click.STRING,
    required=False,
    help="The filter to get data",
)
@click.option(
    "--driver",
    type=click.STRING,
    required=False,
    default="ESRI Shapefile",
    help="The OGR format driver used to write the vector file",
)
@click.option("-v", "--verbose", is_flag=True, default=False)
@pass_config
def download(config: Config, verbose, filter, collection_id, id, driver, filename):
    """Download and save a collection data into file."""
    if filter:
        try:
            filter = json.loads(filter)
        except json.JSONDecodeError as e:
            raise click.BadParameter(f"Filtro inválido: {e}")

    if verbose:
        console.print("\t[bold red]Retrieving the dataset ... [/]")

    collection = config.service.get_collection(id=id, collection_id=collection_id)

    if not collection:
        console.print(f"[bold red]Collection '{collection_id}' not found.[/bold red]")
        return

    gdf = collection.get(filter)

    with console.status("Salving data...", spinner="monkey"):
        config.service.save_feature(filename, gdf, driver)

    console.print(f"[bold red]Saved {collection_id}[/] in {filename}")
