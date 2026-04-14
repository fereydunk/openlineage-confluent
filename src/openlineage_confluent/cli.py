"""CLI entry-point:  ol-confluent [run-once | run | validate]"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

from openlineage_confluent.config import AppConfig

app = typer.Typer(
    name="ol-confluent",
    help="Bridge Confluent Cloud Stream Lineage → OpenLineage",
    add_completion=False,
)
console = Console()


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        level=level,
        stream=sys.stderr,
    )


def _load_config(config_file: Path | None) -> AppConfig:
    if config_file:
        return AppConfig.from_yaml(config_file)
    return AppConfig.from_env()


@app.command()
def run_once(
    config: Annotated[Optional[Path], typer.Option("--config", "-c")] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
) -> None:
    """Run a single poll cycle and exit."""
    _setup_logging(verbose)
    from openlineage_confluent.pipeline import LineagePipeline

    cfg = _load_config(config)
    with LineagePipeline(cfg) as pipeline:
        stats = pipeline.run_once()

    table = Table(title="Poll cycle stats")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    for k, v in stats.items():
        table.add_row(str(k), str(v))
    console.print(table)


@app.command()
def run(
    config: Annotated[Optional[Path], typer.Option("--config", "-c")] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
) -> None:
    """Run continuously, polling on the configured interval."""
    _setup_logging(verbose)
    from openlineage_confluent.pipeline import LineagePipeline

    cfg = _load_config(config)
    with LineagePipeline(cfg) as pipeline:
        pipeline.run_forever()


@app.command()
def validate(
    config: Annotated[Optional[Path], typer.Option("--config", "-c")] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
) -> None:
    """Fetch lineage graph and print a summary — no events emitted."""
    _setup_logging(verbose)
    from openlineage_confluent.confluent.client import ConfluentLineageClient

    cfg = _load_config(config)
    with ConfluentLineageClient(cfg.confluent) as client:
        graph = client.get_lineage_graph()

    console.print_json(json.dumps(graph.summary()))

    table = Table(title="Lineage edges")
    table.add_column("Job")
    table.add_column("Job Type")
    table.add_column("Source")
    table.add_column("Target")

    for edge in graph.edges:
        table.add_row(edge.job_name, edge.job_type, edge.source_name, edge.target_name)
    console.print(table)


if __name__ == "__main__":
    app()
