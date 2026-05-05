"""Confluent Cloud topology helpers — used by client, wizard, and demo scripts.

Intentionally dependency-free (stdlib only): the wizard imports from here on
startup and we don't want to pull in httpx / openlineage-python / pydantic
just to parse a Kafka bootstrap host.
"""

from __future__ import annotations


def parse_cloud_region(bootstrap: str) -> tuple[str, str]:
    """Extract (cloud, region) from a Confluent Cloud Kafka bootstrap host.

    Bootstrap format: pkc-XXXXXX.<region>.<cloud>.confluent.cloud:<port>
    e.g. pkc-921jm.us-east-2.aws.confluent.cloud:9092 → ("aws", "us-east-2")

    Returns ("", "") when the bootstrap doesn't match the expected shape.
    Same parser works for Schema Registry endpoint hosts (psrc-XXXXXX.…) and
    Flink endpoint hosts (flink-XXXXXX.…) since they all follow the same
    `<resource>-<id>.<region>.<cloud>.confluent.cloud` shape.
    """
    host = (bootstrap or "").split(":", 1)[0]
    parts = host.split(".")
    if len(parts) >= 5 and parts[-2] == "confluent" and parts[-1] == "cloud":
        return parts[-3], parts[-4]
    return "", ""


def flink_region_args(kafka_bootstrap: str) -> list[str]:
    """Cloud/region flags required by `confluent flink statement` commands.

    The CLI errors with "no cloud provider and region selected" unless these
    flags are passed (or the user has set a context with `confluent flink
    region use`).

    Returns [] when the bootstrap doesn't match the expected shape, in which
    case the CLI falls back to whatever region the user's context has set.
    """
    cloud, region = parse_cloud_region(kafka_bootstrap)
    if cloud and region:
        return ["--cloud", cloud, "--region", region]
    return []
