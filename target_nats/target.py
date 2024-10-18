"""nats target class."""

from __future__ import annotations
import asyncio

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_nats.sinks import (
    NatsSink,
)


class Targetnats(Target):
    """Sample target for nats."""

    name = "target-nats"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "nats_url",
            th.StringType,
            description="The URL of the NATS broker",
            default="nats://localhost:4222",
        ),
        th.Property(
            "queue_prefix",
            th.StringType,
            description="A prefix to set for publishing any messages to NATS",
            default="",
        ),
    ).to_dict()

    default_sink_class = NatsSink


def main():
    Targetnats.cli()


if __name__ == "__main__":
    main()
