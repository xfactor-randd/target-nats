"""NATS target for Singer ELT pipelines.

This module provides a Singer target implementation for publishing data streams
to NATS messaging systems. It's built using the Singer SDK and supports both
individual record and batch processing with optional schema registry
integration.

The target publishes JSON-encoded records to NATS topics constructed from
configurable prefixes and stream names. It includes built-in support for
DataDog telemetry and optional schema registration to Apicurio Registry.

Example:
    Basic usage:
        $ target-nats --config config.json

    With Meltano:
        $ meltano invoke target-nats --version

See Also:
    target_nats.sinks: Core sink implementation with async NATS publishing
"""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_nats.sinks import NatsSink


class Targetnats(Target):
    """Singer target for publishing data streams to NATS messaging systems.

    This target inherits from the Singer SDK Target base class and provides
    configuration for connecting to NATS brokers, publishing records to topics,
    and optionally registering schemas with Apicurio Registry.

    The target uses the NatsSink class to handle the actual message publishing
    with async operations for optimal performance.

    Attributes:
        name: The official name of this target plugin.
        config_jsonschema: JSON schema defining configuration parameters for
            the target.
        default_sink_class: The sink class used for processing records
            (NatsSink).

    Note:
        This target supports the following key features:
        - Async publishing to NATS brokers for high throughput
        - Configurable topic prefixes for message routing
        - Optional schema registry integration with Apicurio
        - DataDog telemetry integration for monitoring data streams
        - Batch processing for improved performance

    Example:
        Create a target instance:
            target = Targetnats(config={"nats_url": "nats://localhost:4222"})

        Run the CLI:
            Targetnats.cli()
    """

    name = "target-nats"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "nats_url",
            th.StringType,
            description="The URL of the NATS broker to connect to. "
            "Supports both 'nats://' and 'tls://' schemes for secure "
            "connections.",
            default="nats://localhost:4222",
        ),
        th.Property(
            "queue_prefix",
            th.StringType,
            description="Optional prefix prepended to all NATS topic names. "
            "Final topic format: '{queue_prefix}{stream_name}'.",
            default="",
        ),
        th.Property(
            "enable_schema_registry",
            th.StringType,
            description="Boolean flag (as string) to enable schema "
            "registration with Apicurio Registry. Set to 'true' to enable.",
        ),
        th.Property(
            "schema_registry_url",
            th.StringType,
            description="Base URL of the Apicurio Registry API endpoint. "
            "Used when enable_schema_registry is enabled.",
            default="http://localhost:8080/apis/registry/v3",
        ),
        th.Property(
            "schema_group_id",
            th.StringType,
            description="Schema group identifier used for organizing schemas "
            "in the Apicurio Registry. Groups provide namespace isolation.",
            default="default",
        ),
        th.Property(
            "schema_topic_prefix",
            th.StringType,
            description="Optional prefix for schema artifact names in the "
            " registry. Schema artifact format: "
            "'{schema_topic_prefix}{stream_name}'.",
            default="",
        ),
    ).to_dict()

    default_sink_class = NatsSink


def main() -> None:
    """Entry point for the target-nats CLI application.

    This function serves as the main entry point when the target is executed
    as a command-line application. It delegates to the Singer SDK's built-in
    CLI handler which provides standard Singer target functionality including:

    - Configuration validation and loading
    - Input stream processing from stdin
    - Error handling and logging
    - Help and version information

    The CLI supports standard Singer target options:
    - --config: Path to JSON configuration file
    - --input: Input stream file (defaults to stdin)
    - --version: Display version information
    - --help: Show usage information

    Example:
        Run with configuration file:
            $ python -m target_nats --config config.json

        Show version:
            $ python -m target_nats --version

        Process piped input:
            $ tap-csv | target-nats --config config.json

    See Also:
        target_nats.target.Targetnats.cli: The underlying CLI method from
            Singer SDK
    """
    Targetnats.cli()


if __name__ == "__main__":
    main()
