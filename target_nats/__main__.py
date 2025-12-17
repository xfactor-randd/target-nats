"""Main entry point module for the NATS Singer target.

This module serves as the primary entry point when the target-nats package
is executed as a module using `python -m target_nats`. It automatically
initializes DataDog tracing and delegates to the main target CLI.

The module is designed for command-line execution and provides the standard
Singer target interface through the target_nats.target.Targetnats class.

DataDog automatic tracing is enabled via `ddtrace.auto` import, which
instruments the application for distributed tracing and monitoring when
DataDog agent is configured.

Usage:
    Execute as a module:
        python -m target_nats --config config.json

    Process Singer data from stdin:
        tap-csv | python -m target_nats --config config.json

    Show version information:
        python -m target_nats --version

    Show help:
        python -m target_nats --help

Note:
    This entry point is equivalent to calling `target-nats` directly after
    installation, but allows running the target without installation by
    executing the module directly from the source directory.

    The automatic DataDog tracing initialization happens at import time,
    ensuring all subsequent operations are traced when a DataDog agent
    is available and properly configured.

See Also:
    target_nats.target: Main target implementation with CLI interface
    target_nats.sinks: Core sink functionality for NATS publishing

Example:
    Run with configuration file:
        $ python -m target_nats --config ./config.json

    Get schema information:
        $ python -m target_nats --about --format=markdown

    Test with sample data:
        $ echo '{"type": "RECORD", "record": {"id": 1}}' | \
            python -m target_nats --config config.json
"""

from __future__ import annotations

import ddtrace.auto  # Enable automatic DataDog tracing instrumentation

from target_nats.target import Targetnats

# Execute the CLI when this module is run directly
Targetnats.cli()
