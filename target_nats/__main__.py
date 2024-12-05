"""nats entry point."""
from __future__ import annotations

import ddtrace.auto

from target_nats.target import Targetnats

Targetnats.cli()
