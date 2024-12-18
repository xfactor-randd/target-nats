"""nats target sink class, which handles writing streams."""

from __future__ import annotations
import asyncio
import json

from nats.aio.client import Client as NATS

import ddtrace
from ddtrace.internal.datastreams.processor import PROPAGATION_KEY_BASE_64


from singer_sdk.sinks import RecordSink


class NatsSink(RecordSink):
    """nats target sink class."""

    max_size = 10000  # Max records to write in one batch

    _nats: NATS | None = None
    _loop: asyncio.AbstractEventLoop | None = None
    _topic_prefix: str = ""

    async def _connect(self) -> None:
        """Connect to the provided NATS broker"""
        self.logger.info("NATS connected")
        await self._nats.connect(self.config.get("nats_url"))

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the sink."""
        super().__init__(*args, **kwargs)
        self.logger.info(f"Target nats initialized with {self.config}")
        self._topic_prefix = self.config.get("queue_prefix")
        self._loop = asyncio.get_event_loop()

        self._nats = NATS()
        task = self._loop.create_task(self._connect())
        self._loop.run_until_complete(task)

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """

        headers = {}
        data = json.dumps(record, default=str).encode()
        topic = f"{self._topic_prefix}{self.stream_name}"
        if ddtrace.config._data_streams_enabled:
            pathway = ddtrace.tracer.data_streams_processor.set_checkpoint(
                [
                    "type:nats",
                    f"topic:{topic}",
                    "direction:out",
                ],
                payload_size=len(data),
            )
            headers[PROPAGATION_KEY_BASE_64] = pathway.encode_b64()

        task = self._loop.create_task(
            self._nats.publish(
                topic,
                data,
                headers=headers,
            )
        )
        self._loop.run_until_complete(task)
