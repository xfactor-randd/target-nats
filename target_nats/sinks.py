"""NATS sink implementation for Singer ELT pipelines.

This module provides the core sink functionality for publishing Singer records
to NATS messaging systems. It implements async-first design patterns for
optimal performance and includes comprehensive support for DataDog telemetry,
schema registry integration, and proper connection management.

The sink handles both individual record processing and batch processing, with
automatic event loop management to bridge sync/async boundaries required by
the Singer SDK framework.

Key Features:
    - Async NATS publishing with connection pooling
    - Batch processing for improved throughput
    - DataDog data streams telemetry integration
    - Schema registry support via Apicurio Registry
    - Robust error handling and connection management
    - Event loop management for sync/async bridge

Example:
    Basic usage with Singer SDK:
        sink = NatsSink(
            target=target_instance,
            stream_name="users",
            schema=user_schema,
            key_properties=["id"]
        )
        sink.process_record({"id": 1, "name": "John"}, {})

    Batch processing:
        context = {"records": [record1, record2, record3]}
        sink.process_batch(context)

See Also:
    target_nats.target: Main target class that uses this sink
    singer_sdk.sinks.RecordSink: Base class providing Singer protocol
    compliance
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import ddtrace
import requests
from ddtrace.internal.datastreams.processor import PROPAGATION_KEY_BASE_64
from nats.aio.client import Client as NATS
from singer_sdk.sinks import RecordSink


class NatsSink(RecordSink):
    """Async NATS sink for publishing Singer records to NATS messaging systems.

    This sink extends the Singer SDK's RecordSink to provide async publishing
    capabilities to NATS brokers. It manages NATS connections, handles both
    individual and batch record processing, and optionally integrates with
    DataDog telemetry and Apicurio schema registry.

    The sink uses an async-first design with proper event loop management to
    bridge the sync Singer SDK interface with async NATS operations.

    Attributes:
        max_size: Maximum number of records to process in a single batch
        _nats: The NATS client instance for broker communication.
        _loop: Event loop for managing async operations.
        _topic_prefix: Prefix applied to all NATS topic names.
        _connection_ready: Flag indicating whether NATS connection is
            established.

    Args:
        target: The parent Singer target instance containing configuration.
        stream_name: Name of the data stream being processed.
        schema: JSON schema defining the structure of records in this stream.
        key_properties: List of field names that serve as primary keys.

    Note:
        The sink automatically manages NATS connections and event loops.
        Connection establishment is lazy and happens during the first record
        processing attempt.

        Topic names are constructed as: `{queue_prefix}{stream_name}` where
        queue_prefix comes from the target configuration.

        When DataDog data streams are enabled, the sink automatically adds
        telemetry checkpoints and propagation headers to track data lineage.

    Example:
        Initialize sink within a target:
            sink = NatsSink(
                target=self,
                stream_name="user_events",
                schema={"type": "object", "properties": {...}},
                key_properties=["user_id", "event_id"]
            )

        Process individual record:
            record = {
                "user_id": 123,
                "event": "login",
                "timestamp": "2023-01-01T00:00:00Z"
            }
            sink.process_record(record, context={})

        Process batch of records:
            context = {"records": [record1, record2, record3]}
            sink.process_batch(context)
    """

    max_size = 10000  # Max records to write in one batch

    _nats: NATS | None = None
    _loop: asyncio.AbstractEventLoop | None = None
    _topic_prefix: str = ""
    _connection_ready: bool = False

    async def _connect(self) -> None:
        """Establish connection to the configured NATS broker.

        This method implements connection establishment with the NATS broker
        using the configured URL. It includes connection state management to
        prevent duplicate connections and proper error handling.

        The connection is established lazily - it's only created when needed
        for the first publish operation. Once established, the connection is
        reused for all subsequent operations.

        Raises:
            Exception: If connection to the NATS broker fails. The original
                exception is logged and re-raised for upstream handling.

        Note:
            This method is idempotent - calling it multiple times when already
            connected will return immediately without creating additional
            connections.

            The NATS URL is retrieved from the target configuration under the
            'nats_url' key, defaulting to 'nats://localhost:4222'.

        Example:
            Manual connection (typically not needed):
                await sink._connect()
        """
        if self._connection_ready:
            return

        if not self._nats:
            self._nats = NATS()

        try:
            await self._nats.connect(self.config.get("nats_url"))
            self._connection_ready = True
            self.logger.info("NATS connection established")
        except Exception:
            self.logger.exception("failed to connect to NATS")
            raise

    def _get_or_create_event_loop(self) -> asyncio.AbstractEventLoop:
        """Get the current event loop or create a new one if needed.

        This method handles the complex task of managing asyncio event loops
        within the Singer SDK's synchronous framework. It attempts to use an
        existing running loop if available, or creates and manages a new loop
        if none exists.

        Returns:
            The event loop to use for async operations. Either the currently
            running loop or a newly created one.

        Note:
            This method implements a critical bridge between the synchronous
            Singer SDK interface and the asynchronous NATS operations. It
            handles several scenarios:

            1. Running loop exists: Returns the existing loop
            2. No loop running: Creates a new loop and sets it as the current
                loop
            3. Previous loop closed: Creates a replacement loop

            The created loop is stored in self._loop for reuse across
            operations.

        Example:
            Get loop for async operations:
                loop = sink._get_or_create_event_loop()
                loop.run_until_complete(async_operation())
        """
        try:
            loop = asyncio.get_running_loop()
            return loop
        except RuntimeError:
            if self._loop is None or self._loop.is_closed():
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
            return self._loop

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the NATS sink with configuration and async setup.

        This constructor initializes the sink with the provided configuration,
        sets up the NATS client, manages the event loop, and initiates the
        connection to the NATS broker.

        Args:
            *args: Positional arguments passed to the parent RecordSink
                constructor. Typically includes target, stream_name, schema,
                and key_properties.
            **kwargs: Keyword arguments passed to the parent RecordSink
                constructor.

        Note:
            The initialization process includes several key steps:

            1. Call parent class constructor to set up Singer SDK functionality
            2. Extract topic prefix from configuration
            3. Initialize NATS client instance
            4. Set up or retrieve event loop for async operations
            5. Establish initial connection to NATS broker

            The connection establishment is handled differently depending on
            whether an event loop is already running (uses asyncio.create_task)
            or not (uses loop.run_until_complete).

        Example:
            Typical initialization within Singer target:
                sink = NatsSink(
                    target=self,
                    stream_name="events",
                    schema=event_schema,
                    key_properties=["event_id"]
                )
        """
        super().__init__(*args, **kwargs)
        self.logger.info(f"Target nats initialized with {self.config}")
        self._topic_prefix = self.config.get("queue_prefix", "")

        self._nats = NATS()

        self._loop = self._get_or_create_event_loop()

        if not self._loop.is_running():
            self._loop.run_until_complete(self._connect())
        else:
            _ = asyncio.create_task(self._connect())

    def setup(self) -> None:
        """Perform stream setup operations including optional schema
        registration.

        This method is called once per stream to perform any necessary setup
        operations. Currently, it handles registration of the stream's JSON
        schema with an Apicurio Registry if schema registry is enabled in
        the configuration.

        The schema registration process includes:
        1. Check if schema registry is enabled via configuration
        2. Serialize the stream schema to JSON format
        3. Construct schema artifact name from prefix and stream name
        4. Create the schema group if it doesn't exist
        5. Register the schema artifact with versioning support

        Raises:
            requests.RequestException: If any HTTP requests to the schema
                registry fail.
            json.JSONEncodeError: If the stream schema cannot be serialized
                to JSON.

        Note:
            Schema registry integration is optional and controlled by the
            'enable_schema_registry' configuration parameter. When enabled,
            schemas are registered using the Apicurio Registry REST API.

            The artifact ID is constructed as:
            `{schema_topic_prefix}{stream_name}` where schema_topic_prefix
            comes from configuration.

            The registration uses the 'FIND_OR_CREATE_VERSION' policy, which
            means it will reuse existing schema versions if they match exactly,
            or create a new version if the schema has changed.

        Example:
            Setup is called automatically by the Singer SDK:
                # SDK calls this method internally
                sink.setup()

            Manual setup (typically not needed):
                sink.setup()  # Registers schema if configured
        """
        enable_registry = bool(
            self.config.get(
                "enable_schema_registry",
                "",
            )
        )
        if enable_registry:
            self.logger.debug("Submitting schema to registry")
            schema_json = json.dumps(self.schema, default=str)
            schema_prefix = self.config.get("schema_topic_prefix")
            topic = f"{schema_prefix}{self.stream_name}"
            base_url = self.config.get("schema_registry_url")
            group_id = self.config.get("schema_group_id", "test")
            resp = requests.get(
                f"{base_url}/groups/{group_id}",
                timeout=2,
            )
            if resp.status_code == 404:
                requests.post(
                    f"{base_url}/groups",
                    timeout=2,
                    json={
                        "groupId": group_id,
                    },
                )

            requests.post(
                (
                    f"{base_url}/groups/{group_id}/artifacts"
                    "?canonical=true&ifExists=FIND_OR_CREATE_VERSION"
                ),
                timeout=2,
                json={
                    "artifactId": topic,
                    "artifactType": "JSON",
                    "firstVersion": {
                        "content": {
                            "contentType": "application/json",
                            "content": schema_json,
                        },
                    },
                },
            )

    async def _publish_record_async(
        self, record: dict[str, Any], context: dict[str, Any]
    ) -> None:
        """Asynchronously publish a single record to NATS with telemetry
        support.

        This method handles the core record publishing functionality, including
        JSON serialization, topic construction, DataDog telemetry integration,
        and error handling. It ensures the NATS connection is established
        before attempting to publish.

        Args:
            record: The individual record to publish. Will be JSON-serialized
                before sending to NATS. All values must be JSON-serializable.
            context: Stream processing context dictionary provided by the
                Singer SDK. Contains partition information and other metadata.

        Raises:
            Exception: If the NATS publish operation fails. The original
                exception is logged and re-raised for upstream handling.
            json.JSONEncodeError: If the record cannot be serialized to JSON.

        Note:
            The publishing process includes several steps:

            1. Ensure NATS connection is established
            2. Serialize record to JSON and encode as UTF-8 bytes
            3. Construct topic name from prefix and stream name
            4. Add DataDog telemetry headers if data streams are enabled
            5. Publish message to NATS with headers
            6. Log success or handle/log errors

            DataDog telemetry integration creates checkpoints for data stream
            monitoring with tags indicating NATS type, topic name, and
            direction.

            The topic name follows the format: `{queue_prefix}{stream_name}`

        Example:
            Publish a user record:
                record = {
                    "id": 123,
                    "name": "John Doe",
                    "email": "john@example.com"
                }
                await sink._publish_record_async(record, {})

            Publish with context:
                context = {"partition": 0, "stream_map": {...}}
                await sink._publish_record_async(record, context)
        """
        # Ensure connection is established
        await self._connect()

        headers = {}
        data = json.dumps(record, default=str).encode()
        topic = f"{self._topic_prefix}{self.stream_name}"

        # Add DataDog telemetry if enabled
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

        try:
            await self._nats.publish(topic, data, headers=headers)
            self.logger.debug(f"Published record to topic: {topic}")
        except Exception as e:
            self.logger.error(f"Failed to publish record to {topic}: {e}")
            raise

    def process_record(
        self,
        record: dict[str, Any],
        context: dict[str, Any],
    ) -> None:
        """Process a single record by publishing it to NATS.

        This is the main sync interface method called by the Singer SDK to
        process individual records. It bridges the synchronous SDK interface
        with the asynchronous NATS publishing operations.

        Args:
            record: Individual record from the data stream to be published.
                Must contain JSON-serializable data.
            context: Stream processing context provided by the Singer SDK.
                Contains metadata about the stream and processing state.

        Note:
            This method handles the critical sync/async bridge by managing how
            the async publish operation is executed within the sync Singer
            framework:

            1. If no event loop exists, the method returns early
            2. If an event loop is running, it schedules the publish as a task
            3. If no loop is running, it executes the publish synchronously

            The actual publishing work is delegated to _publish_record_async()
            which handles connection management, serialization, and NATS
            operations.

        Example:
            Called automatically by Singer SDK:
                # SDK calls this for each record
                sink.process_record({"id": 1, "name": "John"}, stream_context)

            Manual processing (testing):
                record = {"user_id": 123, "action": "login"}
                context = {"stream": "user_events"}
                sink.process_record(record, context)

        See Also:
            _publish_record_async: The async method that performs actual
                publishing
            process_batch: Batch processing method for multiple records
        """
        # Handle async publishing within sync context
        if not self._loop:
            return

        if self._loop.is_running():
            # If event loop is running, schedule the task
            task = asyncio.create_task(
                self._publish_record_async(record, context),
            )

        else:
            # If no loop is running, we can safely run until complete
            self._loop.run_until_complete(
                self._publish_record_async(record, context),
            )

    async def _publish_batch_async(
        self, records: list[dict[str, Any]], context: dict[str, Any]
    ) -> None:
        """Asynchronously publish a batch of records to NATS with concurrent
        processing.

        This method provides high-throughput batch processing by publishing
        multiple records concurrently using asyncio.gather(). All records
        are processed in parallel, significantly improving performance for
        large batches compared to sequential processing.

        Args:
            records: List of records to publish in this batch. Each record must
                contain JSON-serializable data.
            context: Stream processing context dictionary provided by the
                Singer SDK. Shared across all records in the batch.

        Raises:
            Exception: If any record in the batch fails to publish. The first
                exception encountered will be raised, potentially canceling
                remaining operations.

        Note:
            This method implements high-performance batch processing by:

            1. Ensuring NATS connection is established once for the entire batch
            2. Creating concurrent async tasks for each record
            3. Using asyncio.gather() to execute all tasks in parallel
            4. Providing comprehensive error handling and logging

            All records in the batch share the same processing context and are
            published to the same NATS topic (determined by stream name and
            prefix).

            The concurrent approach can significantly improve throughput,
            especially when network latency is a factor, as multiple publish
            operations can be in flight simultaneously.

        Example:
            Publish a batch of user records:
                records = [
                    {"id": 1, "name": "Alice"},
                    {"id": 2, "name": "Bob"},
                    {"id": 3, "name": "Carol"}
                ]
                await sink._publish_batch_async(records, context)

            Large batch processing:
                # Process 1000 records concurrently
                await sink._publish_batch_async(large_record_list, context)

        See Also:
            _publish_record_async: Single record publishing method used
                internally
            process_batch: Synchronous batch processing interface
        """
        # Ensure connection is established
        await self._connect()

        # Create concurrent tasks for all records
        tasks = []
        for record in records:
            task = self._publish_record_async(record, context)
            tasks.append(task)

        # Wait for all tasks to complete
        try:
            await asyncio.gather(*tasks)
            self.logger.debug(f"Published batch of {len(records)} records")
        except Exception:
            self.logger.exception("failed to publish batch")
            raise

    def process_batch(self, context: dict[str, Any]) -> None:
        """Process a batch of records efficiently using concurrent NATS
        publishing.

        This method provides the synchronous interface for batch processing
        required by the Singer SDK. It extracts records from the context and
        delegates to the async batch publishing method for optimal performance.

        Args:
            context: Stream processing context containing batch records and
                metadata. Expected to have a 'records' key with a list of
                records to process.

        Note:
            This method implements efficient batch processing by:

            1. Extracting records from the context dictionary
            2. Validating that records exist before processing
            3. Bridging sync/async boundaries for the async publish operations
            4. Providing comprehensive logging for batch operations

            The method handles the sync/async bridge similarly to
            process_record(), either scheduling a task if an event loop is
            running or executing synchronously if no loop is active.

            Batch processing is significantly more efficient than individual
            record processing for large datasets as it enables concurrent
            publishing of multiple records to NATS.

        Example:
            Called by Singer SDK with batch context:
                context = {
                    "records": [
                        {"id": 1, "name": "Alice"},
                        {"id": 2, "name": "Bob"},
                        {"id": 3, "name": "Carol"}
                    ],
                    "stream": "users"
                }
                sink.process_batch(context)

            Empty batch handling:
                context = {"records": []}  # No records to process
                sink.process_batch(context)  # Returns early

        See Also:
            _publish_batch_async: Async method that performs the actual batch
                publishing
            process_record: Single record processing method
        """
        records = context.get("records", [])
        if not records:
            return

        self.logger.info(f"Processing batch of {len(records)} records")

        # Handle async batch publishing within sync context
        if self._loop.is_running():
            # If event loop is running, schedule the batch task
            _ = asyncio.create_task(
                self._publish_batch_async(records, context),
            )
        else:
            # If no loop is running, we can safely run until complete
            self._loop.run_until_complete(
                self._publish_batch_async(records, context),
            )

    async def _disconnect_async(self) -> None:
        """Asynchronously close the NATS connection and clean up resources.

        This method handles the proper shutdown of the NATS connection,
        ensuring that all resources are cleaned up gracefully. It includes
        error handling to prevent exceptions during cleanup from propagating.

        Note:
            This method is typically called during sink destruction or explicit
            cleanup operations. It ensures that:

            1. The NATS connection is properly closed
            2. The connection state is updated to reflect disconnection
            3. Any errors during cleanup are logged but not raised
            4. Resources are released to prevent memory leaks

            The method is idempotent - calling it multiple times or on an
            already disconnected sink will not cause errors.

        Example:
            Manual cleanup (typically not needed):
                await sink._disconnect_async()

            Cleanup during shutdown:
                # Called automatically during sink destruction
                del sink
        """
        if self._nats and self._connection_ready:
            try:
                await self._nats.close()
                self._connection_ready = False
                self.logger.info("NATS connection closed")
            except Exception:
                self.logger.exception("error closing NATS connection")

    def __del__(self) -> None:
        """Destructor that ensures proper cleanup of NATS connection and
        resources.

        This method is called when the sink object is being garbage collected.
        It ensures that NATS connections are properly closed and event loops
        are cleaned up to prevent resource leaks.

        Note:
            The destructor handles several cleanup scenarios:

            1. If connection is active and event loop exists, schedules cleanup
            2. Handles both running and non-running event loops appropriately
            3. Catches and logs any cleanup errors to prevent crashes

            This is a safety mechanism - proper cleanup should ideally be
            handled explicitly before object destruction, but this ensures
            resources are released even if explicit cleanup is missed.

            The method uses different approaches based on event loop state:
            - Running loop: Schedules cleanup as a task
            - Non-running loop: Executes cleanup synchronously

        Example:
            Automatic cleanup during garbage collection:
                sink = NatsSink(...)
                # ... use sink ...
                del sink  # Triggers __del__ and cleanup

            Implicit cleanup when going out of scope:
                def process_data():
                    sink = NatsSink(...)
                    # ... process data ...
                # sink.__del__ called automatically when function ends

        Warning:
            Relying solely on __del__ for cleanup is not recommended as Python's
            garbage collection timing is not guaranteed. Explicit cleanup is
            preferred.
        """
        if (
            self._connection_ready
            and self._loop
            and not self._loop.is_closed()
        ):
            try:
                if self._loop.is_running():
                    # Schedule cleanup if loop is running
                    asyncio.create_task(self._disconnect_async())
                else:
                    # Run cleanup if loop is not running
                    self._loop.run_until_complete(self._disconnect_async())
            except Exception as e:
                self.logger.error(f"Error during cleanup: {e}")
