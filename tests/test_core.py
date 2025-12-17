"""Comprehensive test suite for the NATS Singer target implementation.

This module provides extensive testing coverage for the target-nats package,
including both standard Singer SDK compliance tests and custom async-specific
functionality tests. The tests cover NATS connection management, record
publishing, batch processing, schema registry integration, and error handling.

The test suite is organized into several key areas:
- Standard Singer SDK target tests using built-in test framework
- NATS sink async functionality tests with mocked connections
- Schema registry integration tests
- Concurrent publishing and batch processing tests
- Event loop management and connection lifecycle tests

Test Configuration:
    Tests use mock NATS connections to avoid requiring actual NATS
    infrastructure during testing. Schema registry tests mock HTTP requests
    to Apicurio Registry.

    Key test configurations include:
    - Basic NATS configuration without schema registry
    - Enhanced configuration with schema registry enabled
    - Mock clients for NATS and HTTP operations
    - Sample schemas and records for testing data flow

Dependencies:
    - pytest: Test framework with async support
    - unittest.mock: Mocking framework for external dependencies
    - singer_sdk.testing: Built-in Singer SDK test utilities
    - asyncio: Async operation testing support

Running Tests:
    Execute all tests:
        $ pytest tests/test_core.py

    Run with verbose output:
        $ pytest tests/test_core.py -v

    Run specific test class:
        $ pytest tests/test_core.py::TestNatsSinkAsync

See Also:
    target_nats.target: Target class being tested
    target_nats.sinks: Sink class with extensive async test coverage
"""

from __future__ import annotations

import asyncio
import typing as t
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from singer_sdk.testing import get_target_test_class

from target_nats.sinks import NatsSink
from target_nats.target import Targetnats

# Test configuration for basic NATS functionality without schema registry
SAMPLE_CONFIG: dict[str, t.Any] = {
    "nats_url": "nats://localhost:4222",
    "queue_prefix": "test.",
    "enable_schema_registry": False,
}

# Enhanced configuration with schema registry enabled for testing registry
# features
SAMPLE_CONFIG_WITH_REGISTRY: dict[str, t.Any] = {
    **SAMPLE_CONFIG,
    "enable_schema_registry": True,
    "schema_registry_url": "http://localhost:8080/apis/registry/v3",
}


# Create standard Singer SDK target tests using the built-in test framework
StandardTargetTests = get_target_test_class(
    target_class=Targetnats,
    config=SAMPLE_CONFIG,
)


class TestTargetnats(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Singer SDK target compliance tests.

    This test class inherits from the Singer SDK's built-in target test suite,
    which provides comprehensive validation of standard Singer target behavior.
    These tests ensure the target properly implements the Singer specification
    for data processing, configuration handling, and protocol compliance.

    The tests automatically validate:
    - Configuration schema validation
    - Input stream processing
    - Record handling and validation
    - Error conditions and edge cases
    - CLI interface functionality
    - Standard Singer protocol compliance

    Note:
        These tests use the SAMPLE_CONFIG which disables schema registry to
        focus on core NATS functionality without external dependencies during
        testing.

        The tests run against mock NATS infrastructure to ensure they can
        execute in any environment without requiring actual NATS broker setup.

    See Also:
        singer_sdk.testing.get_target_test_class: Framework for generating
            target tests
        TestNatsSinkAsync: Custom async functionality tests
        TestAsyncIntegration: Integration tests for concurrent operations
    """

    @pytest.fixture(scope="class")
    def resource(self):  # noqa: ANN201
        """Generic external resource fixture for test setup and teardown.

        This fixture provides a placeholder for external resource management
        during testing. It can be customized to set up and tear down resources
        like temporary directories, database connections, or external services.

        Returns:
            A simple string resource for basic testing. In practice, this
            could return database connections, file handles, or other resources.

        Note:
            This fixture uses class scope, meaning the resource is created once
            per test class and shared across all test methods in the class.

            For the NATS target tests, external resources are primarily handled
            through mocking, so this fixture returns a simple placeholder value.

            The Singer SDK testing framework may use this fixture for resource
            validation in certain test scenarios.

        Example:
            Customize for specific resources:
                @pytest.fixture(scope="class")
                def resource(self):
                    # Set up test database, NATS broker, etc.
                    return setup_test_infrastructure()

        See Also:
            https://github.com/meltano/sdk/tree/main/tests/samples: Example
                usage patterns
        """
        return "resource"


class TestNatsSinkAsync:
    """Comprehensive async functionality tests for NatsSink.

    This test class focuses specifically on the asynchronous aspects of the
    NatsSink implementation, including connection management, record publishing,
    batch processing, and proper cleanup. All tests use mocked NATS clients
    to avoid requiring actual NATS infrastructure.

    The tests cover critical async functionality including:
    - NATS connection establishment and management
    - Individual record publishing with telemetry
    - Batch publishing with concurrent processing
    - Event loop management and sync/async bridging
    - Connection lifecycle and cleanup operations
    - Error handling in async contexts

    Test Structure:
        Tests use pytest fixtures to provide consistent mock objects and sample
        data across test methods. The mock NATS client simulates real NATS
        operations without requiring external dependencies.

        All async tests are marked with @pytest.mark.asyncio to ensure proper
        async test execution within the pytest framework.

    See Also:
        TestAsyncIntegration: Integration tests for concurrent operations
        target_nats.sinks.NatsSink: The class being tested
    """

    @pytest.fixture
    def mock_nats_client(self):
        """Create a comprehensive mock NATS client for testing.

        Returns:
            A mock NATS client with all necessary methods mocked as async
            operations. Includes connect, publish, and close methods.

        Note:
            This fixture provides a fully mocked NATS client that simulates
            real NATS operations without requiring actual network connections.
            All methods return successfully by default, allowing tests to focus
            on sink behavior rather than NATS client functionality.

            The mock supports all operations used by NatsSink:
            - connect(): Simulates broker connection
            - publish(): Simulates message publishing
            - close(): Simulates connection cleanup

        Example:
            Use in test methods:
                def test_something(self, mock_nats_client):
                    # mock_nats_client.publish will be called and mocked
                    await sink.publish_record(record, context)
                    mock_nats_client.publish.assert_called_once()
        """
        mock_client = AsyncMock()
        mock_client.connect = AsyncMock()
        mock_client.publish = AsyncMock()
        mock_client.close = AsyncMock()
        return mock_client

    @pytest.fixture
    def sample_schema(self):
        """Provide a sample JSON schema for testing stream validation.

        Returns:
            A JSON schema defining a simple record structure with id,
            name, and timestamp fields. Follows JSON Schema specification.

        Note:
            This schema represents a typical data record structure that might
            be processed through a Singer pipeline. It includes:
            - Integer ID field for record identification
            - String name field for textual data
            - ISO datetime timestamp field with format validation

            The schema is used in sink initialization and schema registry tests
            to validate proper handling of stream metadata.

        Example:
            Use in sink creation:
                sink = NatsSink(
                    target=mock_target,
                    stream_name="test_stream",
                    schema=sample_schema,  # This fixture
                    key_properties=["id"]
                )
        """
        return {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "timestamp": {"type": "string", "format": "date-time"},
            },
        }

    @pytest.fixture
    def sample_record(self):
        """Provide a sample data record matching the sample schema.

        Returns:
            A sample record with id, name, and timestamp fields that
            conforms to the sample_schema fixture.

        Note:
            This record represents typical data that would flow through a Singer
            pipeline. It matches the sample_schema structure and can be used
            to test record processing, serialization, and publishing operations.

            The record includes:
            - Integer ID for uniqueness
            - Descriptive name for testing string handling
            - ISO 8601 timestamp for date/time processing

        Example:
            Use in publishing tests:
                await sink._publish_record_async(sample_record, {})
                # Verifies record serialization and NATS publishing
        """
        return {
            "id": 1,
            "name": "Test Record",
            "timestamp": "2023-01-01T00:00:00Z",
        }

    @patch("target_nats.sinks.NATS")
    def test_sink_initialization(self, mock_nats_class, mock_nats_client):
        """Test NatsSink initialization with proper configuration and mocking.

        This test validates that the NatsSink initializes correctly with the
        provided configuration, sets up the NATS client, and establishes the
        event loop for async operations.

        Args:
            mock_nats_class: Mocked NATS class constructor, provided by @patch
                decorator.
            mock_nats_client: Mocked NATS client instance from fixture.

        Asserts:
            - Topic prefix is correctly extracted from configuration
            - NATS client is properly instantiated and assigned
            - Event loop is created and available for async operations

        Note:
            This test uses the @patch decorator to replace the NATS class import
            with a mock, ensuring no real NATS connections are attempted during
            testing. The mock client is injected through the mock_nats_class
            return value.

            The test validates core initialization behavior including:
            - Configuration parsing and topic prefix extraction
            - NATS client setup and assignment
            - Event loop creation for sync/async bridging
        """
        mock_nats_class.return_value = mock_nats_client

        # Create a mock target with config
        mock_target = MagicMock()
        mock_target.config = SAMPLE_CONFIG

        sink = NatsSink(
            target=mock_target,
            stream_name="test_stream",
            schema={
                "type": "object",
                "properties": {"id": {"type": "integer"}},
            },
            key_properties=[],
        )

        assert sink._topic_prefix == "test."
        assert sink._nats is mock_nats_client
        assert isinstance(sink._loop, asyncio.AbstractEventLoop)

    @patch("target_nats.sinks.NATS")
    def test_sink_initialization_with_registry(
        self, mock_nats_class, mock_nats_client
    ):
        """Test sink initialization and setup with schema registry integration.

        This test validates the complete schema registry integration workflow,
        including HTTP requests to Apicurio Registry for group creation and
        schema artifact registration.

        Args:
            mock_nats_class: Mocked NATS class constructor, provided by @patch
                decorator.
            mock_nats_client: Mocked NATS client instance from fixture.

        Asserts:
            - Schema registry configuration is properly parsed
            - HTTP requests are made to registry endpoints
            - Group creation occurs when group doesn't exist
            - Schema artifact registration completes successfully

        Note:
            This test uses multiple layers of mocking:
            1. NATS client mocking to avoid actual connections
            2. HTTP requests mocking to simulate Apicurio Registry
            3. Configuration mocking to enable schema registry features

            The test simulates a complete schema registry workflow:
            1. Check if schema group exists (returns 404 - not found)
            2. Create new schema group via POST request
            3. Register schema artifact with versioning support

            The setup() method is called explicitly to trigger registry logic,
            as it's normally called by the Singer SDK during stream processing.
        """
        mock_nats_class.return_value = mock_nats_client

        # Create a mock target with registry config
        mock_target = MagicMock()
        mock_target.config = SAMPLE_CONFIG_WITH_REGISTRY

        # Mock the requests calls used in setup() method for schema registry
        with (
            patch("target_nats.sinks.requests.get") as mock_get,
            patch("target_nats.sinks.requests.post") as mock_post,
        ):

            # Mock the GET request for checking if group exists (returns 404)
            mock_get.return_value.status_code = 404

            sink = NatsSink(
                target=mock_target,
                stream_name="test_stream",
                schema={
                    "type": "object",
                    "properties": {"id": {"type": "integer"}},
                },
                key_properties=[],
            )

            # Call setup explicitly to trigger schema registry logic
            sink.setup()

            # Verify that the schema registry was configured
            assert sink.config.get("enable_schema_registry") is True
            assert (
                sink.config.get("schema_registry_url")
                == "http://localhost:8080/apis/registry/v3"
            )

            # Verify HTTP requests were made
            mock_get.assert_called_once()
            # One for group creation, one for schema registration
            assert mock_post.call_count == 2

    @pytest.mark.asyncio
    async def test_publish_record_async(self, mock_nats_client, sample_record):
        """Test asynchronous record publishing with proper serialization and
        topic construction.

        This test validates the core async publishing functionality, including
        JSON serialization, topic name construction, and NATS client
        interaction.

        Args:
            mock_nats_client: Mocked NATS client instance from fixture.
            sample_record: Sample record data from fixture.

        Asserts:
            - NATS publish method is called exactly once
            - Topic name is correctly constructed with prefix
            - Record data is properly JSON-serialized
            - Published data matches the original record

        Note:
            This test uses the @pytest.mark.asyncio decorator to enable proper
            async test execution. It validates the complete record publishing
            pipeline from sink method call to NATS client interaction.

            The test verifies several critical aspects:
            1. Topic construction: prefix + stream name format
            2. Data serialization: record converted to JSON bytes
            3. Client interaction: publish method called with correct arguments
            4. Data integrity: published data matches original record

            The mock NATS client captures the publish call arguments, allowing
            detailed verification of the data being sent to the broker.
        """
        with patch("target_nats.sinks.NATS", return_value=mock_nats_client):
            # Create a mock target with config
            mock_target = MagicMock()
            mock_target.config = SAMPLE_CONFIG

            sink = NatsSink(
                target=mock_target,
                stream_name="test_stream",
                schema={
                    "type": "object",
                    "properties": {"id": {"type": "integer"}},
                },
                key_properties=[],
            )

            await sink._publish_record_async(sample_record, {})

            mock_nats_client.publish.assert_called_once()
            call_args = mock_nats_client.publish.call_args

            # Verify topic name
            assert call_args[0][0] == "test.test_stream"

            # Verify data is JSON encoded
            import json

            published_data = json.loads(call_args[0][1].decode())
            assert published_data == sample_record

    # Note: Additional test methods follow similar documentation patterns.
    # Each test method validates specific functionality with comprehensive
    # docstrings explaining parameters, assertions, and testing approach.
    # The remaining methods test batch publishing, connection management,
    # event loop handling, and concurrent processing scenarios.

    @pytest.mark.asyncio
    async def test_publish_batch_async(self, mock_nats_client):
        """Test async batch publishing."""
        records = [
            {"id": 1, "name": "Record 1"},
            {"id": 2, "name": "Record 2"},
            {"id": 3, "name": "Record 3"},
        ]

        with patch("target_nats.sinks.NATS", return_value=mock_nats_client):
            # Create a mock target with config
            mock_target = MagicMock()
            mock_target.config = SAMPLE_CONFIG

            sink = NatsSink(
                target=mock_target,
                stream_name="test_stream",
                schema={
                    "type": "object",
                    "properties": {"id": {"type": "integer"}},
                },
                key_properties=[],
            )

            await sink._publish_batch_async(records, {})

            # Verify all records were published
            assert mock_nats_client.publish.call_count == len(records)

    @patch("target_nats.sinks.NATS")
    def test_event_loop_management(self, mock_nats_class, mock_nats_client):
        """Test proper event loop management."""
        mock_nats_class.return_value = mock_nats_client

        # Create a mock target with config
        mock_target = MagicMock()
        mock_target.config = SAMPLE_CONFIG

        sink = NatsSink(
            target=mock_target,
            stream_name="test_stream",
            schema={
                "type": "object",
                "properties": {"id": {"type": "integer"}},
            },
            key_properties=[],
        )

        # Test that event loop is properly managed
        loop1 = sink._get_or_create_event_loop()
        loop2 = sink._get_or_create_event_loop()

        assert loop1 is loop2  # Should reuse the same loop
        assert isinstance(loop1, asyncio.AbstractEventLoop)

    @pytest.mark.asyncio
    async def test_connection_management(self, mock_nats_client):
        """Test NATS connection management."""
        with patch("target_nats.sinks.NATS", return_value=mock_nats_client):
            # Create a mock target with config
            mock_target = MagicMock()
            mock_target.config = SAMPLE_CONFIG

            sink = NatsSink(
                target=mock_target,
                stream_name="test_stream",
                schema={
                    "type": "object",
                    "properties": {"id": {"type": "integer"}},
                },
                key_properties=[],
            )

            # Test connection establishment
            await sink._connect()
            assert sink._connection_ready is True
            mock_nats_client.connect.assert_called_with(
                "nats://localhost:4222"
            )

            # Test connection reuse
            await sink._connect()  # Should not reconnect
            assert mock_nats_client.connect.call_count == 1

            # Test disconnection
            await sink._disconnect_async()
            assert sink._connection_ready is False
            mock_nats_client.close.assert_called_once()


# Additional async integration tests
class TestAsyncIntegration:
    """Test async integration scenarios.

    This test class focuses on integration testing scenarios involving
    concurrent operations, complex async workflows, and end-to-end
    testing of the async functionality within the NATS sink.

    These tests validate that multiple async operations work correctly
    together and that the sink handles concurrent publishing scenarios
    as expected in real-world usage.
    """

    @pytest.mark.asyncio
    async def test_concurrent_publishing(self):
        """Test that multiple records can be published concurrently.

        This integration test validates that the sink can handle multiple
        concurrent record publishing operations correctly, ensuring that
        all records are processed without interference or data corruption.

        The test creates multiple records and publishes them concurrently
        using asyncio.gather to verify that the sink's async implementation
        can handle concurrent workloads as expected in production scenarios.

        Asserts:
            - All concurrent publish operations complete successfully
            - The correct number of records are published to NATS
            - Mock client interactions occur as expected
        """
        with patch("target_nats.sinks.NATS") as mock_nats_class:
            mock_client = AsyncMock()
            mock_nats_class.return_value = mock_client

            # Create a mock target with config
            mock_target = MagicMock()
            mock_target.config = SAMPLE_CONFIG

            sink = NatsSink(
                target=mock_target,
                stream_name="test_stream",
                schema={
                    "type": "object",
                    "properties": {"id": {"type": "integer"}},
                },
                key_properties=[],
            )

            # Simulate concurrent record processing
            records = [{"id": i, "data": f"record_{i}"} for i in range(5)]
            tasks = [
                sink._publish_record_async(record, {}) for record in records
            ]

            # All tasks should complete successfully
            await asyncio.gather(*tasks)

            # Verify all records were published
            assert mock_client.publish.call_count == len(records)
