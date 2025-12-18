# target-nats

`target-nats` is a Singer target for nats.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPi:

```bash
pipx install target-nats
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/target-nats.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the target.

This section can be created by copy-pasting the CLI output from:

```
target-nats --about --format=markdown
```
-->

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-nats --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Authentication and Authorization

<!--
Developer TODO: If your target requires special access on the destination system, or any special authentication requirements, provide those here.
-->

## Async Architecture

This NATS target features an **async-first design** for optimal performance when publishing messages to NATS brokers. Key improvements include:

### 🚀 Performance Features

- **Concurrent Publishing**: Records are published asynchronously using `asyncio` for better throughput
- **Connection Pooling**: NATS connections are reused and managed efficiently
- **Batch Processing**: Support for concurrent batch publishing of multiple records
- **Smart Event Loop Management**: Automatically detects and reuses existing event loops

### 🔧 Async Compatibility

- **Singer SDK Compatible**: Maintains full compatibility with the synchronous Singer SDK interface
- **Schema Registry Integration**: Async support for Apicurio Schema Registry operations
- **DataDog Telemetry**: Integrated with DataDog data streams monitoring for async operations

### 📊 Configuration Options

```json
{
  "nats_url": "nats://localhost:4222",
  "queue_prefix": "my_prefix_",
  "enable_schema_registry": true,
  "schema_registry_url": "http://localhost:8080/apis/registry/v3"
}
```

See `examples/async_usage.py` for detailed examples of using the async capabilities.

## Usage

You can easily run `target-nats` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-nats --version
target-nats --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-nats --config /path/to/target-nats-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-nats` CLI interface directly using `poetry run`:

```bash
poetry run target-nats --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-nats
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-nats --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano run tap-carbon-intensity target-nats
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
