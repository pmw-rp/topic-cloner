# Topic Cloner

This tool can be used to clone topics from one cluster to another.

# Usage

To run from the command line, either compile and run with the following:

```bash
go build .
./clone --config config.yaml
```

Or run directly:

```bash
go run . --config config.yaml
```

Usage can be see as follows:

```text
$ ./clone --help
Usage of ./clone:
  -config string
        path to cloner config file (default "config.yaml")
  -loglevel string
        logging level (default "info")

```

# Configuration

A single YAML file is used to define the source and destinations - in both cases, this can be either a local file or a
Redpanda cluster. Complete examples can be found in the [examples](./examples) directory.

## Source

Here we see an example source definition, which includes optional SASL and TLS sections.

The list of topics to clone can contain regex patterns to match multiple topics.

```yaml
source:
  bootstrap_servers: seed-something.somewhere.fmc.prd.cloud.redpanda.com:9092
  sasl:
    sasl_method: SCRAM-SHA-256
    sasl_username: some-username
    sasl_password: some-password
  tls:
    enabled: true
  topics:
    - _internal_.*
    - _redpanda.*
   ```

### File Source (Alternative)

```yaml
source:
  file: topics.json
```

## Destination

This is is configured exactly as with a source.
