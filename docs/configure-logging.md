# Configure Logging

The timeseries library relies on the file identified by the environment variable `TIMESERIES_CONFIG` for configurations.

The simplest possible specification is `"logging": {},`.
This signifies no logging, leaving the responsibility entirely to the application.

If not empty, anything that will resolve to a valid :logging:dictConfig: in the standard Python logging library.

This guide provides some `dictConfig` examples.

Note: The examples cover only the logging part of the configuration.
See the {Quickstart Guide} for a working minimal configuration to put it into,
or {Configure IO} for how to define the repositories for data and metadata storage.

## Design choices: Log levels and tracing

Log level `INFO` is used for start and finish messages for every read, write and query operation.
That applies to data and metadata separately.

In the current version, tracing between related operations (like a read followed by a calculation and a write) is primarily achieved through log analysis of the dataset names.

Planned extensions include adding **process identifiers** (UUIDs) to log messages.
This will allow for more robust lineage tracking even when multiple processes are operating on the same datasets.
Users can currently implement this by using a custom `logging.LoggerAdapter` to inject correlation IDs into the log record.

## Example: Log to console

```json
"logging": {
    "version": 1,
    "disable_existing_loggers": false,
    "formatters": {
        "simple": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "simple",
            "stream": "ext://sys.stdout"
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"]
    }
}
```

## Example: Log to file

```json
"logging": {
    "version": 1,
    "disable_existing_loggers": false,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        }
    },
    "handlers": {
        "file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "standard",
            "filename": "timeseries.log",
            "mode": "a"
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["file"]
    }
}
```

## Example: Rotating log to file

```json
"logging": {
    "version": 1,
    "disable_existing_loggers": false,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        }
    },
    "handlers": {
        "rotating_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "standard",
            "filename": "timeseries.log",
            "maxBytes": 10485760,
            "backupCount": 5
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["rotating_file"]
    }
}
```

## Example: Log to queue

A simple way to integrate with event based workflow orchestration,
is to add a log handler that will put every entry at log level INFO to a queue.
An orchestration service that consumes the messages from the queue will then be able to do its magic.
The details of that is out of scope here, but we will show an example of how to configure the log handler.

```json
"logging": {
    "version": 1,
    "disable_existing_loggers": false,
    "handlers": {
        "queue": {
            "class": "logging.handlers.QueueHandler",
            "queue": "ext://my_app.logging_queue"
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["queue"]
    }
}
```

## Further Reading

For more complex logging setups, refer to the standard Python [logging.config](https://docs.python.org/3/library/logging.config.html#logging-config-dictschema) documentation.
The library is designed to be compatible with any valid `dictConfig`.
