"""Configurations for the SSB timeseries library.

An environment variable TIMESERIES_CONFIG is expected to point to a JSON file with configurations.
If these exist, they will be loaded and put into a Config object CONFIG when the configuration module is loaded.

In most cases, this would happen behind the scene when :py:mod:`ssb_timeseries.dataset` or :py:mod:`ssb_timeseries.catalog` are imported.

Directly accessing the configuration module should only be required when manipulating configurations from Python code.

Example:
    >>> # doctest: +SKIP
    >>> from ssb_timeseries.config import CONFIG
    >>> CONFIG.catalog = 'gs://{bucket}/timeseries/metadata/'
    >>> CONFIG.save()
    >>> # doctest: -SKIP

For switching between preset configurations, use the `timeseries-config` command::

    poetry run timeseries-config <option>

which is equivalent to::

    python ./config.py <option>

See :py:func:`ssb_timeseries.config.main` for details on the named options.
"""

from __future__ import annotations

import os
from pathlib import Path

from .types import ConfigDict

# mypy: disable-error-code="assignment, arg-type, override,call-arg,has-type,no-untyped-def,attr-defined,import-untyped,"

PACKAGE_NAME = "ssb_timeseries"
ENV_VAR_NAME = "TIMESERIES_CONFIG"
DEFAULT_HANDLER = "simple-parquet"

HOME = str(Path.home())

DAPLALAB_WORK = "/home/onyxia/work"
DAPLALAB_FUSE = "/buckets/produkt"
SSB_DIR_NAME = "tidsserier"
ROOT_DIR_NAME = "timeseries"
META_DIR_NAME = "metadata"
SSB_CONF_DIR = "konfigurasjon"
LINUX_CONF_DIR = ".config"
SSB_LOGDIR = "logger"
LOGDIR = "logs"
LOGFILE = "timeseries.log"
CONFIGFILE = "timeseries_config.json"

PROJECT_ID = os.getenv("DAPLA_TEAM_GOOGLE_PROJECT_ID", "<team_name>")
DAPLA_TEAM_CONTEXT = os.getenv("DAPLA_TEAM_CONTEXT", PROJECT_ID)
DAPLA_ENV = os.getenv("DAPLA_ENVIRONMENT", "")
"""Returns the Dapla environment: 'prod' | test | dev"""
DAPLA_TEAM = os.getenv("DAPLA_TEAM", PROJECT_ID)
"""Returns the Dapla team/project name.'"""
DAPLA_BUCKET = f"gs://{DAPLA_TEAM}-data-produkt-{DAPLA_ENV.lower()}"
"""Returns the Dapla product bucket name for the current environment: gs://{DAPLA_TEAM}-data-produkt{DAPLA_ENV}"""

LOGGING_PRESETS = {
    "simple": {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "level": "INFO",
            },
        },
        "loggers": {
            PACKAGE_NAME: {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            }
        },
    },
    "console+file": {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(levelname)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "json": {
                "format": '{"time": %(asctime)-s, "level": %(levelname)-s, "message": %(message)s},',
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "default",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "level": "INFO",
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "default",
                "filename": "ssb_timeseries.log",
                "maxBytes": 10_000,
                "backupCount": 3,
            },
        },
        "loggers": {
            PACKAGE_NAME: {
                "level": "INFO",
                "handlers": ["console", "file"],
                "propagate": False,
            }
        },
    },
}

BUILTIN_IO_HANDLERS = {
    "simple-parquet": {
        "handler": "ssb_timeseries.io.pyarrow_simple.FileSystem",
        "options": {},
    },
    "hive-partitioned-parquet": {
        "handler": "ssb_timeseries.io.pyarrow_hive.FileSystem",
        "options": {},
    },
    "json": {
        "handler": "ssb_timeseries.io.json_metadata.JsonMetaIO",
        "options": {},
    },
    "snapshots": {
        "handler": "ssb_timeseries.io.snapshot.FileSystem",
        "options": {},
    },
}
PRESETS: dict[str, ConfigDict] = {
    "home": {
        "configuration_file": str(Path(HOME, LINUX_CONF_DIR, PACKAGE_NAME, CONFIGFILE)),
        "io_handlers": BUILTIN_IO_HANDLERS,
        "repositories": {
            DAPLA_TEAM: {
                "directory": {
                    "handler": DEFAULT_HANDLER,
                    "options": {"path": str(Path(HOME, ROOT_DIR_NAME))},
                },
                "catalog": {
                    "handler": "json",
                    "options": {
                        "path": str(Path(HOME, ROOT_DIR_NAME, META_DIR_NAME)),
                    },
                },
            }
        },
        "logging": {},
    },
    "dapla": {
        "configuration_file": str(
            Path(DAPLA_BUCKET, SSB_CONF_DIR, PACKAGE_NAME, CONFIGFILE)
        ),
        "io_handlers": BUILTIN_IO_HANDLERS,
        "repositories": {
            PROJECT_ID: {
                "name": PROJECT_ID,
                "directory": {
                    "handler": DEFAULT_HANDLER,
                    "options": {"path": str(Path(DAPLA_BUCKET, SSB_DIR_NAME))},
                },
                "catalog": {
                    "handler": "json",
                    "options": {
                        "path": str(Path(DAPLA_BUCKET, SSB_DIR_NAME, META_DIR_NAME)),
                    },
                },
            }
        },
        "logging": {},
    },
    "daplalab": {
        "configuration_file": str(
            Path(DAPLA_BUCKET, SSB_CONF_DIR, PACKAGE_NAME, CONFIGFILE)
        ),
        "io_handlers": BUILTIN_IO_HANDLERS,
        "repositories": {
            PROJECT_ID: {
                "name": PROJECT_ID,
                "directory": {
                    "handler": DEFAULT_HANDLER,
                    "options": {"path": str(Path(DAPLALAB_FUSE, SSB_DIR_NAME))},
                },
                "catalog": {
                    "handler": "json",
                    "options": {
                        "path": str(Path(DAPLALAB_FUSE, SSB_DIR_NAME, META_DIR_NAME)),
                    },
                },
            }
        },
        "logging": {},
    },
}

PRESETS["default"] = PRESETS["home"]
PRESETS["defaults"] = PRESETS["home"]

DEFAULTS = PRESETS["default"]
