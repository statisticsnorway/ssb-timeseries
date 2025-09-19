"""IO modules manage regular read and write functionality for both data and metadata.

Multiple repositories can be configured with different storage locations and technologies.
Data and metadata are separated, so that a metadata catalog may be maintained per repository, or common to all repositories.

Support for file based IO is built in, but for added flexibility,
a protocol specification allows custom IO modules to be defined outside the ssb-timeseries library.
"""
