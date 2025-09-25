"""Defines the structural contracts for I/O handlers using typing.Protocol.

This is the formal API that a custom I/O plugins must adhere to.
By using Protocols (structural typing, or "duck typing"), external users can create
handler classes that are compatible with ssb-timeseries without needing to
inherit from any of its base classes. This provides maximum flexibility and
decoupling for plugin authors.

For a more guided implementation experience with runtime checks and potential
helper methods, developers can optionally inherit from a class that implements
this protocol, such as ssb_timeseries.io.abc.AbstractIOHandler.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Protocol
from typing import runtime_checkable

if TYPE_CHECKING:
    from ..dataset import Dataset


@runtime_checkable
class IOHandler(Protocol):
    """A protocol that defines the contract for a generic I/O handler.

    An I/O handler is a class responsible for the logic of persisting and
    loading a Dataset to and from a specific storage backend (e.g., a local
    filesystem, a cloud bucket, or a database).

    Attributes:
        root_path (str): The base path or connection string for the storage
                         location this handler is responsible for.
    """

    def __init__(self, root_path: str, **options: str) -> None:
        """Initializes the handler for a specific storage location.

        This constructor is called by the io dispatcher. It configures the
        handler instance to operate within a specific base context.

        Args:
            root_path (str): The base path, URI, or connection string for the
                             storage backend (e.g., '/data/stable/',
                             's3://my-bucket/snapshots/').
            **options (str): A dict of handler-specific options, as defined
                             in the 'io_handlers' section of the config.
        """
        ...

    def write(self, name: str, ds: Dataset) -> None:
        """Writes a Dataset to the storage backend.

        The handler is responsible for its own internal logic, such as creating
        versioned subdirectories or writing to a specific table.

        Args:
            name (str): The logical name of the dataset. The handler will use
                        this to construct the final destination path within its
                        configured `root_path`.
            ds (Dataset): The Dataset object to persist.
        """
        ...

    def read(self, name: str, version: str | None = None) -> Dataset:
        """Reads a Dataset from the storage backend.

        Args:
            name (str): The logical name of the dataset to read.
            version (str | None): The specific version of the dataset to load.
                                  If None, the handler should attempt to load the
                                  latest available version.

        Returns:
            The loaded Dataset object.
        """
        ...
