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

from datetime import datetime
from typing import Any
from typing import Protocol
from typing import runtime_checkable

# mypy: disable-error-code="no-untyped-def"
# ,no-any-return"


@runtime_checkable
class DataReadWrite(Protocol):
    """Defines the contract (protocol) for data IO."""

    def __init__(
        self,
        repository: str | dict,  # TODO: streamline - update to use dict config only
        set_name: str,  # TODO: remove -> turn into method parameter
        set_type: str,  # TODO: remove -> turn into method parameter
        as_of_utc: datetime | None = None,  # TODO: remove -> turn into method parameter
        **kwargs,
    ) -> None:
        """Initializes the IO handler with configuration for a specific data storage.

        This constructor is called by the io dispatcher. It configures the
        handler instance to operate within a specific base context.

        Args:
            repository: The data repository name or configuration.
            set_name: The dataset name.
            set_type: The data type for the dataset.
            as_of_utc: The version marker (should be timezone aware).
            **kwargs (str): Any parameters defined for the handler or data storage in the configuration.
        """
        ...

    @property
    def exists(self) -> bool:
        """Check if data exists in the configured storage."""
        ...

    def write(self, data: Any, tags: dict | None = None) -> None:
        """Writes data to the configured storage."""
        ...

    def read(self, *args, **kwargs) -> Any:
        """Reads from the configured storage."""
        ...

    def versions(self, *args, **kwargs) -> list[datetime | str]:
        """Reatrieves available versions from the configured storage."""
        ...


@runtime_checkable
class MetadataReadWrite(Protocol):
    """Defines the contract (protocol) for metadata IO."""

    def __init__(
        self,
        repository: str | dict,  # TODO: streamline - update to use dict config only
        set_name: str,  # TODO: remove -> turn into method parameter
        **kwargs,
    ) -> None:
        """Initializes the IO handler with configuration for a specific metadata storage."""
        ...

    def exists(self, name: str) -> bool:
        """Check if metadata exists in configured storage."""
        ...

    def find(self, **kwargs) -> bool:
        """Find datasets in configured storage."""
        ...

    def write(self, **kwargs) -> None:
        """Writes metadata to configured storage."""
        ...

    def read(self, **kwargs) -> dict[str, Any]:
        """Reads metadata from configured storage."""
        ...

    @classmethod
    def search(cls, **kwargs) -> dict[str, Any]:
        """Searches and retrieves metadata for all sets of a repository from configured storage."""
        ...
