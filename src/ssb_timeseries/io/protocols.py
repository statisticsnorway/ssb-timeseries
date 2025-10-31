"""Defines the structural contracts for I/O handlers using `typing.Protocol`.

This module specifies the formal API that a custom I/O plugin must adhere to.
By using Protocols (structural typing), external users can create handler
classes that are compatible with `ssb-timeseries` without needing to
inherit from any of its base classes.
This provides maximum flexibility and decoupling for plugin authors.
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
    """Defines the contract (protocol) for data IO handlers."""

    def __init__(
        self,
        repository: str | dict,  # TODO: streamline - update to use dict config only
        set_name: str,  # TODO: remove -> turn into method parameter
        set_type: str,  # TODO: remove -> turn into method parameter
        as_of_utc: datetime | None = None,  # TODO: remove -> turn into method parameter
        **kwargs,
    ) -> None:
        """Initialize the IO handler with configuration for a specific data storage.

        This constructor is called by the IO dispatcher.
        It configures the handler instance to operate within a specific context.

        Args:
            repository: The data repository name or configuration.
            set_name: The dataset name.
            set_type: The data type for the dataset.
            as_of_utc: The version marker (should be timezone aware).
            **kwargs: Any parameters defined for the handler in the configuration.
        """
        ...

    @property
    def exists(self, set_name: str = "") -> bool:
        """Check if the dataset exists in the configured storage."""
        ...

    def write(self, data: Any, tags: dict | None = None) -> None:
        """Write the dataset's data to the configured storage.

        This method should handle both the creation of new data files and the
        updating/merging of data into existing files, depending on the
        versioning strategy of the dataset.

        Args:
            data: The data to be written (e.g., a pandas DataFrame or PyArrow Table).
            tags: A dictionary of metadata tags to be stored with the data,
                often in the file's schema.
        """
        ...

    def read(self, *args, **kwargs) -> Any:
        """Read data from the configured storage.

        Returns:
            The dataset's data in a dataframe-like format (e.g., PyArrow Table).
            If the data does not exist, an empty dataframe should be returned.
        """
        ...

    def versions(self, *args, **kwargs) -> list[datetime | str]:
        """Retrieve a list of available versions for the dataset.

        Returns:
            A sorted list of version identifiers (datetimes or strings).
        """
        ...


@runtime_checkable
class MetadataReadWrite(Protocol):
    """Defines the contract (protocol) for metadata IO handlers."""

    def __init__(
        self,
        repository: str | dict,  # TODO: streamline - update to use dict config only
        set_name: str,  # TODO: remove -> turn into method parameter
        **kwargs,
    ) -> None:
        """Initialize the IO handler for a specific metadata storage.

        Args:
            repository: The metadata repository name or configuration.
            set_name: The dataset name to operate on.
            **kwargs: Any parameters defined for the handler in the configuration.
        """
        ...

    def exists(self, name: str) -> bool:
        """Check if metadata for a given dataset name exists."""
        ...

    def find(self, **kwargs) -> bool:
        """Find datasets in the configured storage based on metadata criteria."""
        ...

    def write(self, **kwargs) -> None:
        """Write metadata to the configured storage."""
        ...

    def read(self, **kwargs) -> dict[str, Any]:
        """Read metadata from the configured storage.

        Returns:
            A dictionary containing the metadata tags for the dataset.
        """
        ...

    @classmethod
    def search(cls, **kwargs) -> dict[str, Any]:
        """Search and retrieve metadata from the configured storage.

        This method should allow searching for datasets based on various
        metadata criteria.

        Returns:
            A dictionary or list of dictionaries containing the search results.
        """
        ...
