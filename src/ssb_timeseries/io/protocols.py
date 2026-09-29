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

from .dataset_ref import DatasetRef

# mypy: disable-error-code="no-untyped-def"
# ,no-any-return"


@runtime_checkable
class DataReadWrite(Protocol):
    """Defines the contract (protocol) for data IO handlers."""

    def __init__(
        self,
        repository: str | dict,  # TODO: streamline - update to use dict config only
        **options: Any,
    ) -> None:
        """Initialize the IO handler with configuration for a specific data storage.

        This constructor is called by the IO dispatcher.
        It configures the handler from the repository and its options only.
        Which dataset an operation concerns is passed to that operation as a
        `DatasetRef`, so one handler instance can serve any number of datasets.

        Args:
            repository: The data repository name or configuration.
            **options: Any parameters defined for the handler in the configuration.
        """
        ...

    def exists(self, dataset: DatasetRef) -> bool:
        """Check if a dataset exists in the configured storage.

        Args:
            dataset: The dataset to check for.
        """
        ...

    def write(self, dataset: DatasetRef, data: Any, tags: dict | None = None) -> None:
        """Write a dataset's data to the configured storage.

        This method should handle both the creation of new data files and the
        updating/merging of data into existing files, depending on the
        versioning strategy of the dataset.

        Args:
            dataset: The dataset to write.
            data: The data to be written (e.g., a pandas DataFrame or PyArrow Table).
            tags: A dictionary of metadata tags to be stored with the data,
                often in the file's schema.
        """
        ...

    def read(self, dataset: DatasetRef, *args: Any, **kwargs: Any) -> Any:
        """Read a dataset's data from the configured storage.

        Args:
            dataset: The dataset to read.
            *args: Accepted and ignored, for call compatibility.
            **kwargs: Handler specific read options, such as an interval.

        Returns:
            The dataset's data in a dataframe-like format (e.g., PyArrow Table).
            If the data does not exist, an empty dataframe should be returned.
        """
        ...

    def versions(
        self, dataset: DatasetRef, *args: Any, **kwargs: Any
    ) -> list[datetime | str]:
        """Retrieve a list of available versions for a dataset.

        Args:
            dataset: The dataset to list versions for.
            *args: Accepted and ignored, for call compatibility.
            **kwargs: Handler specific options, such as a file name pattern.

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
        **options: Any,
    ) -> None:
        """Initialize the IO handler for a specific metadata storage.

        This constructor is called by the IO dispatcher.
        The handler is configured from the repository and its options only.
        Which dataset an operation concerns is passed to that operation.

        Args:
            repository: The metadata repository name or configuration.
            **options: Any parameters defined for the handler in the configuration.
        """
        ...

    @property
    def exists(self, set_name: str = "") -> bool:
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
