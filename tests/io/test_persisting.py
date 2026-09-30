import logging
import re
from copy import deepcopy
from pathlib import Path

import pytest

from ssb_timeseries.io import fs

from ..fixtures.dataset_factories import function_name_hex
from .conftest import PROCESS_STAGE
from .conftest import PRODUCT

# mypy: disable-error-code="no-untyped-def,no-untyped-call,arg-type,attr-defined,assignment"

test_logger = logging.getLogger(__name__)


def log(path, before, after):
    test_logger.debug(
        f"ARCHIVE to {path}\n\thas file count before:{before}, and after: {after}"
    )


# ----- The tests --------------


def test_archiving_is_a_no_op_when_no_archive_is_configured(
    conftest,
    monkeypatch,
    xyz_at,
):
    """A configuration without an `archives` section must not archive, and must not fail.

    Archiving is optional, and no preset configures an archive, so most
    configurations reach the first line of `archive()`. Reading the section as an
    attribute raised `AttributeError` there, which turned an unconfigured feature
    into a crash on every save.
    """
    from ssb_timeseries import io
    from ssb_timeseries.dataset import Dataset

    configuration = deepcopy(conftest.configuration)
    del configuration.__dict__["archives"]
    monkeypatch.setattr(
        "ssb_timeseries.config.Config.active",
        lambda: configuration,
    )

    ds = Dataset(name=function_name_hex(), data=xyz_at, data_type="simple")
    ds.save()

    # Returns without raising, and without writing an archive anywhere.
    io.archive(ds)

    assert io._sharing_destinations([]) == []


def test_archive_after_save_does_not_raise_error(
    caplog,
    xyz_at,
):
    from ssb_timeseries.dataset import Dataset

    caplog.set_level(logging.DEBUG)

    ds = Dataset(name=function_name_hex(), data=xyz_at, data_type="simple")

    ds.save()
    ds.archive()


def test_archive_without_save_raises_error(
    caplog,
    xyz_at,
):
    from ssb_timeseries.dataset import Dataset

    caplog.set_level(logging.DEBUG)

    ds = Dataset(name=function_name_hex(), data=xyz_at, data_type="simple")

    with pytest.raises(FileNotFoundError):
        # no ds.save() to see here!
        ds.archive()


def test_the_deprecated_names_still_archive_and_say_so(
    dataset_with_sharing_config,
):
    """The old names keep working, and say that they are going away.

    `persist` and `snapshot` both meant what `archive` now means.
    A caller on one of them should be redirected rather than left guessing which
    name is current.
    """
    import ssb_timeseries.io as io

    (expected, dataset) = dataset_with_sharing_config
    dataset.save()

    with pytest.warns(DeprecationWarning, match="archive"):
        io.persist(dataset)

    with pytest.warns(DeprecationWarning, match="archive"):
        dataset.snapshot()

    # The alias archives rather than only warning, and each call adds a version.
    assert fs.file_count(expected["expected_archive_path"] / dataset.name) == 2


def test_archive_does_not_mutate_the_sharing_configuration(
    dataset_with_sharing_config,
):
    """An archive must not write its defaults back into the dataset's configuration.

    `DatasetRef` is documented as a snapshot rather than a live view, so a
    handler that filled in a missing key would be editing the dataset it was
    handed, and every ref built before that call as well.

    The fixture is parametrised over several sharing configurations, one of which
    deliberately leaves keys unconfigured, so the defaulting path is covered.
    """
    (_, dataset) = dataset_with_sharing_config
    sharing_before = deepcopy(dataset.sharing)

    dataset.save()
    dataset.archive()

    assert dataset.sharing == sharing_before


def test_archive_writes_one_file_per_version_and_keeps_every_one(
    caplog,
    dataset_with_sharing_config,
):
    caplog.set_level(logging.DEBUG)
    (expected, dataset) = dataset_with_sharing_config

    archived = expected["expected_archive_path"] / dataset.name

    count_before_archived = fs.file_count(archived, create=True)

    n = 2
    dataset.save()

    dataset.archive()
    dataset.archive()

    count_after_archived = fs.file_count(archived)

    log(archived, count_before_archived, count_after_archived)

    assert count_after_archived == count_before_archived + n


def test_the_generic_archive_is_not_copied_to_shared_locations(
    caplog,
    dataset_with_sharing_config,
):
    """Whether an archive is also copied to the shared locations is a convention.

    The generic convention makes no claim about where else data should live, so
    it writes the archive and stops.
    A dataset naming shared locations is not thereby copied to them, and this is
    why a convention has to be chosen deliberately.
    """
    caplog.set_level(logging.DEBUG)
    (expected, dataset) = dataset_with_sharing_config

    path_123 = expected["expected_sharing_path_123"] / dataset.name
    path_234 = expected["expected_sharing_path_234"] / dataset.name

    count_before_123 = fs.file_count(path_123, create=True)
    count_before_234 = fs.file_count(path_234, create=True)

    dataset.save()
    dataset.archive()

    assert fs.file_count(path_123) == count_before_123
    assert fs.file_count(path_234) == count_before_234


def test_the_ssb_archive_is_copied_to_shared_locations(
    caplog,
    dataset_with_sharing_config,
    use_archive_format,
):
    """Statistics Norway's convention requires shared data to be archived where it is shared."""
    caplog.set_level(logging.DEBUG)
    use_archive_format("ssb")
    (expected, dataset) = dataset_with_sharing_config

    # A shared location holds the archive where the archive has it, so that a
    # location holding several datasets still resolves them the same way.
    relative = Path(dataset.product) / dataset.process_stage / dataset.name
    path_123 = expected["expected_sharing_path_123"] / relative
    path_234 = expected["expected_sharing_path_234"] / relative

    count_before_123 = fs.file_count(path_123, create=True)
    count_before_234 = fs.file_count(path_234, create=True)

    n = 2
    dataset.save()
    dataset.archive()
    dataset.archive()

    log(path_123, count_before_123, fs.file_count(path_123))
    log(path_234, count_before_234, fs.file_count(path_234))

    assert fs.file_count(path_123) == count_before_123 + n
    assert fs.file_count(path_234) == count_before_234 + n


def test_the_archive_follows_the_configured_convention(
    dataset_with_sharing_config,
    use_archive_format,
):
    """The convention decides the folders and the name, not the handler.

    Under the SSB convention the product and the data state are folders above
    the dataset, and the period, as-of and version are in the name.
    """
    use_archive_format("ssb")
    (expected, dataset) = dataset_with_sharing_config
    dataset.save()
    dataset.archive()

    folder = (
        expected["expected_archive_path"]
        / dataset.product
        / dataset.process_stage
        / dataset.name
    )
    (archived,) = folder.glob("*.parquet")

    # The period is written with dashes in place of colons, to milliseconds.
    assert re.search(r"\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.\d{3}", archived.name)
    assert ":" not in archived.name
    assert archived.name.endswith("_v1.parquet")


def test_a_dataset_name_the_convention_cannot_write_is_refused(
    dataset_with_sharing_config,
    use_archive_format,
):
    """A name that does not fit the convention is refused, not silently rewritten.

    Rewriting it would produce an archive whose name nobody chose, holding a
    dataset nobody can look up by the name they know it under.
    """
    from ssb_timeseries.io.format import SSB

    use_archive_format("ssb")

    with pytest.raises(ValueError, match="does not allow"):
        SSB.normalise("folketelling 2024")


def test_an_unconfigured_sharing_key_falls_back_to_the_default_location(
    sharing_configs,
    one_new_set_for_each_data_type,
    use_archive_format,
):
    """Naming a location that is not set up yet is not a reason to write nothing.

    The key is resolved to the default location, so the archive still goes
    somewhere the dataset's sharers will find it.
    The convention has to require replication for there to be anywhere to go.
    """
    (_, shared_default, _, _) = sharing_configs
    use_archive_format("ssb")

    dataset = one_new_set_for_each_data_type
    dataset.product = PRODUCT
    dataset.process_stage = PROCESS_STAGE
    dataset.sharing = ["not-a-configured-team"]

    dataset.save()
    dataset.archive()

    relative = Path(dataset.product) / dataset.process_stage / dataset.name
    assert fs.file_count(shared_default / relative) == 1


def test_archive_of_a_dataset_that_is_not_stored_as_a_file(
    dataset_with_sharing_config,
    monkeypatch,
):
    """A data repository need not be a filesystem, so archiving must not need a path.

    The data handler is replaced with one that has no file layout at all, which
    stands in for a database or an API behind the configured repository.
    It has no `fullpath`, so archiving can only work if the data is read through
    the handler and written by the archive handler.
    """
    from ssb_timeseries.io import DataIO
    from ssb_timeseries.io import _handler_class

    (expected, dataset) = dataset_with_sharing_config
    dataset.save()
    data = DataIO(dataset).dh.read(dataset.ref)

    class InMemoryDataHandler:
        """Serves data from memory, and has nowhere to put a file."""

        def __init__(self, repository, **options):
            self.repository = repository
            self.options = options

        def exists(self, ref):
            return True

        def read(self, ref, interval=""):
            return data

        def write(self, ref, data, tags=None):
            raise AssertionError("archiving must not write through the data handler")

        def versions(self, ref):
            raise AssertionError("archiving must not ask the data handler for versions")

    assert not hasattr(InMemoryDataHandler, "fullpath")

    real_handler_class = _handler_class
    monkeypatch.setattr(
        "ssb_timeseries.io._handler_class",
        lambda handler_name: (
            InMemoryDataHandler
            if handler_name == "simple-parquet"
            else real_handler_class(handler_name)
        ),
    )

    dataset.archive()

    archived = list(
        (expected["expected_archive_path"] / dataset.name).glob("*.parquet")
    )
    assert len(archived) == 1


def test_archive_archives_the_dataset_the_repository_holds(
    dataset_with_sharing_config,
):
    """The archive holds the dataset's data, laid out by the archive's own rules.

    The archive is an artifact in its own right, with its own naming and format
    conventions, so it need not reproduce the source repository's storage
    details.
    What it must hold is the dataset itself, which is what the data repository
    serves.
    """
    import pyarrow.parquet as pq

    from ssb_timeseries.io import DataIO

    (expected, dataset) = dataset_with_sharing_config
    dataset.save()
    original = DataIO(dataset).dh.read(dataset.ref)

    dataset.archive()

    (archived,) = (expected["expected_archive_path"] / dataset.name).glob("*.parquet")
    assert pq.read_table(archived).equals(original)


def test_an_archive_records_its_version_in_the_file_name(
    dataset_with_sharing_config,
):
    """The archive has no version column, so the file name is what marks a version.

    A versioned dataset's marker is a storage detail of the data repository and
    is not part of the archived data, so the archive has to be able to say which
    version a file holds from the file itself.
    """
    (expected, dataset) = dataset_with_sharing_config
    dataset.save()

    dataset.archive()
    dataset.archive()

    archived = sorted(
        path.name
        for path in (expected["expected_archive_path"] / dataset.name).glob("*.parquet")
    )
    assert [name.rsplit("_v", 1)[-1] for name in archived] == ["1.parquet", "2.parquet"]


def test_a_sharing_key_carries_no_storage(dataset_with_sharing_config):
    """A dataset names where its archive belongs, never where those places are.

    The keys travel to the handler as they are, so that resolving them to a
    location stays the configuration's business.
    """
    (_, dataset) = dataset_with_sharing_config

    assert all(isinstance(key, str) for key in dataset.sharing)
    assert dataset.ref.sharing == list(dataset.sharing)
