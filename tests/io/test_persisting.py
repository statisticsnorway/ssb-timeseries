import logging
from copy import deepcopy

import pytest

from ssb_timeseries.io import fs

from ..fixtures.dataset_factories import function_name_hex

# mypy: disable-error-code="no-untyped-def,no-untyped-call,arg-type,attr-defined,assignment"

test_logger = logging.getLogger(__name__)


def log(path, before, after):
    test_logger.debug(
        f"SNAPSHOT to {path}\n\thas file count before:{before}, and after: {after}"
    )


# ----- The tests --------------


def test_snapshot_after_save_does_not_raise_error(
    caplog,
    xyz_at,
):
    from ssb_timeseries.dataset import Dataset

    caplog.set_level(logging.DEBUG)

    ds = Dataset(name=function_name_hex(), data=xyz_at, data_type="simple")

    ds.save()
    ds.snapshot()


def test_snapshot_without_save_raises_error(
    caplog,
    xyz_at,
):
    from ssb_timeseries.dataset import Dataset

    caplog.set_level(logging.DEBUG)

    ds = Dataset(name=function_name_hex(), data=xyz_at, data_type="simple")

    with pytest.raises(FileNotFoundError):
        # no ds.save() to see here!
        ds.snapshot()


def test_snapshot_does_not_mutate_the_sharing_configuration(
    dataset_with_sharing_config,
):
    """A snapshot must not write its defaults back into the dataset's configuration.

    `DatasetRef` is documented as a snapshot rather than a live view, so a
    handler that filled in a missing team would be editing the dataset it was
    handed, and every ref built before that call as well.

    The fixture is parametrised over several sharing configurations, one of which
    deliberately leaves a team unspecified, so the defaulting path is covered.
    """
    (_, dataset) = dataset_with_sharing_config
    sharing_before = deepcopy(dataset.sharing)

    dataset.save()
    dataset.snapshot()

    assert dataset.sharing == sharing_before


def test_snapshot_and_sharing_increases_file_count_in_configured_locations(
    caplog,
    dataset_with_sharing_config,
):
    caplog.set_level(logging.DEBUG)
    (expected, dataset) = dataset_with_sharing_config

    persisted = expected["expected_snapshot_path"] / dataset.name
    path_123 = expected["expected_sharing_path_123"] / dataset.name
    path_234 = expected["expected_sharing_path_234"] / dataset.name

    count_before_persisted = fs.file_count(persisted, create=True)
    count_before_123 = fs.file_count(path_123, create=True)
    count_before_234 = fs.file_count(path_234, create=True)

    n = 2
    dataset.save()

    dataset.snapshot()
    dataset.snapshot()

    count_after_persisted = fs.file_count(persisted)
    count_after_123 = fs.file_count(path_123)
    count_after_234 = fs.file_count(path_234)

    log(persisted, count_before_persisted, count_after_persisted)
    log(path_123, count_before_123, count_after_123)
    log(path_234, count_before_234, count_after_234)

    assert count_after_persisted == count_before_persisted + n
    assert count_after_123 == count_before_123 + n
    assert count_after_234 == count_before_234 + n
