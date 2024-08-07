# mypy: ignore-errors = True

import logging

import pytest

# from ssb_timeseries.catalog import CatalogItem
from ssb_timeseries.catalog import DataCatalog
from ssb_timeseries.catalog import ObjectType
from ssb_timeseries.logging import ts_logger

# from ssb_timeseries.meta import Taxonomy


@pytest.mark.xfail
def test_datasets_with_no_params_lists_all_datasets(
    one_new_set_for_each_data_type,
    buildup_and_teardown,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Find datasets where ."""
    caplog.set_level(logging.DEBUG)
    config = buildup_and_teardown
    ts_logger.debug(f"{config}")
    for ds in one_new_set_for_each_data_type:
        ts_logger.debug(f"{ds.name}\t{ds.io.metadata_fullpath}")
    expected = {ds.name for ds in one_new_set_for_each_data_type}

    catalog = DataCatalog(directory=config.catalog)
    all_sets = {ds[-1] for ds in catalog.datasets()}
    ts_logger.debug(f"{all_sets=}")
    assert all_sets >= expected
    assert catalog.count(ObjectType.DATASET) >= 4


def test_find_datasets_using_single_set_attribute(
    caplog,
) -> None:
    """Find datasets where ."""
    caplog.set_level(logging.DEBUG)

    ...
