import logging

import ssb_timeseries as ts
from ssb_timeseries import fs
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc

# from ssb_timeseries.io import CONFIG
from ssb_timeseries.logging import log_start_stop
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# mypy: disable-error-code="no-untyped-def,no-untyped-call,arg-type,attr-defined,assignment"


# BUCKET = CONFIG.bucket
PRODUCT = "sample-data-product"


@log_start_stop
def test_snapshot_simple_set_has_higher_snapshot_file_count_after(
    conftest,
    caplog,
):
    caplog.set_level(logging.DEBUG)
    bucket = conftest.configuration.bucket
    x = Dataset(
        name=conftest.function_name(),
        data_type=SeriesType.simple(),
        load_data=False,
        data=create_df(
            ["p", "q", "r"], start_date="2022-01-01", end_date="2022-12-31", freq="YS"
        ),
    )
    x.process_stage = "statistikk"
    x.product = PRODUCT

    stage_path = x.io.snapshot_directory(
        product=x.product, process_stage=x.process_stage
    )
    shared_base_path = x.io.dir(bucket, x.product, "shared", "all")
    path_123 = shared_base_path
    path_234 = shared_base_path
    x.sharing = [
        {
            # should work even if tartget team is not specified(?
            "path": path_123,
        },
        {
            "team": "",
            "path": path_234,
        },
    ]

    x.save()

    path_123 = x.io.dir(path_123, x.name)
    path_234 = x.io.dir(path_234, x.name)

    count_before_snapshot = fs.file_count(stage_path, create=True)
    count_before_123 = fs.file_count(path_123, create=True)
    count_before_234 = fs.file_count(path_234, create=True)

    x.snapshot()

    count_after_snapshot = fs.file_count(stage_path)
    count_after_123 = fs.file_count(path_123)
    count_after_234 = fs.file_count(path_234)

    def log(path, before, after):
        ts.logger.debug(
            f"SNAPSHOT to {path}\n\tfile count before:{before}, after: {after}"
        )

    log(stage_path, count_before_snapshot, count_after_snapshot)
    log(path_123, count_before_123, count_after_123)
    log(path_234, count_before_234, count_after_234)

    assert count_before_snapshot < count_after_snapshot
    assert count_before_123 < count_after_123
    assert count_before_234 < count_after_234


@log_start_stop
def test_snapshot_estimate_specified_has_higher_file_count_after(conftest, caplog):
    caplog.set_level(logging.DEBUG)
    bucket = conftest.configuration.bucket
    x = Dataset(
        name=conftest.function_name(),
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        load_data=False,
        data=create_df(
            ["p", "q", "r"], start_date="2022-01-01", end_date="2022-12-31", freq="YS"
        ),
    )
    x.process_stage = "statistikk"
    x.product = PRODUCT
    stage_path = x.io.snapshot_directory(
        product=x.product, process_stage=x.process_stage
    )
    shared_base_path = x.io.dir(bucket, x.product, "shared")
    team_path_123 = x.io.dir(shared_base_path, "s123")
    team_path_234 = x.io.dir(shared_base_path, "s234")
    x.sharing = [
        {
            "team": "s123",
            "path": team_path_123,
        },
        {
            "team": "s234",
            "path": team_path_234,
        },
    ]

    path_123 = x.io.dir(team_path_123, x.name)
    path_234 = x.io.dir(team_path_234, x.name)
    x.save()
    ts.logger.debug(f"SNAPSHOT conf.bucket {bucket}")
    ts.logger.debug(f"SNAPSHOT to {path_123}")

    count_before_snapshot = fs.file_count(stage_path, create=True)
    count_before_123 = fs.file_count(path_123, create=True)
    count_before_234 = fs.file_count(path_234, create=True)

    x.snapshot()

    count_after_snapshot = fs.file_count(stage_path)
    count_after_123 = fs.file_count(path_123)
    count_after_234 = fs.file_count(path_234)

    def log(path, before, after):
        ts.logger.debug(
            f"SNAPSHOT to {path}\n\tfile count before:{before}, after: {after}"
        )

    log(stage_path, count_before_snapshot, count_after_snapshot)
    log(path_123, count_before_123, count_after_123)
    log(path_234, count_before_234, count_after_234)

    assert count_before_snapshot < count_after_snapshot
    assert count_before_123 < count_after_123
    assert count_before_234 < count_after_234
