import logging
from pathlib import Path

import pytest

from ssb_timeseries.io import fs

# mypy: disable-error-code="no-untyped-def,no-untyped-call,arg-type,attr-defined,assignment"

test_logger = logging.getLogger(__name__)

# ========================= test setup ================================

# BUCKET = CONFIG.bucket
PRODUCT = "sample-data-product"
PROCESS_STAGE = "statistikk"


@pytest.fixture(scope="function")
def sharing_configs(conftest) -> tuple:
    """Read base paths only once."""
    config = conftest.configuration

    persisted = Path(config["snapshots"]["default"]["directory"]["options"]["path"])
    shared = Path(config["sharing"]["default"]["directory"]["options"]["path"])
    shared_123 = Path(config["sharing"]["s123"]["directory"]["options"]["path"])
    shared_234 = Path(config["sharing"]["s234"]["directory"]["options"]["path"])

    yield (persisted, shared, shared_123, shared_234)


@pytest.fixture(scope="function")
def without_specified_teams(sharing_configs) -> dict:
    """Team name is not required in the configuration."""
    (persisted, shared, _, _) = sharing_configs
    sharing = [
        {
            "team": "",
            "path": shared,
        },
        {
            # target team is *really* not specified
            "path": shared,
        },
    ]

    yield {
        "process_stage": PROCESS_STAGE,
        "product": PRODUCT,
        "sharing": sharing,
        "expected_snapshot_path": persisted / PROCESS_STAGE / PRODUCT,
        "expected_sharing_path_123": shared,
        "expected_sharing_path_234": shared,
    }


@pytest.fixture(scope="function")
def with_specified_teams(sharing_configs) -> dict:
    """Team name is not required in the configuration."""
    (persisted, _, shared_123, shared_234) = sharing_configs
    sharing = [
        {
            "team": "s123",
            "path": shared_123,
        },
        {
            "team": "s234",
            "path": shared_234,
        },
    ]

    yield {
        "process_stage": PROCESS_STAGE,
        "product": PRODUCT,
        "sharing": sharing,
        "expected_snapshot_path": persisted / PROCESS_STAGE / PRODUCT,
        "expected_sharing_path_123": shared_123,
        "expected_sharing_path_234": shared_234,
    }


@pytest.fixture(scope="function")
def no_product(sharing_configs) -> dict:
    """Specifying a product is optional the configuration."""
    (persisted, _, shared_123, shared_234) = sharing_configs
    sharing = [
        {
            "team": "s123",
            "path": shared_123,
        },
        {
            "team": "s234",
            "path": shared_234,
        },
    ]

    yield {
        "process_stage": PROCESS_STAGE,
        # "product": "",
        "sharing": sharing,
        "expected_snapshot_path": persisted / PROCESS_STAGE,
        "expected_sharing_path_123": shared_123,
        "expected_sharing_path_234": shared_234,
    }


@pytest.fixture(
    params=[
        "with_specified_teams",
        "without_specified_teams",
        "no_product",
    ],
    scope="function",
)
def dataset_with_sharing_config(
    request,
    one_new_set_for_each_data_type,
) -> tuple:
    """Combines parameter sets with datasets of all types to create complete test cases."""
    cfg = request.getfixturevalue(request.param)
    dataset = one_new_set_for_each_data_type
    dataset.process_stage = cfg.pop("process_stage")
    if "product" in cfg:
        dataset.product = cfg.pop("product")
    dataset.sharing = cfg.pop("sharing")
    yield (cfg, dataset)


def log(path, before, after):
    test_logger.debug(
        f"SNAPSHOT to {path}\n\thas file count before:{before}, and after: {after}"
    )


# ========================= the tests ================================


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
