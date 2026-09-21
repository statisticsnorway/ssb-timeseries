from pathlib import Path

import pytest

# mypy: disable-error-code="no-untyped-def,no-untyped-call,arg-type,attr-defined,assignment"


# ========================= test setup for archiving ================================

PRODUCT = "sample-data-product"
PROCESS_STAGE = "statistikk"


@pytest.fixture
def sharing_configs(conftest) -> tuple:
    """Read base paths only once."""
    config = conftest.configuration

    persisted = Path(config["snapshots"]["default"]["directory"]["options"]["path"])
    shared = Path(config["sharing"]["default"]["directory"]["options"]["path"])
    shared_123 = Path(config["sharing"]["s123"]["directory"]["options"]["path"])
    shared_234 = Path(config["sharing"]["s234"]["directory"]["options"]["path"])

    yield (persisted, shared, shared_123, shared_234)


@pytest.fixture
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


@pytest.fixture
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


@pytest.fixture
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


# -------- multiple config scenarios for sharing --------------------


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
