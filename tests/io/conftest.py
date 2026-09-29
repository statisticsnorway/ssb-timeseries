from pathlib import Path

import pytest

# mypy: disable-error-code="no-untyped-def,no-untyped-call,arg-type,attr-defined,assignment"


# ========================= test setup for archiving ================================

PRODUCT = "sample-data-product"
PROCESS_STAGE = "statistikk"


@pytest.fixture
def use_archive_format(monkeypatch):
    """Choose the archive convention the configuration asks for.

    The convention is configuration, so a test that wants a different one says
    so in the configuration rather than reaching into the handler.
    """
    from ssb_timeseries.config import Config

    def _use(name):
        monkeypatch.setitem(
            Config.active().snapshots["default"],
            "options",
            {"archive_format": name},
        )

    return _use


@pytest.fixture
def sharing_configs(conftest) -> tuple:
    """Read base paths only once."""
    config = conftest.configuration

    persisted = Path(config["snapshots"]["default"]["directory"]["options"]["path"])
    shared = Path(config["sharing"]["default"]["directory"]["options"]["path"])
    shared_123 = Path(config["sharing"]["s123"]["directory"]["options"]["path"])
    shared_234 = Path(config["sharing"]["s234"]["directory"]["options"]["path"])

    return (persisted, shared, shared_123, shared_234)


@pytest.fixture
def without_specified_teams(sharing_configs) -> dict:
    """A key with no location of its own falls back to the default one."""
    (persisted, shared, _, _) = sharing_configs
    sharing = [
        "not-a-configured-team",
        "also-not-configured",
    ]

    return {
        "process_stage": PROCESS_STAGE,
        "product": PRODUCT,
        "sharing": sharing,
        "expected_archive_path": persisted,
        "expected_sharing_path_123": shared,
        "expected_sharing_path_234": shared,
    }


@pytest.fixture
def with_specified_teams(sharing_configs) -> dict:
    """Keys name configured locations, and each has one of its own."""
    (persisted, _, shared_123, shared_234) = sharing_configs
    sharing = [
        "s123",
        "s234",
    ]

    return {
        "process_stage": PROCESS_STAGE,
        "product": PRODUCT,
        "sharing": sharing,
        "expected_archive_path": persisted,
        "expected_sharing_path_123": shared_123,
        "expected_sharing_path_234": shared_234,
    }


@pytest.fixture
def no_product(sharing_configs) -> dict:
    """Specifying a product is optional the configuration."""
    (persisted, _, shared_123, shared_234) = sharing_configs
    sharing = [
        "s123",
        "s234",
    ]

    return {
        "process_stage": PROCESS_STAGE,
        # "product": "",
        "sharing": sharing,
        "expected_archive_path": persisted,
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
    return (cfg, dataset)
