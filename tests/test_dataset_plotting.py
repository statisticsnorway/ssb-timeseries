import logging

from matplotlib import pyplot as plt
from pytest import LogCaptureFixture

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.logging import ts_logger

# magic comment disables mypy checks:
## mypy: disable-error-code="arg-type,attr-defined,no-untyped-def,union-attr"


def test_dataset_none_at_plot_returns_axes(
    in_memory_none_at: Dataset, caplog: LogCaptureFixture
):
    caplog.set_level(logging.DEBUG)

    x = in_memory_none_at
    y = x.plot()

    assert isinstance(y, plt.Axes)


def test_dataset_plot_as_of_at_plot_returns_axes(
    in_memory_as_of_at: Dataset, caplog: LogCaptureFixture
):
    caplog.set_level(logging.DEBUG)

    x = in_memory_as_of_at
    y = x.plot()

    assert isinstance(y, plt.Axes)


def test_dataset_plot_as_of_from_to_returns_axes(
    in_memory_as_of_from_to: Dataset, caplog: LogCaptureFixture
):
    caplog.set_level(logging.DEBUG)

    x = in_memory_as_of_from_to
    y = x.plot()
    ts_logger.debug(f"y = x.filter(regex='^x')\n{y}")

    assert isinstance(y, plt.Axes)


def test_dataset_plot_none_from_to_returns_axes(
    in_memory_none_from_to: Dataset, caplog: LogCaptureFixture
):
    caplog.set_level(logging.DEBUG)

    x = in_memory_none_from_to
    y = x.plot()
    ts_logger.debug(f"y = x.filter(regex='^x')\n{y}")

    assert isinstance(y, plt.Axes)


# @pytest.mark.skip(reason="TODO: revisit dataset.__repr__.")
