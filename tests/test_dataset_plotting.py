import logging

from matplotlib import pyplot as plt
from pytest import LogCaptureFixture

import ssb_timeseries as ts
from ssb_timeseries.dataset import Dataset

# magic comment disables mypy checks:
# mypy: disable-error-code="attr-defined,no-untyped-def"
# arg-type,,union-attr


def test_dataset_plot_none_at_returns_axes(
    new_dataset_none_at: Dataset, caplog: LogCaptureFixture
):
    caplog.set_level(logging.DEBUG)

    p = new_dataset_none_at.plot()
    ts.logger.debug(f"p = dataset.plot(): {p}")

    assert isinstance(p, plt.Axes)


def test_dataset_plot_as_of_at_returns_axes(
    new_dataset_as_of_at: Dataset, caplog: LogCaptureFixture
):
    caplog.set_level(logging.DEBUG)

    p = new_dataset_as_of_at.plot()
    ts.logger.debug(f"p = dataset.plot(): {p}")

    assert isinstance(p, plt.Axes)


def test_dataset_plot_as_of_from_to_returns_axes(
    new_dataset_as_of_from_to: Dataset, caplog: LogCaptureFixture
):
    caplog.set_level(logging.DEBUG)

    p = new_dataset_as_of_from_to.plot()
    ts.logger.debug(f"p = dataset.plot(): {p}")

    assert isinstance(p, plt.Axes)


def test_dataset_plot_none_from_to_returns_axes(
    new_dataset_none_from_to: Dataset, caplog: LogCaptureFixture
):
    caplog.set_level(logging.DEBUG)

    p = new_dataset_none_from_to.plot()
    ts.logger.debug(f"p = dataset.plot(): {p}")

    assert isinstance(p, plt.Axes)
