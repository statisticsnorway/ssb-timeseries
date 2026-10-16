"""Unit tests for the `DatasetRef` I/O support guard."""

import pytest

from ssb_timeseries.io.dataset_ref import DatasetRef
from ssb_timeseries.types import AS_OF_AT
from ssb_timeseries.types import NONE_AT
from ssb_timeseries.types import SeriesType
from ssb_timeseries.types import Temporality
from ssb_timeseries.types import Versioning

NAMES_AT = SeriesType(versioning=Versioning.NAMES, temporality=Temporality.AT)


def test_require_supported_rejects_named_versions():
    """`Versioning.NAMES` is reserved but not implemented by any handler."""
    ref = DatasetRef(name="named", data_type=NAMES_AT)
    with pytest.raises(NotImplementedError, match=r"Versioning\.NAMES"):
        ref.require_supported()


def test_require_supported_accepts_implemented_versionings():
    """The versionings the I/O layer implements pass the guard."""
    for data_type in (NONE_AT, AS_OF_AT):
        DatasetRef(name="supported", data_type=data_type).require_supported()


def test_require_identified_reports_unsupported_versioning_first():
    """The support guard wins over the 'as of' completeness check."""
    ref = DatasetRef(name="named", data_type=NAMES_AT)
    with pytest.raises(NotImplementedError, match=r"Versioning\.NAMES"):
        ref.require_identified()
