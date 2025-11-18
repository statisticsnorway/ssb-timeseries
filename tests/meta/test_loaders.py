"""Tests for the ssb_timeseries.meta.loaders module."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import narwhals as nw
import pandas as pd
import pyarrow as pa
import pytest

from ssb_timeseries.meta.loaders import KLASS_ITEM_SCHEMA
from ssb_timeseries.meta.loaders import DataLoader
from ssb_timeseries.meta.loaders import FileLoader
from ssb_timeseries.meta.loaders import KlassLoader


@pytest.fixture
def sample_klass_data() -> list[dict]:
    """Provides sample KLASS data in the expected format."""
    return [
        {"code": "0", "parentCode": None, "name": "KLASS-123", "level": "0"},
        {"code": "1", "parentCode": "0", "name": "Item 1", "level": "1"},
        {"code": "1.1", "parentCode": "1", "name": "Subitem 1.1", "level": "2"},
        {"code": "2", "parentCode": "0", "name": "Item 2", "level": "1"},
    ]


@pytest.fixture
def sample_arrow_table(sample_klass_data: list[dict]) -> pa.Table:
    """Provides sample PyArrow Table."""
    return pa.Table.from_pylist(sample_klass_data, schema=KLASS_ITEM_SCHEMA)


class TestDataLoader:
    def test_load_from_list_of_dicts(self, sample_klass_data: list[dict]) -> None:
        loader = DataLoader(sample_klass_data)
        table = loader.load()
        assert isinstance(table, pa.Table)
        assert table.num_rows == len(sample_klass_data)
        assert table.schema == KLASS_ITEM_SCHEMA

    def test_load_from_pandas_dataframe(self, sample_klass_data: list[dict]) -> None:
        df = pd.DataFrame(sample_klass_data)
        loader = DataLoader(df)
        table = loader.load()
        assert isinstance(table, pa.Table)
        assert table.num_rows == len(sample_klass_data)
        assert table.schema == KLASS_ITEM_SCHEMA

    def test_load_from_narwhals_dataframe(self, sample_klass_data: list[dict]) -> None:
        df = nw.from_native(pd.DataFrame(sample_klass_data))
        loader = DataLoader(df)
        table = loader.load()
        assert isinstance(table, pa.Table)
        assert table.num_rows == len(sample_klass_data)
        assert table.schema == KLASS_ITEM_SCHEMA

    def test_load_unsupported_type_raises_error(self) -> None:
        with pytest.raises(TypeError, match="Unsupported data type for DataLoader"):
            DataLoader(123).load()  # type: ignore[arg-type]


class TestFileLoader:
    def test_load_from_json_file(
        self, tmp_path: Path, sample_klass_data: list[dict]
    ) -> None:
        file_path = tmp_path / "test_taxonomy.json"
        with open(file_path, "w") as f:
            json.dump(sample_klass_data, f)

        loader = FileLoader(file_path)
        table = loader.load()
        assert isinstance(table, pa.Table)
        assert table.num_rows == len(sample_klass_data)
        assert table.schema == KLASS_ITEM_SCHEMA

    def test_load_non_existent_file_raises_error(self, tmp_path: Path) -> None:
        file_path = tmp_path / "non_existent.json"
        loader = FileLoader(file_path)
        with pytest.raises(FileNotFoundError):
            loader.load()


class TestKlassLoader:
    @patch("ssb_timeseries.meta.loaders.get_classification")
    def test_load_from_klass_api(
        self, mock_get_classification: MagicMock, sample_klass_data: list[dict]
    ) -> None:
        KlassLoader._klass_classification.cache_clear()  # Clear cache before test
        # Mock the behavior of klass.get_classification
        mock_classification_instance = MagicMock()
        mock_classification_instance.get_codes.return_value.data.to_dict.return_value = sample_klass_data[
            1:
        ]  # Exclude root node
        mock_get_classification.return_value = mock_classification_instance

        loader = KlassLoader(klass_id=123)
        table = loader.load()

        mock_get_classification.assert_called_once_with("123")
        assert isinstance(table, pa.Table)
        assert table.num_rows == len(sample_klass_data)
        assert table.schema == KLASS_ITEM_SCHEMA

        # Verify the root node is correctly added by the loader
        root_node_row = table.filter(pa.compute.field("code") == "0").to_pylist()[0]
        assert root_node_row["name"] == "KLASS-123"
        assert root_node_row["parentCode"] is None

    @patch("ssb_timeseries.meta.loaders.get_classification")
    def test_klass_data_with_missing_parent_code(
        self, mock_get_classification: MagicMock
    ) -> None:
        KlassLoader._klass_classification.cache_clear()  # Clear cache before test
        # Simulate KLASS data where some items are missing parentCode
        klass_data_missing_parent = [
            {"code": "1", "parentCode": None, "name": "Item 1", "level": "1"},
            {"code": "1.1", "parentCode": "1", "name": "Subitem 1.1", "level": "2"},
        ]
        mock_classification_instance = MagicMock()
        mock_classification_instance.get_codes.return_value.data.to_dict.return_value = klass_data_missing_parent
        mock_get_classification.return_value = mock_classification_instance

        loader = KlassLoader(klass_id=456)
        table = loader.load()

        # Verify that the root node is correctly assigned as parent for items with missing parentCode
        item_1_row = table.filter(pa.compute.field("code") == "1").to_pylist()[0]
        assert item_1_row["parentCode"] == "0"
