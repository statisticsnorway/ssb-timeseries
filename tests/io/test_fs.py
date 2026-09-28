import inspect
import logging
import os
import uuid
from pathlib import Path
from types import SimpleNamespace

import polars
import pyarrow
import pytest
from fsspec.implementations.local import LocalFileSystem

import ssb_timeseries as ts
from ssb_timeseries.io import fs
from ssb_timeseries.sample_data import create_df

# mypy: ignore-errors

BUCKET = "gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/"
JOVYAN = "/home/jovyan/series_data/"
HOME = str(Path.home())
IS_DAPLA = HOME == "/home/jovyan"


@pytest.fixture(scope="function", autouse=True)
def df():
    simple_data = create_df(
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-06-03",
        freq="MS",
    )
    yield simple_data


@pytest.mark.skipif(not os.getenv("DAPLA_TEAM_CONTEXT"), reason="Not on Dapla")
def test_bucket_exists_if_running_on_dapla() -> None:
    ts.logger.warning(f"Home directory is {HOME}")
    assert fs.exists(BUCKET)


def test_remove_prefix() -> None:
    assert (
        fs.remove_prefix("gs://ssb-prod-dapla-felles-data-delt")
        == "ssb-prod-dapla-felles-data-delt"
    )
    assert fs.remove_prefix("/home/jovyan") == "/home/jovyan"


def test_is_gcs() -> None:
    assert fs.is_gcs("gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/")
    assert not fs.is_gcs("/home/jovyan")


def test_is_local() -> None:
    assert not fs.is_local("gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/")
    assert fs.is_local("/home/jovyan")


def test_fs_type() -> None:
    assert fs.fs_type("gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/") == "gcs"
    assert fs.fs_type("/home/jovyan") == "local"


def test_fs_path() -> None:
    assert fs.path(BUCKET, "a", "b", "c") == fs.path_to_str(
        "gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/a/b/c"
    )
    assert fs.path(JOVYAN, "a", "b", "c") == fs.path_to_str(
        "/home/jovyan/series_data/a/b/c"
    )


@pytest.mark.skipif(not os.getenv("DAPLA_TEAM_CONTEXT"), reason="Not on Dapla")
def test_same_path() -> None:
    assert (
        fs.same_path(BUCKET, "/home/jovyan") == fs.path_to_str("/")
        # or fs.same_path(BUCKET, "/home/jovyan") == "\\"
    )
    assert fs.same_path(
        fs.path("/home/jovyan/a"),
        fs.path("/home/jovyan"),
    ) == fs.path("/home/jovyan")
    assert (
        fs.same_path(
            "/ssb-prod-dapla-felles-data-delt/poc-tidsserier",
            "/ssb-prod-dapla-felles-data-delt/poc-tidsserier/a",
        )
        == "/ssb-prod-dapla-felles-data-delt/poc-tidsserier"
    )


def test_home_exists() -> None:
    assert fs.exists(HOME)


def test_existing_subpath() -> None:
    long_path = Path(HOME) / f"this-dir-does-not-to-exist-{uuid.uuid4()}"
    assert fs.exists(HOME)
    assert not fs.exists(long_path)
    assert str(fs.existing_subpath(long_path)) == str(HOME)


def test_touch_creates_file_rm_removes_it(tmp_path) -> None:
    long_path = tmp_path / f"this-dir-does-not-to-exist-{uuid.uuid4()}/file.txt"
    assert not fs.exists(long_path)
    fs.touch(long_path)
    assert fs.exists(long_path)
    fs.rm(long_path)
    assert not fs.exists(long_path)


def test_to_arrow(caplog, tmp_path, df) -> None:
    caplog.set_level(logging.DEBUG)
    assert not isinstance(df, pyarrow.Table)
    table = fs.to_arrow(df)
    assert isinstance(table, pyarrow.Table)


def test_write_parquet_with_no_schema_creates_a_file(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)
    temp_file = tmp_path / "no_schema_sample.parquet"
    assert not fs.exists(temp_file)
    fs.write_parquet(
        path=temp_file,
        data=fs.to_arrow(df),
        schema=None,
    )
    assert fs.exists(temp_file)
    xyz = fs.read_parquet(path=temp_file, implementation="pandas").to_native()
    assert all(df == xyz)


def test_write_parquet_with_hardcoded_schema_creates_a_file(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)
    temp_file = tmp_path / "no_schema_sample.parquet"
    schema = pyarrow.schema(
        [
            pyarrow.field("valid_at", pyarrow.date64(), nullable=False),
            pyarrow.field("x", pyarrow.float64(), nullable=True),
            pyarrow.field("y", pyarrow.float64(), nullable=True),
            pyarrow.field("z", pyarrow.float64(), nullable=True),
        ]
    )
    assert not fs.exists(temp_file)
    fs.write_parquet(
        path=temp_file,
        data=fs.to_arrow(df),
        schema=schema,
    )
    assert fs.exists(temp_file)


def test_write_parquet_fails_if_date_columns_does_not_match_schema(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)
    temp_file = tmp_path / "no_schema_sample.parquet"
    # df has valid_at datecolumn
    # --> create schema with valid_from and valid_to
    schema_with_wrong_date_columns = pyarrow.schema(
        [
            pyarrow.field("valid_from", pyarrow.date64(), nullable=False),
            pyarrow.field("valid_to", pyarrow.date64(), nullable=False),
            pyarrow.field("x", pyarrow.float64(), nullable=True),
            pyarrow.field("y", pyarrow.float64(), nullable=True),
            pyarrow.field("z", pyarrow.float64(), nullable=True),
        ]
    )
    with pytest.raises(KeyError):
        fs.write_parquet(
            path=temp_file,
            data=fs.to_arrow(df),
            schema=schema_with_wrong_date_columns,
        )


# def test_write_parquet_raises_key_error_if_df_contains_column_not_defined_in_schema(
def test_write_parquet_handles_that_df_contains_column_not_defined_in_schema_but_writes_only_the_defined_ones(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)
    temp_file = tmp_path / "no_schema_sample.parquet"
    data_with_z = df
    # df has columns valid_at, x, y, z
    # --> create schema with only valid_at, x, y
    schema_without_z = pyarrow.schema(
        [
            pyarrow.field("valid_at", pyarrow.date64(), nullable=False),
            pyarrow.field("x", pyarrow.float64(), nullable=True),
            pyarrow.field("y", pyarrow.float64(), nullable=True),
        ]
    )
    # with pytest.raises(KeyError):
    #     fs.write_parquet(
    #         path=temp_file,
    #         data=data_with_z,
    #         schema=schema_without_z,
    #     )
    fs.write_parquet(
        path=temp_file,
        data=data_with_z,
        schema=schema_without_z,
    )
    assert fs.exists(temp_file)
    read_back = fs.read_parquet(path=temp_file)
    assert sorted(read_back.columns) == sorted(schema_without_z.names)
    # assert sorted(read_back.columns) == sorted(df.columns)


def test_write_parquet_handles_that_df_and_schema_columns_are_not_in_same_order(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)
    temp_file = tmp_path / "no_schema_sample.parquet"
    # df has columns valid_at, x, y, z
    # --> create schema with same columns in different order: valid_at, x, z, y
    schema_with_columns_out_of_order = pyarrow.schema(
        [
            pyarrow.field("valid_at", pyarrow.date64(), nullable=False),
            pyarrow.field("x", pyarrow.float64(), nullable=True),
            pyarrow.field("z", pyarrow.float64(), nullable=True),
            pyarrow.field("y", pyarrow.float64(), nullable=True),
        ]
    )
    # writing will reorder the data columns to match the schema
    fs.write_parquet(
        path=temp_file,
        data=fs.to_arrow(df),
        schema=schema_with_columns_out_of_order,
    )
    assert fs.exists(temp_file)


def test_write_parquet_fails_if_df_does_not_contain_all_schema_columns(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)
    temp_file = tmp_path / "no_schema_sample.parquet"
    # df has columns x, y, z
    # --> create schema with x and y
    schema_with_extra_columns = pyarrow.schema(
        [
            pyarrow.field("valid_at", pyarrow.date64(), nullable=False),
            pyarrow.field("x", pyarrow.float64(), nullable=True),
            pyarrow.field("y", pyarrow.float64(), nullable=True),
            pyarrow.field("z", pyarrow.float64(), nullable=True),
            pyarrow.field("æ", pyarrow.float64(), nullable=True),
            pyarrow.field("ø", pyarrow.float64(), nullable=True),
            pyarrow.field("å", pyarrow.float64(), nullable=True),
        ]
    )
    with pytest.raises(KeyError):
        fs.write_parquet(
            path=temp_file,
            data=fs.to_arrow(df),
            schema=schema_with_extra_columns,
        )


def test_write_parquet_supports_pandas_df_input(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)

    # df is already pandas
    temp_file = tmp_path / "pandas_df.parquet"
    fs.write_parquet(
        path=temp_file,
        data=df,
        # schema=None,
    )
    assert fs.exists(temp_file)


def test_write_parquet_supports_polars_df_input(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)

    # polars
    temp_file = tmp_path / "polars_df.parquet"
    fs.write_parquet(
        path=temp_file,
        data=polars.from_pandas(df),
        schema=None,
    )
    assert fs.exists(temp_file)


def test_write_parquet_supports_arrow_table_input(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)

    # arrow
    temp_file = tmp_path / "arrow_table.parquet"
    fs.write_parquet(
        path=temp_file,
        data=fs.to_arrow(df),
        schema=None,
    )
    assert fs.exists(temp_file)


# ------------------------------------------------------------------
# find
# ------------------------------------------------------------------

FIND_TREE_FILES = [
    "a.json",
    "b.txt",
    "sub1/c.json",
    "sub1/sub1_deep/d.json",
    "sub2/e.json",
]


@pytest.fixture
def find_root(tmp_path):
    """A repository-like tree with entries at three depths.

    find_root/a.json, find_root/b.txt
    find_root/sub1/c.json, find_root/sub1/sub1_deep/d.json, find_root/sub2/e.json
    """
    root = tmp_path / "repo"
    for relative in FIND_TREE_FILES:
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("{}")
    return root


@pytest.fixture
def gcs_bucket(tmp_path):
    """A bucket holding the same tree as `find_root`, addressed as a gs:// URL.

    Returns the bucket directory. Its children mirror `find_root`, so
    `gs://<bucket.name>/repo` resolves to the same shape as `find_root`.
    """
    bucket = tmp_path / "bucket"
    for relative in FIND_TREE_FILES:
        target = bucket / "repo" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("{}")
    return bucket


@pytest.fixture
def fake_gcs(monkeypatch, gcs_bucket):
    """A real fsspec filesystem standing in for GCSFileSystem.

    Only `_strip_protocol` is overridden, so `glob` and `find` are the inherited
    `AbstractFileSystem` implementations. That is the point: `find` does not glob,
    so a change back to `find` fails here on behaviour instead of on a mock. Like
    `gcsfs`, stripping the protocol keeps the bucket as a path segment, which is
    what lets `glob` match its pattern against the names it gets back from `find`.
    """
    calls: list = []
    bucket_dir = gcs_bucket
    bucket_name = gcs_bucket.name

    class LocalBucketFileSystem(LocalFileSystem):
        protocol = "gcs"

        def _strip_protocol(cls, path):
            return super()._strip_protocol(
                str(path).replace(f"gs://{bucket_name}", str(bucket_dir))
            )

        def glob(self, path, *args, **kwargs):
            calls.append(("glob", str(path)))
            return super().glob(path, *args, **kwargs)

    monkeypatch.setattr(fs, "GCSFileSystem", LocalBucketFileSystem)
    return SimpleNamespace(calls=calls)


def test_find_no_longer_offers_a_full_path_option() -> None:
    """Assert that the lossy `full_path=False` mode stays removed.

    It reduced every match to its last path component, which silently merged
    equally named entries in different subdirectories.
    """
    assert "full_path" not in inspect.signature(fs.find).parameters


def test_find_returns_full_paths_of_the_top_level_entries_only(
    find_root,
) -> None:
    found = fs.find(find_root, search_sub_dirs=False)
    assert found == [
        str(find_root / "a.json"),
        str(find_root / "b.txt"),
        str(find_root / "sub1"),
        str(find_root / "sub2"),
    ]


def test_find_keeps_equally_named_entries_in_different_subdirectories_distinct(
    tmp_path,
) -> None:
    """Assert that equally named entries in different subdirectories stay distinct.

    The dropped `full_path=False` mode reduced every match to its last
    component, so these two entries used to collapse into one name.
    """
    for sub in ("first", "second"):
        (tmp_path / sub).mkdir()
        (tmp_path / sub / "0.parquet").write_text("")

    found = fs.find(tmp_path, pattern="0.parquet", search_sub_dirs=True)

    assert found == [
        str(tmp_path / "first" / "0.parquet"),
        str(tmp_path / "second" / "0.parquet"),
    ]
    assert len(set(found)) == 2


def test_find_with_contains_returns_only_the_paths_containing_the_substring(
    find_root,
) -> None:
    found = fs.find(find_root, contains="c.json")
    assert found == [str(find_root / "sub1" / "c.json")]


def test_find_with_equals_returns_only_the_exactly_matching_path(find_root) -> None:
    found = fs.find(find_root, equals="b.txt", search_sub_dirs=False)
    assert found == [str(find_root / "b.txt")]


def test_find_with_search_sub_dirs_true_reaches_one_level_below_but_not_further(
    find_root,
) -> None:
    found = fs.find(find_root, pattern="*.json", search_sub_dirs=True)
    assert found == [
        str(find_root / "sub1" / "c.json"),
        str(find_root / "sub2" / "e.json"),
    ]


def test_find_with_recursive_true_matches_files_at_every_depth(find_root) -> None:
    found = fs.find(find_root, pattern="*.json", recursive=True)
    assert found == [
        str(find_root / "a.json"),
        str(find_root / "sub1" / "c.json"),
        str(find_root / "sub1" / "sub1_deep" / "d.json"),
        str(find_root / "sub2" / "e.json"),
    ]


def test_find_with_recursive_false_matches_only_the_search_path_itself(
    find_root,
) -> None:
    found = fs.find(find_root, pattern="d.json", recursive=False)
    assert found == []


def test_find_without_criteria_returns_every_entry_one_level_below_including_directories(
    find_root,
) -> None:
    found = fs.find(find_root, search_sub_dirs=True)
    assert found == [
        str(find_root / "sub1" / "c.json"),
        str(find_root / "sub1" / "sub1_deep"),
        str(find_root / "sub2" / "e.json"),
    ]


def test_find_with_replace_root_returns_paths_prefixed_with_root_instead_of_raising(
    find_root,
) -> None:
    found = fs.find(find_root, search_sub_dirs=False, replace_root=True)
    assert found == [
        "root/a.json",
        "root/b.txt",
        "root/sub1",
        "root/sub2",
    ]


def test_find_with_replace_root_and_a_trailing_slash_keeps_the_separator(
    find_root,
) -> None:
    found = fs.find(f"{find_root}/", search_sub_dirs=False, replace_root=True)
    assert found == [
        "root/a.json",
        "root/b.txt",
        "root/sub1",
        "root/sub2",
    ]


def test_find_with_two_criteria_raises_value_error_naming_both_arguments(
    find_root,
) -> None:
    with pytest.raises(ValueError, match="equals"):
        fs.find(find_root, equals="a.json", contains="c")


def test_find_returns_results_in_sorted_order(find_root) -> None:
    found = fs.find(find_root, search_sub_dirs=False)
    assert found == sorted(found)


def test_find_with_a_gcs_search_path_queries_the_gcs_filesystem_with_an_unmangled_gs_url(
    fake_gcs, gcs_bucket
) -> None:
    fs.find(f"gs://{gcs_bucket.name}/repo", pattern="*.json")
    assert fake_gcs.calls == [("glob", f"gs://{gcs_bucket.name}/repo/*/*.json")]


def test_find_with_a_gcs_search_path_returns_the_matching_entries_of_the_bucket(
    fake_gcs, gcs_bucket
) -> None:
    found = fs.find(f"gs://{gcs_bucket.name}/repo", pattern="*.json")
    assert [Path(p).name for p in found] == ["c.json", "e.json"]


def test_find_with_a_gcs_search_path_and_recursive_true_matches_every_depth(
    fake_gcs, gcs_bucket
) -> None:
    found = fs.find(f"gs://{gcs_bucket.name}/repo", pattern="*.json", recursive=True)
    assert [Path(p).name for p in found] == ["a.json", "c.json", "d.json", "e.json"]


def test_find_with_a_gcs_search_path_returns_subdirectories_like_the_local_branch(
    fake_gcs, gcs_bucket
) -> None:
    found = fs.find(f"gs://{gcs_bucket.name}/repo", search_sub_dirs=True)
    assert sorted(Path(p).name for p in found) == ["c.json", "e.json", "sub1_deep"]


def test_find_with_a_gcs_search_path_and_replace_root_rewrites_the_stripped_root(
    fake_gcs, gcs_bucket
) -> None:
    found = fs.find(
        f"gs://{gcs_bucket.name}/repo",
        search_sub_dirs=False,
        replace_root=True,
    )
    assert [Path(p).as_posix() for p in found] == [
        "root/a.json",
        "root/b.txt",
        "root/sub1",
        "root/sub2",
    ]


def test_find_with_a_local_search_path_never_queries_the_gcs_filesystem(
    find_root, fake_gcs
) -> None:
    found = fs.find(find_root, search_sub_dirs=False)
    assert fake_gcs.calls == []
    assert found == [
        str(find_root / "a.json"),
        str(find_root / "b.txt"),
        str(find_root / "sub1"),
        str(find_root / "sub2"),
    ]
