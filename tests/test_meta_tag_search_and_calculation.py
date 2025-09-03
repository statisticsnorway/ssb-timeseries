# mypy: ignore-errors = True

import itertools
import logging
import uuid

import numpy as np
import pytest

import ssb_timeseries as ts
from ssb_timeseries import sample_metadata
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dataset import is_df_like
from ssb_timeseries.dates import date_utc
from ssb_timeseries.meta import Taxonomy
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# ---mypy: disable-error-code="attr-defined,no-untyped-def,union-attr,index,call-overload"


def test_find_data_using_single_metadata_attribute(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Select series in set by series tag: {'A': 'a'}."""
    caplog.set_level(logging.DEBUG)

    set_name = f"test-datetimecols-{uuid.uuid4().hex}"
    set_tags = {
        "About": "ImportantThings",
        "Country": "Norway",
    }
    series_tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["z"]}
    tag_values: list[list[str]] = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        tags=set_tags,
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        attributes=["A", "B", "C"],
    )

    x_selected_on_attribute_a = x.select(tags={"A": "a"})
    expected_matches = ["a_p_z", "a_q_z", "a_r_z"]

    ts.logger.debug(
        f"x_selected_on_attribute_a: \n\t{x_selected_on_attribute_a.series}\n vs expected:\n\t{expected_matches}"
    )
    assert isinstance(x_selected_on_attribute_a, Dataset)
    assert sorted(x_selected_on_attribute_a.numeric_columns) == sorted(expected_matches)

    returned_series_tags = x_selected_on_attribute_a.tags["series"]
    for key in returned_series_tags.keys():
        assert returned_series_tags[key]["dataset"] != set_name
        assert returned_series_tags[key]["name"] == key
        assert returned_series_tags[key]["A"] == "a"


def test_find_data_using_multiple_metadata_attributes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Select series in set by series tags: {'A': 'a', 'B': 'q'}.

    Returned series should satisfy {'A': 'a'} AND {'B': 'q'}
    """
    caplog.set_level(logging.DEBUG)

    set_name = f"test-datetimecols-{uuid.uuid4().hex}"
    set_tags = {
        "About": "ImportantThings",
        "Country": "Norway",
    }
    series_tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["z"]}
    tag_values: list[list[str]] = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        tags=set_tags,
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        attributes=["A", "B", "C"],
    )

    x_selected_on_attribute_a_and_b = x.select(tags={"A": "a", "B": "q"})
    expected_matches = ["a_q_z"]

    ts.logger.debug(
        f"x_selected_on_attribute_a: \n\t{x_selected_on_attribute_a_and_b.series}\n vs expected:\n\t{expected_matches}"
    )
    assert isinstance(x_selected_on_attribute_a_and_b, Dataset)
    assert sorted(x_selected_on_attribute_a_and_b.numeric_columns) == sorted(
        expected_matches
    )

    returned_series_tags = x_selected_on_attribute_a_and_b.tags["series"]
    for key in returned_series_tags.keys():
        assert returned_series_tags[key]["dataset"] != set_name
        assert returned_series_tags[key]["name"] == key
        assert returned_series_tags[key]["A"] == "a"
        assert returned_series_tags[key]["B"] == "q"


def test_find_data_using_metadata_criteria_with_single_attribute_and_multiple_values(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Select series in set by series tags: {'A': ['a', 'b']}.

    Returned series should satisfy {'A': 'a'} OR {'A': 'b'}
    """
    caplog.set_level(logging.DEBUG)

    set_name = f"test-datetimecols-{uuid.uuid4().hex}"
    set_tags = {
        "About": "ImportantThings",
        "Country": "Norway",
    }
    series_tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["z"]}
    tag_values: list[list[str]] = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        tags=set_tags,
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        attributes=["A", "B", "C"],
    )

    x_selected_on_attribute_a = x.select(tags={"A": ["a", "b"]})
    expected_matches = ["a_p_z", "a_q_z", "a_r_z", "b_p_z", "b_q_z", "b_r_z"]

    ts.logger.debug(
        f"x_selected_on_attribute_a: \n\t{x_selected_on_attribute_a.series}\n vs expected:\n\t{expected_matches}"
    )
    assert isinstance(x_selected_on_attribute_a, Dataset)
    assert sorted(x_selected_on_attribute_a.numeric_columns) == sorted(expected_matches)
    assert (
        x_selected_on_attribute_a.name != set_name
    )  # .select() returns a new set and changes the name

    returned_series_tags = x_selected_on_attribute_a.tags["series"]
    for key in returned_series_tags.keys():
        assert (
            returned_series_tags[key]["dataset"] != set_name
        )  # new set name propagates to the series
        ts.logger.warning("debug: %s¨", returned_series_tags[key])
        assert returned_series_tags[key]["name"] == key
        assert (
            returned_series_tags[key]["A"] == "a"
            or returned_series_tags[key]["A"] == "b"
        )


def test_aggregate_sum_for_flat_list_taxonomy(
    conftest,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass48 = Taxonomy(klass_id=48)
    klass48_leaves = [f"{n.name:s}" for n in klass48.structure.root.leaves]

    set_name = conftest.function_name()
    set_tags = {
        "Country": "Norway",
    }
    series_tags = {"A": klass48_leaves, "B": ["q"], "C": ["z"]}
    tag_values: list[list[str]] = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        tags=set_tags,
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        attributes=["A", "B", "C"],
    )

    assert len(x.numeric_columns) == len(klass48_leaves)
    ts.logger.debug(f"{set_name}:\n{x.data}")

    y = x.aggregate(attributes=["A"], taxonomies=[klass48], functions=["sum"])
    assert isinstance(y, Dataset)
    assert len(y.numeric_columns) == len(klass48.parent_nodes())
    assert sorted(y.numeric_columns) == sorted(
        [f"sum({n.name})" for n in klass48.parent_nodes()]
    )
    assert not np.isnan(y.numeric_array()).any()
    assert (y.numeric_array() > 0).all()


def test_aggregate_multiple_functions_for_flat_list_taxonomy(
    conftest,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass48 = Taxonomy(klass_id=48)
    klass48_leaves = [f"{n.name:s}" for n in klass48.structure.root.leaves]

    set_name = conftest.function_name()
    set_tags = {
        "Country": "Norway",
    }
    series_tags = {"A": klass48_leaves, "B": ["q"], "C": ["z"]}
    tag_values: list[list[str]] = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        tags=set_tags,
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        attributes=["A", "B", "C"],
    )

    assert len(x.numeric_columns) == len(klass48_leaves)

    def custom_func_perc10(x):
        return x.quantile(0.1, axis=1, numeric_only=True)

    aggregate_functions = [custom_func_perc10, "median", ["quantile", 0.9, "nearest"]]
    ts.logger.debug(f"{set_name}:\n{x.data}")
    y = x.aggregate(
        attributes=["A"], taxonomies=[klass48], functions=aggregate_functions
    )
    assert isinstance(y, Dataset)
    assert len(y.numeric_columns) == len(klass48.parent_nodes()) * len(
        aggregate_functions
    )
    assert sorted(y.numeric_columns) == sorted(
        ["custom_func_perc10(0)", "median(0)", "quantile0.9(0)"]
    )
    assert not np.isnan(y.numeric_array()).any()
    assert (y.numeric_array() > 0).all()


def test_aggregate_sums_for_hierarchical_taxonomy(
    conftest,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(klass_id=157)
    klass157_leaves = [n.name for n in klass157.structure.root.leaves]

    set_name = conftest.function_name()
    set_tags = {
        "Country": "Norway",
    }
    series_tags = {"A": klass157_leaves, "B": ["q"], "C": ["z"]}
    tag_values: list[list[str]] = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        tags=set_tags,
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        attributes=["A", "B", "C"],
    )

    assert len(x.numeric_columns) == len(klass157_leaves)

    y = x.aggregate(["A"], [klass157], {"sum"})
    assert isinstance(y, Dataset)
    assert len(y.numeric_columns) == len(klass157.parent_nodes())
    assert sorted(y.numeric_columns) == sorted(
        [f"sum({n.name})" for n in klass157.parent_nodes()]
    )
    assert not np.isnan(y.numeric_array()).any()
    assert (y.numeric_array() > 0).all()


def test_aggregate_mean_for_hierarchical_taxonomy(
    conftest,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(klass_id=157)
    klass157_leaves = [n.name for n in klass157.structure.root.leaves]

    set_name = conftest.function_name()
    set_tags = {
        "Country": "Norway",
    }
    series_tags = {"A": klass157_leaves, "B": ["pq"], "C": ["xyz"]}
    tag_values: list[list[str]] = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=create_df(
            *tag_values, start_date="2020-01-01", end_date="2024-01-03", freq="YS"
        ),
        tags=set_tags,
        attributes=["A", "B", "C"],
    )

    assert len(x.numeric_columns) == len(klass157_leaves)

    y = x.aggregate(["A"], [klass157], {"mean"})
    assert isinstance(y, Dataset)
    assert len(y.numeric_columns) == len(klass157.parent_nodes())
    assert sorted(y.numeric_columns) == sorted(
        [f"mean({n.name})" for n in klass157.parent_nodes()]
    )
    assert not np.isnan(y.numeric_array()).any()
    assert (y.numeric_array() > 0).all()


def test_aggregate_multiple_methods_for_hierarchical_taxonomy(
    conftest,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(klass_id=157)
    klass157_leaves = [n.name for n in klass157.structure.root.leaves]

    set_name = conftest.function_name()
    set_tags = {
        "Country": "Norway",
    }
    series_tags = {"A": klass157_leaves, "B": ["pq"], "C": ["xyz"]}
    tag_values = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        tags=set_tags,
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        attributes=["A", "B", "C"],
    )

    assert len(x.numeric_columns) == len(klass157_leaves)
    multiple_functions = ["count", "sum", "mean"]
    y = x.aggregate(
        attributes=["A"],
        taxonomies=[klass157],
        functions=multiple_functions,
    )
    assert isinstance(y, Dataset)
    assert len(y.numeric_columns) == len(
        klass157.parent_nodes() * len(multiple_functions)
    )
    for func in multiple_functions:
        assert sorted(y.select(pattern=func).numeric_columns) == sorted(
            [f"{func}({n.name})" for n in klass157.parent_nodes()]
        )

    assert (y["mean(12.3)"] == y["sum(12.3)"] / y["count(12.3)"]).all()

    assert not np.isnan(y.numeric_array()).any()
    assert (y.numeric_array() > 0).all()


def test_aggregate_multiple_methods_for_multiple_hierarchical_taxonomies(
    conftest,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # caplog.set_level(logging.DEBUG)

    balance = sample_metadata.balance()
    balance_leaves = [n.name for n in balance.structure.root.leaves]

    commodity = sample_metadata.commodity()
    commodity_leaves = [n.name for n in commodity.structure.root.leaves]

    geography = sample_metadata.nordic_countries()
    geography_leaves = [n.name for n in geography.structure.root.leaves]

    set_name = conftest.function_name()
    series_tags = {
        "bal": balance_leaves,
        "com": commodity_leaves,
        "geo": geography_leaves,
    }
    tag_values = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        attributes=["bal", "com", "geo"],
    )

    assert len(x.numeric_columns) == len(balance_leaves) * len(commodity_leaves) * len(
        geography_leaves
    )

    multiple_functions = ["count", "sum", "mean"]
    y = x.aggregate(
        attributes=["bal", "com", "geo"],
        taxonomies=[balance, commodity, geography],
        functions=multiple_functions,
    )
    assert isinstance(y, Dataset)
    logging.debug(f"{y.series=}")

    bal_aggregates = [b.name for b in balance.parent_nodes()]
    com_aggregates = [c.name for c in commodity.parent_nodes()]
    geo_aggregates = [b.name for b in geography.parent_nodes()]
    attribute_permutations = [
        c for c in itertools.product(bal_aggregates, com_aggregates, geo_aggregates)
    ]

    ts.logger.debug(f"{attribute_permutations}")
    attribute_permutation_names = ["_".join(p[:]) for p in attribute_permutations]
    assert len(y.numeric_columns) == len(attribute_permutations) * len(
        multiple_functions
    )

    for func in multiple_functions:
        z = y.select(pattern=func)
        print(z)
        assert sorted(z.numeric_columns) == sorted(
            [f"{func}({n})" for n in attribute_permutation_names]
        )

    assert not np.isnan(y.numeric_array()).any()
    assert (y.numeric_array() > 0).all()

    yy_mean = y["mean(S_petro_scan)"]
    yy_sum = y["sum(S_petro_scan)"]
    yy_count = y["count(S_petro_scan)"]
    yy_calc = yy_sum / yy_count
    assert isinstance(yy_mean, Dataset) and is_df_like(yy_mean.data)
    assert isinstance(yy_calc, Dataset) and is_df_like(yy_calc.data)
    print(yy_mean.nw)
    print(yy_calc.nw)
    # assert (yy_mean == yy_calc).all()
    assert yy_calc.isclose(yy_mean)  # we observe decimal differences


def test_aggregate_percentiles_by_strings_for_hierarchical_taxonomy(
    conftest,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(klass_id=157)
    klass157_leaves = [n.name for n in klass157.structure.root.leaves]

    set_name = conftest.function_name()
    series_tags = {"A": klass157_leaves, "B": ["pq"], "C": ["xyz"]}
    tag_values: list[list[str]] = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=create_df(
            *tag_values, start_date="2020-01-01", end_date="2024-01-03", freq="YS"
        ),
        attributes=["A", "B", "C"],
    )

    assert len(x.numeric_columns) == len(klass157_leaves)
    multiple_functions = [
        ["quantile", 10],
        ["percentile", 50, "nearest"],
        ["quantile", 90],
    ]

    y = x.aggregate(["A"], [klass157], multiple_functions)
    assert isinstance(y, Dataset)
    assert len(y.numeric_columns) == 3 * len(klass157.parent_nodes())
    expected_names = [
        f"{f[0]}{f[1]}({n.name})"
        for n in klass157.parent_nodes()
        for f in multiple_functions
    ]
    assert sorted(y.numeric_columns) == sorted(expected_names)
    assert not np.isnan(y.numeric_array()).any()
    assert (y.numeric_array() > 0).all()


def test_aggregate_callable_for_hierarchical_taxonomy(
    conftest,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(klass_id=157)
    klass157_leaves = [n.name for n in klass157.structure.root.leaves]

    set_name = conftest.function_name()
    set_tags = {
        "Country": "Norway",
    }
    series_tags = {"A": klass157_leaves, "B": ["pq"], "C": ["xyz"]}
    tag_values: list[list[str]] = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=create_df(
            *tag_values, start_date="2020-01-01", end_date="2024-01-03", freq="YS"
        ),
        tags=set_tags,
        attributes=["A", "B", "C"],
    )

    assert len(x.numeric_columns) == len(klass157_leaves)

    def perc10(x):
        return x.quantile(0.1, axis=1, numeric_only=True)

    def perc90(x):
        return x.quantile(0.9, axis=1, numeric_only=True)

    y = x.aggregate(["A"], [klass157], [perc10, perc90])
    assert isinstance(y, Dataset)
    assert len(y.numeric_columns) == 2 * len(klass157.parent_nodes())
    expected_names = [
        f"{f.__name__}({n.name})"
        for n in klass157.parent_nodes()
        for f in [perc10, perc90]
    ]
    assert sorted(y.numeric_columns) == sorted(expected_names)
    assert not np.isnan(y.numeric_array()).any()
    assert (y.numeric_array() > 0).all()
