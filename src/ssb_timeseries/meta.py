"""Placeholder utility covering meta data functionality used by timeseries.

Ideally, this functionality should live elsewhere, in ssb-python-klass and other meta data libraries. Likely subject to refactoring later.
"""

import io

import bigtree
import bigtree.node
import bigtree.tree
import pandas as pd
from klass import get_classification
from typing_extensions import Self

from ssb_timeseries import fs
from ssb_timeseries import properties
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.types import PathStr

# mypy: disable-error-code="assignment, override, type-arg, attr-defined, no-untyped-def, import-untyped"


class Taxonomy:
    """Wraps taxonomies defined in KLASS or json files in a object structure.

    Attributes:
        definition (str): Descriptions of the taxonomy.
            name:
            structure_type:     enum:   list | tree | graph
            levels: number of levels not counting the root node
        entities (pd.Dataframe): Entity definitions, represented as a dataframe with columns:
            code: str
            A unique entity identifier within the taxonomy.
            It may very well consist of numeric values, but will be represented as a string.

            parent:  str
            "parentCode"
            The code for the parent entity.

            name: str
            A unique human readable name. Not nullable.

            short:
            "shortName"
            A short version / mnemonic for name, if applicable.

            presentationName
            A "self explanatory" unique name, if applicable.

            validFrom

            validTo

    Notes:
        structure:
            Relations between entities of the taxonomy.
            Both lists and trees will be represented as hierarchies; with the root node being the taxonomy.
            Level two will be the first item level, so a flat list will have two levels.
            Hierarchies with a natural top or "root" node should have a single node at level two.

        lookups:
            Listing of supported names for all entities, mapping different categories of names of different standards and in different languages to a unique identifier.
    """

    def __init__(
        self,
        id_or_path: int | PathStr | str,
        root_name: str = "Taxonomy",
        sep: str = ".",
        substitute: dict | None = None,
    ) -> None:
        """Create Taxonomy object from KLASS id or file name.

        Key attributs: .entities holds the list of values and .structure puts the entitiees in a tree.
        """
        self.definition = {"name": root_name}
        if isinstance(id_or_path, int):
            # TO DO: handle versions of KLASS
            klass = get_classification(str(id_or_path)).get_codes().data
            self.entities = add_root_node(
                klass, {"code": "0", "parentCode": None, "name": root_name}
            )
            if substitute:
                for key, value in substitute.items():
                    self.entities["code"] = self.entities["code"].str.replace(
                        key, value
                    )
                    self.entities["parentCode"] = self.entities[
                        "parentCode"
                    ].str.replace(key, value)
        elif isinstance(id_or_path, str):  # isinstance(id_or_path, PathStr) or
            # TO DO: read from file:
            df_from_file = pd.DataFrame.from_dict(fs.read_json(id_or_path))
            self.entities = df_from_file

        self.structure = bigtree.dataframe_to_tree_by_relation(
            data=self.entities,
            child_col="code",
            parent_col="parentCode",
            attribute_cols=[
                "name",
                "shortName",
                "presentationName",
                "validFrom",
                "validTo",
                "notes",
            ],
        )

    def __eq__(self, other: Self) -> bool:
        """Checks for equality. Taxonomies are considered equal if their codes and hierarchical relations are the same."""
        tree_diff = bigtree.get_tree_diff(self.structure, other.structure)
        if tree_diff:
            trees_equal = False
        else:
            trees_equal = True

        fields_to_compare = ["code", "parentCode", "name"]
        s_entities = self.entities[fields_to_compare].reset_index(drop=True)
        o_entities = other.entities[fields_to_compare].reset_index(drop=True)

        ts_logger.debug(
            f"comparing:\n{s_entities.to_string()}\n...and:\n{o_entities.to_string()}"
        )
        ts_logger.debug(
            f".info:\n{_df_info_as_string(s_entities)}\n...and:\n{_df_info_as_string(o_entities)}"
        )
        entities_equal = all(s_entities == o_entities)

        return trees_equal and entities_equal

    def __minus__(self, other: Self) -> bigtree.tree:  # type: ignore
        """Return the tree difference between the two taxonomy (tree) structures."""
        return bigtree.get_tree_diff(self.structure, other.structure)

    def __getitem__(self, key: str) -> bigtree.node:  # type: ignore
        """Get tree node by name (KLASS code)."""
        return bigtree.find_name(self.structure.root, key)

    def subtree(self, key: str) -> bigtree.tree:  # type: ignore
        """Get subtree of node identified by name (KLASS code)."""
        the_node = bigtree.find_name(self.structure, key)
        return bigtree.get_subtree(the_node)

    def print_tree(self, *args, **kwargs) -> str:  # noqa: ANN002, ANN003
        """Return a string with the tree structure.

        Implementation is ugly! It would be preferable not to print the tree to std out.
        ... but this works.
        """
        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            bigtree.print_tree(self.structure, *args, **kwargs)
            output = buf.getvalue()
        return output

    def all_nodes(self) -> list[bigtree.node]:  # type: ignore
        """Return all nodes in the taxonomy."""
        return [n for n in self.structure.root.descendants]

    def leaf_nodes(self) -> list[bigtree.node]:  # type: ignore
        """Return all leaf nodes in the taxonomy."""
        return [n for n in self.structure.root.leaves]

    def parent_nodes(self) -> list[bigtree.node]:  # type: ignore
        """Return all non-leaf nodes in the taxonomy."""
        return [
            n
            for n in self.structure.root.descendants
            if n not in self.structure.root.leaves
        ]

    def save(self, path: PathStr) -> None:
        """Save taxonomy to json file.

        The file can be read using Taxonomy(<path to file>).
        """
        # TODO: make this work with timeseries.fs
        self.entities.to_json(path_or_buf=path)


def add_root_node(df: pd.DataFrame, root_node: dict[str, str | None]) -> pd.DataFrame:
    """Prepend root node row to taxonomy dataframe."""
    new_row = {c: None for c in df.columns}
    for k in root_node:
        new_row[k] = root_node[k]
    df.rename(columns={"name": "fullName"})
    df["parentCode"] = df["parentCode"].fillna(value=root_node["code"])
    root_df = pd.DataFrame(root_node, index=[0])
    df = pd.concat([root_df, df], ignore_index=True)
    df.sort_index(inplace=True)
    return df


# A different apporach for tagging sets and series was considered.
# DatasetTags and SeriesTags classes are currently not used, but kept as the decision may be revisited.


class DatasetTags:
    """Dataset Tags: key value pairs with dataset metadata. Methods for manipulating tags."""

    def __init__(
        self,
        name: str,
        versioning: properties.Versioning,
        temporality: properties.Temporality,
        **kwargs: str | list[str],
    ) -> None:
        """Initialise Dataset Tags."""
        self.name: str = name
        self.versioning: properties.Versioning = versioning
        self.temporality: properties.Temporality = temporality
        self.series: list[SeriesTags] = []
        self.series_tags: dict = kwargs.get(
            "series_tags", ""
        )  # Series(dataset=self.name, versioning=self.versioning

        series_in_set = kwargs.get("series", [])

        # TO DO: for series in set, add SeriesTags
        ts_logger.debug(series_in_set)

    def tag_set(
        self,
        tags: dict | None = None,
        operation: str = "add",
        **kwargs: str | list[str],
    ) -> None:
        """Tag the dataset.

        Tags may be specified as kwargs or as a tags dict.
        Operation (optional: 'add' (default) | 'remove') specifies the action to perform.
        """
        pass
        # if value not in self[attribute]:
        #    self[attribute].append(value)

    def tag_series(
        self,
        identifiers: str | list[str] = "*",
        tags: dict | None = None,
        operation: str = "add",
        **kwargs: str | list[str],
    ) -> None:
        """Tag (identified) series in the dataset.

        Series may be identified by series names or index, a list of series names or list of series indexes, or a dict of tags.
        Tags may be specified as kwargs or as a tags dict.
        Operation (optional: 'add' (default) | 'replace' | 'remove') specifies the action to perform on the identified series.
        """
        pass
        # should handle different datatypes for "item" :
        # name, int index, List of name or index
        # if value not in self.series[item][attribute]:
        #    self.series[attribute].append(value)

    def __eq__(self, other: Self) -> bool:
        """Check if dataset tags are the same."""
        return (self.name, self.versioning, self.temporality, self.tags) == (
            other.name,
            other.versioning,
            other.temporality,
            other.tags,
        )

    def __repr__(self) -> str:
        """Return initialization for a copy of the dataset tag object: DatasetTags(name={self.name}, versioning={self.versioning}, temporality={self.temporality}, tags={self.tags})."""
        return f"DatasetTags(name={self.name}, versioning={self.versioning}, temporality={self.temporality}, tags={self.tags})"


class SeriesTags:
    """Series Tags: key value pairs with series metadata. Methods for manipulating tags."""

    def __init__(
        self,
        dataset: str,
        name: str,
        index: int | None = None,
        tags: dict | None = None,
        lineage: dict | None = None,
        stats: dict | None = None,
    ) -> None:
        """Initialise Series Tags.

        Should inherit from Dataset_Tags.
        """
        self.dataset = dataset
        self.name = name
        self.fullname = f"{self.dataset}_{self.name}"
        self.index = index
        self.tags = tags
        self.lineage = lineage

    """
    def to_str(self, attributes: list(str) = None, separator: str = "_") -> list[str]:
        # [{'A': "a", 'B': "b"}, {'A': "aa", 'B': "bb"},{'A': "aaa", 'B': "bbb"}]
        # ->   ["a_b", "aa_bb", "aaa_bbb"]
        if attributes:
            result = []

        # for tag_dict in self:
        #    joined_values = "_".join([",".join(tag_dict[attr]) for attr in attributes])
        #    result.append(joined_values)

        return result
    """

    def __repr__(self) -> str:
        """Return initialization for a copy of the series tag object: SeriesTags(name={self.name}, versioning={self.versioning}, temporality={self.temporality}, tags={self.tags})."""
        return f"SeriesTags(name={self.name}, versioning={self.versioning}, temporality={self.temporality}, tags={self.tags})"


def _df_info_as_string(df: pd.DataFrame) -> str:
    """Returns the content of df.info() as a string."""
    with io.StringIO() as buffer:
        df.info(buf=buffer)
        return buffer.getvalue()
