"""_summary_

Returns:
    _type_: _description_
"""

import bigtree
import pandas as pd

# import json
# import uuid
# from enum import Enum

from ssb_timeseries import properties

# from ssb_timeseries import dataset as ds # --> circular?
from ssb_timeseries.logging import ts_logger
from ssb_timeseries import fs

# from klass import search_classification
from klass import get_classification


class Taxonomy:
    """Wraps taxonomies defined in KLASS or json files in a object structure.

    Properties:
        definition:
            Descriptions of the taxonomy:
                name:
                structure_type:     enum:   list | tree | graph
                levels: number of levels not counting the root node
        entities:
            entity definitions, represented as a dataframe with columns:
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
                notes
        structure:
            Relations between entities of the taxonomy.
            Both lists and trees will be represented as hierarchies; with the root node being the taxonomy.
            Level two will be the first item level, so a flat list will have two levels.
            Hierarchies with a natural top or "root" node should have a single node at level two.
        lookups:
            Complete listing of supported names for all entities, mapping different categories of names of different standards and in different languages to a unique identifier.
    Methods:

    """

    def __init__(
        self,
        id_or_path,
        root_name="Taxonomy",
        sep=".",
        substitute: dict = None,
    ):
        self.definition = {"name": root_name}
        if isinstance(id_or_path, int):
            # TO DO: handle versions of KLASS
            klass = get_classification(id_or_path).get_codes().data
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
        elif isinstance(id_or_path, str):
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

    def __eq__(self, other) -> bool:
        # this neglects everything but the codes and their hierarchical relations
        tree_diff = bigtree.get_tree_diff(self.structure, other.structure)
        if tree_diff:
            trees_equal = False
        else:
            trees_equal = True

        fields_to_compare = ["code", "parentCode", "name"]
        s_entities = self.entities[fields_to_compare].reset_index(drop=True)
        o_entities = other.entities[fields_to_compare].reset_index(drop=True)

        ts_logger.debug(
            f"comparing:\n{s_entities.to_string()}\n...and:\n{s_entities.to_string()}"
        )
        ts_logger.debug(f".info:\n{s_entities.info()}\n...and:\n{s_entities.info()}")
        entities_equal = all(s_entities == o_entities)

        return trees_equal and entities_equal

    def print_tree(self, *args, **kwargs) -> str:
        # ugly! it would be preferable not to print the tree to std out
        # ... but this works
        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            bigtree.print_tree(self.structure, *args, **kwargs)
            output = buf.getvalue()
        return output

    def save(self, path) -> None:
        # TODO: this will no work with buckets
        self.entities.to_json(path_or_buf=path)


def add_root_node(df: pd.DataFrame, root_node: dict) -> pd.DataFrame:
    """Prepend root node row to taxonomy dataframe."""
    new_row = dict((c, None) for c in df.columns)
    for k in root_node.keys():
        new_row[k] = root_node[k]
    df.rename(columns={"name": "fullName"})
    df["parentCode"] = df["parentCode"].fillna(value=root_node["code"])
    df.loc[-1] = root_node
    df.index = df.index + 1
    df.sort_index(inplace=True)
    return df


class DatasetTags:
    def __init__(
        self,
        name: str,
        versioning: properties.Versioning,
        temporality: properties.Temporality,
        **kwargs,
    ) -> None:
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

    def tag_set(self, attribute: str, value: str):
        pass
        # if value not in self[attribute]:
        #    self[attribute].append(value)

    def tag_series(self, item, attribute: str, value: str):
        pass
        # should handle different datatypes for "item" :
        # name, int index, List of name or index
        # if value not in self.series[item][attribute]:
        #    self.series[attribute].append(value)

    def __eq__(self, other) -> bool:
        return (self.name, self.versioning, self.temporality, self.tags) == (
            other.name,
            other.versioning,
            other.temporality,
            other.tags,
        )

    def __repr__(self):
        return {
            "name": self.name,
            "versioning": self.versioning,
            "temporality": self.temporality,
            # "tags": self.tags,
            # "series": len(self.series),
        }


class SeriesTags:
    def __init__(
        self,
        dataset: str,
        name: str,
        index: int = None,
        tags: dict = {},
        lineage: dict = {},
        stats: dict = {},
    ) -> None:
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

    def __repr__(self):
        return str(
            {
                "name": self.name,
                "dataset": self.dataset,
                "tags": self.tags,
            }
        )
