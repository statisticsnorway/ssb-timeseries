# import json
# import uuid
# from enum import Enum

from timeseries import properties

# from timeseries import dataset as ds # --> circular?
from timeseries import logging as log


class DatasetTags:
    def __init__(
        self,
        name: str,
        versioning: properties.SeriesVersioning,
        temporality: properties.SeriesTemporality,
        **kwargs,
    ) -> None:
        self.name: str = name
        self.versioning: properties.SeriesVersioning = versioning
        self.temporality: properties.SeriesTemporality = temporality
        self.series: list[SeriesTags] = []
        self.series_tags: dict = kwargs.get(
            "series_tags", ""
        )  # Series(dataset=self.name, versioning=self.versioning

        series_in_set = kwargs.get("series", [])

        # TO DO: for series in set, add SeriesTags
        log.debug(series_in_set)

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
