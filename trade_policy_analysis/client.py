import attr
import requests
import json
import pandas as pd
import numpy as np
import vcr
from urllib.parse import urljoin


@attr.s
class OEC(object):
    _classification = attr.ib(default="hs07", converter=str.lower)
    _flow = attr.ib(default="import", converter=str.lower)
    _origin = attr.ib(default="all", converter=str.lower)
    _destination = attr.ib(default="all", converter=str.lower)
    _products = attr.ib(default="show", converter=str.lower)
    _year = attr.ib(default=2017, converter=int)
    # !year 0 should be considered as None
    _end_y = attr.ib(default=0, converter=int)
    # TODO: use Enums instead
    _depth = attr.ib(default=1, converter=int)
    base_uri = "https://atlas.media.mit.edu"

    @_classification.validator
    def __valid_classification(self, attribute, value):
        # ! Assumes str.lower converter
        classifications = ["sitc", "hs92", "hs96", "hs02", "hs07"]
        if value not in classifications:
            raise ValueError(f"{value} is not in {classifications}")

    @_flow.validator
    def __valid_flow(self, attribute, value):
        # ! Assumes str.lower converter
        valid_flows = ["import", "export"]
        if value not in valid_flows:
            raise ValueError(f"{value} is not in {valid_flows}")

    @property
    def flow(self):
        return self._flow

    @flow.setter
    def flow(self, value):
        value = value.lower()
        # TODO: "flow" does nothing, read more about it
        self.__valid_flow("flow", value)
        self._flow = value

    @property
    @vcr.use_cassette("fixtures/countries_attrs.yaml", record_mode="new_episodes")
    def countries_attrs(self):
        endpoint = "/attr/country/"
        r = requests.get(urljoin(self.base_uri, endpoint))
        return pd.DataFrame(json.loads(r.content)["data"])

    @property
    @vcr.use_cassette("fixtures/products_attrs.yaml", record_mode="new_episodes")
    def products_attrs(self):
        endpoint = f"/attr/{self._classification}/"
        r = requests.get(urljoin(self.base_uri, endpoint))
        return pd.DataFrame(json.loads(r.content)["data"])

    def imports(self):
        self.flow = "import"
        return self

    def exports(self):
        self.flow = "export"
        return self

    def products(self, value):
        self._products = value.lower()
        return self

    def origin(self, value):
        # value can be country id, show, or all
        self._origin = value.lower()
        return self

    def destination(self, value):
        self._destination = value.lower()
        return self

    def during(self, start_y, end_y=0):
        # year is start_y, optionally define range with end_y
        # TODO validation end_y must be larger than year
        self._year = start_y
        self._end_y = end_y
        return self

    def depth(self, value):
        self._depth = value
        return self

    def __merge_w_countries_attrs(self, df):
        if "dest_id" in df.columns and "origin_id" in df.columns:
            return df.merge(
                on="dest_id",
                right=self.countries_attrs.rename(
                    columns=lambda col_name: f"dest_{col_name}"
                ),
            ).merge(
                on="origin_id",
                right=self.countries_attrs.rename(
                    columns=lambda col_name: f"origin_{col_name}"
                ),
            )
        elif "dest_id" in df.columns:
            return df.merge(
                on="dest_id",
                right=self.countries_attrs.rename(
                    columns=lambda col_name: f"dest_{col_name}"
                ),
            )
        elif "origin_id" in df.columns:
            return df.merge(
                on="origin_id",
                right=self.countries_attrs.rename(
                    columns=lambda col_name: f"origin_{col_name}"
                ),
            )

    def __detailed_products_attrs(self, df):
        classification_id_col_name = f"{self._classification}_id"
        if classification_id_col_name in df.columns:
            # Extract sections
            # return (
            df.assign(
                section_id=df[classification_id_col_name].str[:2],
                chapter_id=df[classification_id_col_name].str[:4],
                grouping_id=df[classification_id_col_name].str[:6],
                product_id=df[classification_id_col_name].apply(
                    lambda val: val if len(val) == 8 else np.nan
                ),
            )
        return df

    def __merge_w_products_attrs(self, df):
        classification_id_col_name = f"{self._classification}_id"
        if classification_id_col_name in df.columns:
            # Extract sections
            return (
                self.__detailed_products_attrs(df)
                .merge(
                    on="section_id",
                    right=self.products_attrs[["id", "name"]].rename(
                        columns=lambda x: f"section_{x}"
                    ),
                )
                .merge(
                    left_on="chapter_id",
                    right=self.products_attrs[["display_id", "name"]].rename(
                        columns=lambda x: f"chapter_{x}"
                    ),
                    right_on="chapter_display_id",
                )
                .merge(
                    left_on="grouping_id",
                    right=self.products_attrs[["display_id", "name"]].rename(
                        columns=lambda x: f"grouping_{x}"
                    ),
                    right_on="grouping_display_id",
                )
            )
        return df

    @vcr.use_cassette("fixtures/calls.yaml", record_mode="new_episodes")
    def call(self, human=False):
        # At most one of `origin` `destination` and
        # `product` can have a value of "show"
        uri = (
            f"http://atlas.media.mit.edu/{self._classification}"
            f"/{self._flow}/{self._year}/{self._origin}"
            f"/{self._destination}/{self._products}/"
        )
        if self._end_y > 0:
            uri = (
                f"http://atlas.media.mit.edu/{self._classification}"
                f"/{self._flow}/{self._year}.{self._end_y}/{self._origin}"
                f"/{self._destination}/{self._products}/"
            )
        print(uri)
        response = requests.get(uri)
        # TODO: more elegant way to filter these
        ignore_cols = [
            "export_val_growth_pct_5",
            "export_val_growth_val",
            "import_val_growth_pct",
            "import_val_growth_pct_5",
            "dest_borders_land",
            "dest_borders_maritime",
            "dest_color",
            "dest_weight",
            "dest_palette",
            "dest_image",
            "dest_image_author",
            "dest_comtrade_name",
            "dest_id_num",
            "dest_image_link",
            "export_val_growth_pct",
            "dest_id_2char",
            "dest_icon",
            "import_val_growth_val",
            "export_val_growth_val_5",
            "dest_id",
            "dest_display_id",
            "import_val_growth_val_5",
            "origin_borders_land",
            "origin_borders_maritime",
            "origin_color",
            "origin_weight",
            "origin_palette",
            "origin_image",
            "origin_image_author",
            "origin_comtrade_name",
            "origin_id_num",
            "origin_image_link",
            "origin_icon",
            "origin_id",
            "origin_id_2char",
            "origin_display_id",
            f"{self._classification}_id_len",
        ]
        return (
            pd.DataFrame(json.loads(response.content)["data"])
            .pipe(self.__merge_w_countries_attrs)
            .pipe(self.__detailed_products_attrs)
            # .pipe(self.__merge_w_products_attrs)
            .pipe(lambda df: df[df.columns.difference(ignore_cols)] if human else df)
        )

    def __call__(self, *args, **kwargs):
        return self.call(*args, **kwargs)
