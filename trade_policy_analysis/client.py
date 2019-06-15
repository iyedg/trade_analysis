import attr
import requests
import json
import pandas as pd
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
        endpoint = f"/attr/{self.classification.lower()}/"
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

    @vcr.use_cassette("fixtures/calls.yaml", record_mode="new_episodes")
    def call(self):
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
        return pd.DataFrame(json.loads(response.content)["data"])
