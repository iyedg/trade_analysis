import attr
import requests
import json
import pandas as pd
import vcr
from urllib.parse import urljoin


@attr.s
class OEC(object):
    classification = attr.ib()
    _flow = attr.ib(init=False, default="import")
    base_uri = "https://atlas.media.mit.edu"

    @_flow.validator
    def __valid_flow(self, attribute, value):
        valid_flows = ["import", "export"]
        if value not in valid_flows:
            raise ValueError(f"{value} is not in {valid_flows}")

    @classification.validator
    def __valid_classification(self, attribute, value):
        classifications = ["SITC", "HS92", "HS96", "HS02", "HS07"]
        if value not in classifications:
            raise ValueError(f"{value} is not in {classifications}")

    @property
    def flow(self):
        return self._flow.lower()

    @flow.setter
    def flow(self, value):
        # TODO: "flow" does nothing, read more about it
        self.__valid_flow("flow", value)
        self._flow = value

    @property
    @vcr.use_cassette("fixtures/countries.yaml", record_mode="new_episodes")
    def countries(self):
        endpoint = "/attr/country/"
        r = requests.get(urljoin(self.base_uri, endpoint))
        return pd.DataFrame(json.loads(r.content)["data"])

    @property
    @vcr.use_cassette("fixtures/products.yaml", record_mode="new_episodes")
    def products(self):
        endpoint = f"/attr/{self.classification.lower()}/"
        r = requests.get(urljoin(self.base_uri, endpoint))
        return pd.DataFrame(json.loads(r.content)["data"])

    def get_country_comtrade_name(self, country_id):
        """Return COMTRADE name from 3 letter id

        Arguments:
            country_id {str} -- three letter id of country
        """
        return self.countries.pipe(
            lambda df: df.loc[df.display_id == country_id, "comtrade_name"]
        ).values[0]

    def get_country_image(self, country_id):
        # TODO: using values and list indexing is prone to error, better error handling
        link = self.countries.pipe(
            lambda df: df.loc[df.display_id == country_id, "image_link"]
        ).values[0]
        return link

    def get_product_icon(self, product_id):
        # TODO: using values and list indexing is prone to error, better error handling
        return urljoin(
            self.base_uri,
            self.products.pipe(lambda df: df.loc[df.id == product_id, "icon"]).values[
                0
            ],
        )

    def get_product_color(self, product_id):
        # TODO: using values and list indexing is prone to error, better error handling
        return self.products.pipe(
            lambda df: df.loc[df.id == product_id, "color"]
        ).values[0]

    @vcr.use_cassette("fixtures/exports.yaml", record_mode="new_episodes")
    def get_exports(
        self,
        origin="all",
        destination="all",
        product="all",
        depth="section",
        year=None,
        start_y=None,
        end_y=None,
    ):
        # TODO: lower case flow
        if year:
            uri = f"/{self.classification}/export/{year}/{origin}/{destination}/{product}/"
        else:
            uri = f"/{self.classification}/export/{start_y}.{end_y}/{origin}/{destination}/{product}/"
        print(urljoin(self.base_uri, uri).lower())
        r = requests.get(urljoin(self.base_uri, uri).lower())
        df = (
            pd.DataFrame(json.loads(r.content)["data"])
            .pipe(lambda df: df.loc[df["hs07_id_len"] == 8])
            .pipe(
                lambda df: df.drop(
                    columns=[
                        "export_val_growth_pct",
                        "export_val_growth_pct_5",
                        "export_val_growth_val",
                        "export_val_growth_val_5",
                        "import_val_growth_pct",
                        "import_val_growth_pct_5",
                        "import_val_growth_val",
                        "import_val_growth_val_5",
                        "hs07_id_len",
                    ]
                )
            )
            .pipe(
                lambda df: df.assign(
                    hs07_section=df["hs07_id"].str[:2],
                    hs07_chapter=df["hs07_id"].str[2:4],
                    hs07_heading=df["hs07_id"].str[4:6],
                    hs07_subheading=df["hs07_id"].str[6:8],
                )
            )
            .fillna(0)
        )
        return df
