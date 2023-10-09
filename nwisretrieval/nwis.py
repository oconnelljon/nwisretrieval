from __future__ import annotations
import pandas as pd
import requests
from dataclass_wizard import fromdict
from nwisretrieval.schema_nwis_ts import NWISjson


def get_requests_data(service: str, params: dict) -> str:
    base_urls = {
        "iv": "https://nwis.waterservices.usgs.gov/nwis/iv/",
        "dv": "https://nwis.waterservices.usgs.gov/nwis/dv/",
    }
    return requests.get(url=base_urls[service], params=params).json()


class NWISFrame:
    def __init__(self, data, meta):
        self.ts = data
        self.meta = meta

    def __repr__(self):
        return f"URL: {self.url}\n{self.ts}"

    @property
    def query_parameters(self):
        url = self.url
        params = url.split("?")[0].split("&")
        split_params = [key_val.split("=") for key_val in params]
        return {key_val[0]: key_val[1] for key_val in split_params}

    @property
    def url(self) -> str:
        return self.meta.value.query_info.query_url

    @property
    def site_name(self) -> str:
        return self.meta.value.time_series[0].source_info.site_name

    @staticmethod
    def process_nwis_response(
        jdata: str,
        record_path: list | None = None,
        datetime_col: str = "dateTime",
    ) -> pd.DataFrame:
        """Process JSON data from NWIS to pd.DataFrame
        Defaults are set for NWIS queries.

        Parameters
        ----------
        jdata : str
            JSON data string from requests Response from NWIS url.
        record_path : list | None, optional
            List of json keys to traverse to normalize values to DataFrame.
            By default ["value", "timeSeries", "values", "value"]
        datetime_col : str, optional
            Column containing datetime values to convert to DateTimeIndex.
            By default "dateTime"

        Returns
        -------
        pd.DataFrame
            Columns: "value", "qualifiers"
            Index: "dateTime" - DateTimeIndex
        """
        if record_path is None:
            record_path = [
                "value",
                "timeSeries",
                "values",
                "value",
            ]

        meta = fromdict(cls=NWISjson, d=jdata)

        dataframe = pd.json_normalize(
            jdata,
            record_path=record_path,
        )

        dataframe[datetime_col] = pd.to_datetime(
            dataframe[datetime_col].array,
            infer_datetime_format=True,
        )

        dataframe.set_index(
            datetime_col,
            inplace=True,
        )

        dataframe = dataframe.tz_localize(None)
        dataframe["value"] = pd.to_numeric(dataframe["value"])
        return dataframe, meta

    @classmethod
    def get_nwis(
        cls,
        service: str,
        **kwargs,
    ) -> pd.DataFrame:
        """Get time series data from NWIS DV or IV service

        Parameters
        ----------
        Common kwargs:
            format = json
            parameterCd = 00060 # discharge
            startDT
            endDT
            sites
            siteStatus
            access

        Returns
        -------
        pd.DataFrame
            Time-series data in the form of a pandas DataFrame
        """

        json_data = get_requests_data(service=service, params=kwargs)
        dataframe, meta = cls.process_nwis_response(json_data)

        return NWISFrame(dataframe, meta)


if __name__ == "__main__":
    data = NWISFrame.get_nwis(
        format="json",
        sites="12323233",
        startDT="2022-07-01",
        endDT="2022-08-01",
        statCd="00003",
        parameterCd="00060",
        service="dv",
    )
    data.query_parameters
    data.site_name
    pause = 2
