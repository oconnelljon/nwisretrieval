from __future__ import annotations
import pandas as pd
import requests
import sentinel
from dataclass_wizard import fromdict
from nwisretrieval.schema_nwis_ts import NWISjson


def get_requests_data(url, params):
    return requests.get(url=url, params=params).json()


class NWISFrame:
    _Unknown = sentinel.create("Unknown")
    _base_urls = {
        "iv": "https://nwis.waterservices.usgs.gov/nwis/iv/",
        "dv": "https://nwis.waterservices.usgs.gov/nwis/dv/",
    }
    _valid_query_parameters = (
        "format",
        "parameterCd",
        "startDT",
        "endDT",
        "sites",
        "siteStatus",
        "access",
    )

    def __init__(self, data, meta):
        self.ts = data
        self.meta = meta

    @property
    def query_parameters(self):
        url = self.url
        params = url.split("?")[1].split("&")
        split_params = [key_val.split("=") for key_val in params]
        return {key_val[0]: key_val[1] for key_val in split_params}

    @property
    def url(self):
        return self.meta.value.query_info.query_url

    @property
    def site_name(self):
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
        response : requests.Response
            requests Response object from NWIS url.
        record_path : list | None, optional
            List of json keys to traverse to normalize values to DataFrame.
            By default ["value", "timeSeries", "values", "value"]
        datetime_col : str, optional
            Column containing datetime values to convert to DateTimeIndex.
            By default "dateTime"

        Returns
        -------
        pd.DataFrame
            Columns: "values" - approval/qualifiers
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

    @staticmethod
    def item_generator(data, key):
        if isinstance(data, dict):
            for k, v in data.items():
                if k == key:
                    yield v
                else:
                    yield from NWISFrame.item_generator(v, key)
        elif isinstance(data, list):
            for item in data:
                yield from NWISFrame.item_generator(item, key)

    @classmethod
    def get_nwis(
        cls,
        service: str,
        # format: str | None = None,
        # sites: str | None = None,
        # startDT: str | None = None,
        # endDT: str | None = None,
        # parameterCd: str | None = None,
        # siteStatus: str | None = None,
        # access: str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Get time series data from NWIS DV or IV service

        Parameters
        ----------
        Common kwargs:
            format = json
            parameterCd = 00060 # discharge
            startDT =
            endDT =
            sites =
            siteStatus =
            access =

        Returns
        -------
        pd.DataFrame
            Time-series data in the form of a pandas DataFrame
        """
        url = cls._base_urls[service]

        json_data = get_requests_data(url=url, params=kwargs)
        dataframe, meta = cls.process_nwis_response(json_data)

        return NWISFrame(dataframe, meta)

    @classmethod
    def _merge_kwargs(
        cls, format, sites, startDT, endDT, parameterCd, siteStatus, access, kwargs: dict
    ):
        defaults = {
            "format": format,
            "sites": sites,
            "startDT": startDT,
            "endDT": endDT,
            "parameterCd": parameterCd,
            "siteStatus": siteStatus,
            "access": access,
        }
        kwargs = kwargs.update(defaults)
        kwargs = cls._remove_nones(**kwargs)
        return kwargs

    def check_approval(self) -> str:
        """Checks approval level of data.

        Parameters
        ----------
        None

        Returns
        -------
        str
            Return "Provisional" or "Approved".
            If ANY records are provisional, set to "Provisional" level.

        Notes
        -----
        """
        mask = ~self["qualifiers"].isnull()
        unique_quals = list(pd.unique(self["qualifiers"][mask].apply(frozenset)))
        approval_level = next(
            ("Provisional" for approval in unique_quals if "P" in approval),
            "Approved",
        )
        self._metadict["_approval"] = approval_level
        return approval_level


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
    # data.query_parameters
    # data.site_name
    pause = 2
