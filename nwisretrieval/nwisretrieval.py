import pandas as pd
import requests
import sentinel
from requests.models import Response


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

    def __init__(self, data, json_data):
        self.ts = data
        self.json_data = json_data
        self._metadict = {}


    @property
    def query_parameters(self):
        url = self.json_data.url
        params = url.split("?")[1].split("&")
        split_params = [key_val.split("=") for key_val in params]
        return {key_val[0]: key_val[1] for key_val in split_params}

    @property
    def url(self):
        return list(NWISFrame.item_generator(self.json_data, "queryURL"))[0]

    @property
    def status_code(self):
        return self.json_data.status_code

    @property
    def approval(self):
        return self.check_approval()

    # @property
    # def site_name(self):
    #     return self.response.value.timeSeries[0].sourceInfo.siteName

    # def create_metadict(rdata):
    #     metadict = dict(
    #         {
    #             "_STAID": kwargs.get("STAID", NWISFrame.Unknown),
    #             "_start_date": kwargs.get("start_date", NWISFrame.Unknown),
    #             "_end_date": kwargs.get("end_date", NWISFrame.Unknown),
    #             "_param": kwargs.get("param", NWISFrame.Unknown),
    #             "_stat_code": kwargs.get("stat_code", NWISFrame.Unknown),
    #             "_service": kwargs.get("service", NWISFrame.Unknown),
    #             "_access_level": kwargs.get("access", NWISFrame.Unknown),
    #             "_url": kwargs.get("url", NWISFrame.Unknown),
    #             "_gap_tolerance": kwargs.get("gap_tol", NWISFrame.Unknown),
    #             "_gap_fill": kwargs.get("gap_fill", NWISFrame.Unknown),
    #             "_resolve_masking": kwargs.get("resolve_masking", NWISFrame.Unknown),
    #             "_approval": kwargs.get("_approval", NWISFrame.Unknown),
    #         }
    #     )

    @staticmethod
    def process_nwis_response(
        rdata: str,
        record_path: list | None = None,
        # datetime_col: str = "dateTime",
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
        datetime_col : DEPRECATED - str, optional
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

        dataframe = pd.json_normalize(
            rdata,
            record_path=record_path,
        )
        dataframe["dateTime"] = pd.to_datetime(
            dataframe["dateTime"].array,
            infer_datetime_format=True,
        )
        dataframe.set_index(
            "dateTime",
            inplace=True,
        )
        dataframe = dataframe.tz_localize(None)
        dataframe["value"] = pd.to_numeric(dataframe["value"])
        return dataframe

    @staticmethod
    def item_generator(json_input, lookup_key):
        if isinstance(json_input, dict):
            for k, v in json_input.items():
                if k == lookup_key:
                    yield v
                else:
                    yield from item_generator(v, lookup_key)
        elif isinstance(json_input, list):
            for item in json_input:
                yield from item_generator(item, lookup_key)

    @classmethod
    def get_nwis(cls, **kwargs) -> pd.DataFrame:
        service = kwargs.pop("service")
        url = cls._base_urls[service]
        response = requests.get(url=url, params=kwargs)
        rdata = response.json()
        dataframe = cls.process_nwis_response(rdata)
        return NWISFrame(dataframe, rdata)

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
    data.url
    pause = 2
