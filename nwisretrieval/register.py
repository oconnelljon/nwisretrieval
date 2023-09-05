from __future__ import annotations

import numpy as np
import pandas as pd
import requests
import sentinel
from requests.models import Response


@pd.api.extensions.register_dataframe_accessor("_NWISHelper")
class _NWISHelper:
    @classmethod
    def filter_parameters(cls, **kwargs):
        return {
            param: value
            for param, value in kwargs.items()
            if param in NWISFrame._valid_query_parameters
        }

    @classmethod
    def process_nwis_response(
        cls,
        response: requests.Response,
        record_path: list | None = None,
        datetime_col: str = "dateTime",
        value_col: str = "value",
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
        value_col : str, optional
            Column containing retrieved data values to coerce to a numeric dtype
            By default "value"

        Returns
        -------
        pd.DataFrame
            Columns: values, approval/qualifiers
            Index: DateTimeIndex

        Raises
        ------
        SystemExit
            If DataFrame returned from NWIS is empty, exit the program.
            Why continue if there's no data?
        """
        rdata = response.json()

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
        return dataframe


@pd.api.extensions.register_dataframe_accessor("NWISFrame")
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
    _metadata = ["_metadict", "STAID"]

    @staticmethod
    def get_nwis(*args, **kwargs) -> pd.DataFrame:
        service = kwargs.pop("service")
        url = NWISFrame._base_urls[service]
        kwargs = pd.DataFrame._NWISHelper.filter_parameters(**kwargs)
        response = requests.get(url=url, params=kwargs)
        dataframe = pd.DataFrame._NWISHelper.process_nwis_response(response)
        return dataframe


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

    def __init__(self, data, response: Response):
        self.ts = data
        self.response = response
        self._metadict = {}

    @staticmethod
    def process_nwis_response(
        response: requests.Response,
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
            Columns: values, approval/qualifiers
            Index: DateTimeIndex

        Raises
        ------
        SystemExit
            If DataFrame returned from NWIS is empty, exit the program.
            Why continue if there's no data?
        """
        rdata = response.json()

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
    def filter_parameters(**kwargs):
        return {
            param: value
            for param, value in kwargs.items()
            if param in NWISFrame._valid_query_parameters
        }

    @classmethod
    def get_nwis(cls, **kwargs) -> pd.DataFrame:
        service = kwargs.pop("service")
        url = cls._base_urls[service]
        kwargs = cls.filter_parameters(**kwargs)
        response = requests.get(url=url, params=kwargs)
        dataframe = cls.process_nwis_response(response)
        return NWISFrame(dataframe, response)


if __name__ == "__main__":
    data = NWISFrame.get_nwis(
        format="json",
        sites="12323233",
        startDT="2022-07-01",
        endDT="2022-08-01",
        statCd="00003",
        parameterCd="00060",
        service="dv",
        edward="a cat",
    )
    pause = 2
elevation_df = pd.DataFrame(
    {
        "elevation": [1000, 2000, 3000, 4000, 5000, 10000],
        "coeff_a": [1, 2, 3, 4, 5, 10],
        "coeff_b": [10, 20, 30, 40, 50, 100],
        "coeff_c": [100, 200, 300, 400, 500, 1000],
    },
)

disharge_df = pd.DataFrame(
    {
        "elevation": [500, 2000, 3001, 3999, 4001, 4002, 5001, 7002],
    },
)

df3 = pd.merge_asof(disharge_df, elevation_df, on="elevation", direction="forward")

def check_quals(self):
    mask = ~self._obj["qualifiers"].isnull()
    unique_quals = list(pd.unique(self._obj["qualifiers"][mask].apply(frozenset)))
    for qual in unique_quals:
        if "Ice" in qual or "i" in qual:
            return "Ice"
    return Unknown


def query_url(
    url: str,
) -> Response:
    """Qurey NWIS url with requests package.

    Parameters
    ----------
    url : str
        NWIS url pointing to JSON data

    Returns
    -------
    Response
        requests Response object

    Raises
    ------
    SystemExit
        If status code is not 200, some error has occured and no data was
        returned, exit the program.
    """
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Critical error!  No data found at: {url}\n Reason: {response.reason}")
        raise SystemExit
    return response


def create_metadict(
    rdata: dict | None = None,
    **kwargs,
) -> dict:
    """Creates metadictionary for NWISFrame.

    Parameters
    ----------
    rdata : dict | None, optional
        JSON response data from NWIS, by default None

    Returns
    -------
    dict
        Dictionary of metadata for NWISFrame.

    Notes
    -----
    Valid kwargs:
        ""STAID"
        "start_date"
        "end_date"
        "param"
        "stat_code"
        "service"
        "access"
        "url"
        "gap_tol"
        "gap_fill"
        "resolve_masking"
        "_approval"
    """
    # Find all the valid kwargs, if a kwarg is not passed, return Unknown sentinetl
    metadict = dict(
        {
            "_staid": kwargs.get("staid", Unknown),
            "_start_date": kwargs.get("start_date", Unknown),
            "_end_date": kwargs.get("end_date", Unknown),
            "_param": kwargs.get("param", Unknown),
            "_stat_code": kwargs.get("stat_code", Unknown),
            "_service": kwargs.get("service", Unknown),
            "_access_level": kwargs.get("access", Unknown),
            "_url": kwargs.get("url", Unknown),
            "_gap_tolerance": kwargs.get("gap_tol", Unknown),
            "_gap_fill": kwargs.get("gap_fill", Unknown),
            "_resolve_masking": kwargs.get("resolve_masking", Unknown),
            "_approval": kwargs.get("_approval", Unknown),
        }
    )
    if rdata:
        metadict.update(
            {
                "_site_name": rdata["value"]["timeSeries"][0]["sourceInfo"]["siteName"]
                or Unknown,
                "_coords": (
                    rdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"][
                        "geogLocation"
                    ]["latitude"]
                    or Unknown,
                    rdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"][
                        "geogLocation"
                    ]["longitude"]
                    or Unknown,
                ),
                "_var_description": rdata["value"]["timeSeries"][0]["variable"][
                    "variableDescription"
                ]
                or Unknown,
            }
        )
    return metadict


def build_url(
    STAID: str | int,
    start_date: str,
    end_date: str,
    param: str,
    stat_code: str,
    service: str,
    access: str | int,
) -> str:
    """Generate URL to retrieve NWIS data from.

    Parameters
    ----------
    STAID : str | int
        NWIS station ID
    start_date : str
        Start date of data pull range.
    end_date : str
        End date of data pull range.
    param : str
        Parameter code, e.g. 00060
    stat_code : str
        Statistical code
    service : str
        "iv": instantanious data services, "dv": daily value service
    access : str | int
        NWIS access level.  0 - Public, 1 - Coop, 2 - Internal USGS

    Returns
    -------
    str
        URL of data to query from NWIS.
    """
    service_urls = {
        "dv": f"https://nwis.waterservices.usgs.gov/nwis/dv/?format=json&sites={STAID}&startDT={start_date}&endDT={end_date}&statCd={stat_code}&parameterCd={param}&siteStatus=all&access={access}",
        "iv": f"https://nwis.waterservices.usgs.gov/nwis/iv/?format=json&sites={STAID}&parameterCd={param}&startDT={start_date}&endDT={end_date}&siteStatus=all&access={access}",
    }
    return service_urls[service]


def get_nwis(
    **kwargs,
) -> pd.DataFrame:
    """Retreives NWIS time-series data as a dataframe with
    extended methods and metadata properties.

    Parameters
    ----------
    service : str, optional
        Service, e.g. "iv" or "dv", by default "iv"
    access : int | str, optional
        Access level.  0 - Public, 1 - Coop, 2 - Internal, by default 0
    gap_tol : str | None, optional
        gap tolerance of time-series,
        "15min" = 15 minute gap tolerance,
        "D" = 24hr tolerance, by default None
    gap_fill : bool, optional
        Set True to fill any gaps in time-series with np.NaN, by default False
    resolve_masking : bool, optional
        Data with qualifiers such as "Ice" will mask data values
        with -999999 when access level is public.
        Set True and -999999 will be converted to np.NaN values, by default False

    Returns
    -------
    NWISFrame
        Acts just like a pandas DataFrame, but comes with
        extended methods and properties.

    Notes
    -----
    Refactor this function.  It handles too much.
    """

    response = query_url(url)
    response = requests.get(url, data=kwargs)
    rdata = response.json()
    dataframe = process_nwis_response(rdata)
    if dataframe.empty is True:
        print(
            f"Critical error!  Response status code: {response.status_code}\n No data found at: {url}"
        )

    return dataframe


# if __name__ == "__main__":
#     # Just some test data down here.
#     gap_data = get_nwis(
#         format="json",
#         sites="12323233",
#         startDT="2022-07-01",
#         endDT="2022-08-01",
#         statCd="00003",
#         parameterCd="00060",
#         access="1",
#     )

#     # print(gap_data.nwis.staid)
#     # gap_data.nwis.check_gaps(
#     #     "15min",
#     #     start_date="2023-01-03 16:45:00",
#     #     end_date="2023-01-03 17:00:00",
#     # )
#     # gap_data.nwis.gaps
#     pause = 2
