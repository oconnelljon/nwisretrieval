from __future__ import annotations
import pandas as pd
import numpy as np
import requests
from requests import Response


@pd.api.extensions.register_dataframe_accessor("nwismeta")
class NWISMeta:
    def __init__(self, pandas_obj) -> None:
        self._obj = pandas_obj
        self._metadict: dict = {}


@pd.api.extensions.register_dataframe_accessor("nwisframe")
class NWISFrame:
    """Custom pandas DataFrame accessor.  Extends pd.DataFrame class with NWIS time-series specific properties and methods.
    @property decorator allows for methods to be called as if they were normal instance variables.  This functionality means
    methods can calculate properties based on available instance data.
    """

    _metadata = ["_metadict"]

    def __init__(self, pandas_obj) -> None:
        self._validate(pandas_obj)
        self._obj: pd.DataFrame = pandas_obj
        self._metadict = {}

    @staticmethod
    def _validate(obj) -> None:
        # When nwisframe is instantiated, check for minimum qualifications to use properties and methods.
        if set(obj.columns) != {"value", "qualifiers"}:
            raise AttributeError("Must have 'value' and 'qualifiers' as columns")
        if not isinstance(obj.index, pd.DatetimeIndex):
            raise AttributeError("Index must be of type pd.DateTimeIndex")

    @property
    def STAID(self):
        return self._obj.nwismeta._metadict.get("STAID")

    @property
    def start_date(self):
        return self._obj.nwismeta._metadict.get("start_date")

    @property
    def end_date(self):
        return self._obj.nwismeta._metadict.get("end_date")

    @property
    def coords(self):
        return self._obj.nwismeta._metadict.get("coords")

    @property
    def qualifier(self) -> str:
        unique_quals = list(pd.unique(self._obj["qualifiers"].apply(frozenset)))
        for qual in unique_quals:
            if "Ice" in qual or "i" in qual:
                return "Ice"
        return "None"

    def check_gaps(
        self,
        interval: str,
    ) -> None:
        idx = pd.date_range(self._obj.nwismeta._metadict["start_date"], self._obj.nwismeta._metadict["start_date"], freq=interval)
        if idx.difference(self._obj.index).empty:
            self.gap_flag = False
            return None

        print(f"Gaps detected at: {self.STAID}")
        self.gap_flag = True
        return None

    def fill_gaps(
        self,
        interval: str | None = None,
    ) -> None:
        if interval is None:
            interval = self._obj.nwismeta._metadict["gap_tol"]
        self._obj = self._obj.asfreq(freq=interval)
        return None

    def _resolve_gaptolerance(self, gap_tol) -> str:
        """
        If no gap_tol, fall back on self.gap_tolerance property.

        If gap_tol is None and self.gap_tolerance is None, return None

        If neither gap_tol or self.gap_tolerance, return "unknown"
        """
        return gap_tol


def query_url(
    url: str,
) -> Response:
    """Qurey NWIS url

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
        If status code is not 200, some error has occured and no data was returned, exit the program.
    """
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Critical error!  No data found at: {url}\n Reason: {response.reason}")
        raise SystemExit
    return response


def get_nwis(
    STAID: str,
    start_date: str,
    end_date: str,
    param: str,
    stat_code: str = "00003",
    # stat_code: str = "32400",
    service: str = "iv",
    access: int = 0,
    gap_tol: str | None = None,
    gap_fill: bool = False,
    resolve_masking: bool = False,
) -> NWISFrame:
    """Retreives NWIS time-series data as a dataframe with extended methods and metadata properties.

    Parameters
    ----------
    STAID : str
        Station description
    start_date : str
        Start date
    end_date : str
        End date
    param : str
        Parameter
    stat_code : str, optional
        statistical code, by default "00003"
    service : str, optional
        Service, e.g. "iv" or "dv", by default "iv"
    access : int, optional
        Access level.  0 - Public, 1 - Coop, 2 - Internal, by default 0
    gap_tol : str | None, optional
        gap tolerance of time-series, "15min" = 15 minute gap tolerance, "D" = 24hr tolerance, by default None
    gap_fill : bool, optional
        Set True to fill any gaps in time-series with np.NaN, by default False
    resolve_masking : bool, optional
        Data with qualifiers such as "Ice" will mask data values with -999999 when access level is public.
        Set True and -999999 will be converted to np.NaN values, by default False

    Returns
    -------
    NWISFrame
        Acts just like a pandas DataFrame, but comes with extended methods and properties.

    Notes
    -----
    Refactor this function.  It handles too much.
    """

    url = build_url(STAID, start_date, end_date, param, stat_code, service, access)
    response = query_url(url)
    rdata = response.json()
    dataframe = process_nwis_response(url, response, rdata)
    # Custom NWISFrame class inherits from pd.DataFrame.
    # dataframe = NWISFrame(dataframe)
    dataframe.nwisframe._metadict.update(
        {
            "_STAID": STAID,
            "_start_date": start_date,
            "_end_date": end_date,
            "_param": param,
            "_stat_code": stat_code,
            "_service": service,
            "_access_level": access,
            "_url": url,
            "_gap_tolerance": gap_tol,
            "_gap_fill": gap_fill,
            "_resolve_masking": resolve_masking,
            "_site_name": rdata["value"]["timeSeries"][0]["sourceInfo"]["siteName"],
            "_coords": (
                rdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["latitude"],
                rdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["longitude"],
            ),
            "_var_description": rdata["value"]["timeSeries"][0]["variable"]["variableDescription"],
        }
    )

    return dataframe


def process_nwis_response(
    url: str,
    response: Response,
    rdata: dict,
) -> pd.DataFrame:
    """Process JSON data from NWIS to pd.DataFrame

    Parameters
    ----------
    url : str
        Query url
    response : Response
        response object
    rdata : dict
        JSON data from NWIS url

    Returns
    -------
    pd.DataFrame
        DateTimeIndex, values, approval/qualifiers

    Raises
    ------
    SystemExit
        If DataFrame returned from NWIS is empty, exit the program.
    """
    dataframe = pd.json_normalize(rdata, ["value", "timeSeries", "values", "value"])
    if dataframe.empty is True:
        print(f"Critical error!  Response status code: {response.status_code}\n No data found at: {url}")
        raise SystemExit
    dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"].array, infer_datetime_format=True)
    dataframe.set_index("dateTime", inplace=True)
    dataframe = dataframe.tz_localize(None)
    dataframe["value"] = pd.to_numeric(dataframe["value"])
    return dataframe


def build_url(STAID, start_date, end_date, param, stat_code, service, access):
    service_urls = {
        "dv": f"https://nwis.waterservices.usgs.gov/nwis/dv/?format=json&sites={STAID}&startDT={start_date}&endDT={end_date}&statCd={stat_code}&parameterCd={param}&siteStatus=all&access={access}",
        "iv": f"https://nwis.waterservices.usgs.gov/nwis/iv/?format=json&sites={STAID}&parameterCd={param}&startDT={start_date}&endDT={end_date}&siteStatus=all&access={access}",
    }
    return service_urls[service]


@pd.api.extensions.register_dataframe_accessor("geo")
class GeoAccessor:
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        # verify there is a column latitude and a column longitude
        if "latitude" not in obj.columns or "longitude" not in obj.columns:
            raise AttributeError("Must have 'latitude' and 'longitude'.")

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return getattr(self, attr)
        return getattr(self._df, attr)

    def __getitem__(self, item):
        return self._df[item]

    def __setitem__(self, item, data):
        self._df[item] = data

    # @property
    def center(self):
        # return the geographic center point of this DataFrame
        lat = self._obj.latitude
        lon = self._obj.longitude
        return (float(lon.mean()), float(lat.mean()))

    def plot(self):
        # plot this array's data on a map, e.g., using Cartopy
        pass


ds = pd.DataFrame({"longitude": np.linspace(0, 10), "latitude": np.linspace(0, 20)})
ds.geo.center()


if __name__ == "__main__":
    # Just some test data down here.
    gap_data = get_nwis(
        STAID="12301933",
        start_date="2023-01-03",
        end_date="2023-01-04",
        param="63680",
        gap_tol="15min",
    )

    # data = get_nwis(
    #     STAID="12301250",
    #     start_date="2023-01-02",
    #     end_date="2023-01-05",
    #     param="00060",
    #     service="dv",
    #     gap_tol="D",
    # )
    # data.gap_index()
    gap_data.nwisframe.fill_gaps("15min")
    pause = 2
