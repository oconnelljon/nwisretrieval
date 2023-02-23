from __future__ import annotations
import warnings
import numpy as np
import pandas as pd
import requests
import sentinel
from requests.models import Response


class NWISFrame(pd.DataFrame):
    """Inherits from pandas DataFrame to extend properties and methods specific to NWIS time-series data.

    Parameters
    ----------
    pd.DataFrame : pd.DataFrame
        pd.DataFrame to inherit from.

    Returns
    -------
    NWISFrame
        Extended pd.DataFrame.  Acts like a normal DataFrame, but with NWIS specific properties and methods.

    Notes
    -----
        Generally you don't want to inherit from pd.DataFrame.  Pandas offers an API for extending
    DataFrame but there are complications with propogating user defined properties.
    When an operation on the DataFrame returns a new DataFrame, user defined properties
    do not propogate.  Inheriting from DataFrame allows for the use of the
    internal "private" DataFrame property "_metadata" which contains a list of user defined variables
    that will be propogated during DataFrame operations when a new DataFrame is returned.

    In the case of this custom class, _metadata contains one user defined variable "_metadict"
    which is a dictionary populated with metadata from the get_nwis function.
    The properties query the _metadict dictionary of the DataFrame to return metadata values such as
    the URL, start date, variable description, etc.

    For more information on extending pandas:
        https://pandas.pydata.org/docs/dev/development/extending.html#

    Geopandas makes use of _metadata:
        https://github.com/geopandas/geopandas/blob/main/geopandas/geodataframe.py

    @properties cannot be set by assignment e.g. STAID = "12301933" or self.STAID = "12301933"
    However, they are accessed like a class variable e.g. my_dataframe.STAID returns "12301933"

    Because "None" can be viewed as a valid return to some class properties, a custom sentinel object
    from the sentinel library is used in this class to provide the same characteristics as "None"
    but will instead have a name of "Unknown"
    """

    # Register custom dataframe property NAMES in _metadata list.
    # Named properties in this list will propgate after dataframe manipulations e.g. slicing, asfreq, deepcopy(), etc.
    _metadata = ["_metadict"]

    # Custom sentinel object, works like "None"
    Unknown = sentinel.create("Unknown")

    def __init__(self, data: pd.DataFrame, *args, **kwargs):
        if kwargs.get("copy") is None and isinstance(data, pd.DataFrame):
            kwargs.update(copy=True)
        super().__init__(data, *args, **kwargs)
        self._metadict = create_metadict()

    # Override of pd.DataFrame _constructor property.
    # Allows for the retention of subclasses through data manipulations.
    @property
    def _constructor(self):
        return NWISFrame

    @property
    def STAID(self):
        return self._metadict.get("_STAID")

    @property
    def start_date(self):
        return str(self._metadict.get("_start_date"))

    @property
    def end_date(self):
        return str(self._metadict.get("_end_date"))

    @property
    def param(self):
        return self._metadict.get("_param")

    @property
    def stat_code(self):
        return self._metadict.get("_stat_code")

    @property
    def service(self):
        return self._metadict.get("_service")

    @property
    def url(self):
        return self._metadict.get("_url")

    @property
    def site_name(self):
        return self._metadict.get("_site_name")

    @property
    def coords(self):
        return self._metadict.get("_coords")

    @property
    def var_description(self):
        return self._metadict.get("_var_description")

    @property
    def gap_tolerance(self):
        return str(self._metadict.get("_gap_tolerance"))

    @property
    def gaps(self):
        return self.gap_index(self.gap_tolerance)

    @property
    def approval(self):
        return self.check_approval()

    @property
    def qualifier(self):
        return self.check_quals()

    def check_quals(self):
        """Checks if qualifiers are applied to data.

        Parameters
        ----------
        None

        Returns
        -------
        str
            Returns "Ice" if ANY record has an ice qualifier.

        Notes
        -----
        Currently only checking for Ice qualifiers.  May need to add more for equipment malfunctions, etc.
        """
        mask = ~self["qualifiers"].isnull()
        unique_quals = list(pd.unique(self["qualifiers"][mask].apply(frozenset)))
        for qual in unique_quals:
            if "Ice" in qual or "i" in qual:
                return "Ice"
        return self.Unknown

    def check_gaps(
        self,
        gap_tol: str | None = None,
    ) -> bool | object:
        """Checks for gaps in time-series data.
        Parameters
        ----------
        gap_tol : str | None, optional
            Gap tolerance, by default None
            If no gap tolerance is specified, fall back on self.gap_tolerance property

        Returns
        -------
        bool | None
            True if gaps are present in the index
            False if no gaps are found in the index
            If neither gap_tol or self.gap_tolerance property are specified, returns sentinel object: NWISFrame.unknown
        """
        gap_tol = self._resolve_gaptolerance(gap_tol)
        if gap_tol == NWISFrame.Unknown:
            warnings.warn(f"Warning: No gap tolerance specified for {self.STAID}.")
            return NWISFrame.Unknown
        if self.gap_index(gap_tol).empty:
            return False
        warnings.warn(f"Gaps detected at: {self.STAID} with a tolerance of {gap_tol}")
        return True

    def gap_index(
        self,
        gap_tol: str | None = None,
    ) -> pd.DatetimeIndex:
        gap_tol = self._resolve_gaptolerance(gap_tol)
        idx = pd.date_range(self.start_date, self.end_date, freq=gap_tol)
        return idx.difference(self.index)

    def fill_gaps(
        self,
        gap_tol: str | None = None,
    ) -> NWISFrame:
        gap_tol = self._resolve_gaptolerance(gap_tol)
        try:
            self = self.asfreq(freq=gap_tol)
            self._metadict["_gap_tolerance"] = gap_tol
            # self["qualifiers"][self["qualifiers"].isnull()] = NWISFrame.Unknown
            # self["qualifier_set"] = self["qualifiers"].map(set)
            return self
        except ValueError:
            warnings.warn(f"Warning: No gap tolerance specified for {self.STAID}.")
            return self

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

    def resolve_masks(self) -> None:
        """Convert any values from -999999 to NaN values.
        Set True to resolve NWIS masking of Ice qualified data to NaN values.
        NWIS serves public data with an Ice qualifier as -999999 masked values.
        Setting access to 1 "coop" or 2 "internal" returns actual values.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        self["value"] = self["value"].where(self["value"] != -999999.0, np.NaN)
        return None

    def _resolve_gaptolerance(
        self,
        gap_tol: str | None,
    ) -> str | object:
        """
        If no gap_tol, fall back on self.gap_tolerance property.

        If gap_tol is None and self.gap_tolerance is None, return None

        If neither gap_tol or self.gap_tolerance, return "unknown"
        """
        if gap_tol is None:
            return self.gap_tolerance if self.gap_tolerance is not None else NWISFrame.Unknown
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


def create_metadict(
    rdata: dict | None = None,
    **kwargs,
) -> dict:
    metadict = dict(
        {
            "_STAID": kwargs.get("STAID", NWISFrame.Unknown),
            "_start_date": kwargs.get("start_date", NWISFrame.Unknown),
            "_end_date": kwargs.get("end_date", NWISFrame.Unknown),
            "_param": kwargs.get("param", NWISFrame.Unknown),
            "_stat_code": kwargs.get("stat_code", NWISFrame.Unknown),
            "_service": kwargs.get("service", NWISFrame.Unknown),
            "_access_level": kwargs.get("access", NWISFrame.Unknown),
            "_url": kwargs.get("url", NWISFrame.Unknown),
            "_gap_tolerance": kwargs.get("gap_tol", NWISFrame.Unknown),
            "_gap_fill": kwargs.get("gap_fill", NWISFrame.Unknown),
            "_resolve_masking": kwargs.get("resolve_masking", NWISFrame.Unknown),
            "_approval": kwargs.get("_approval", NWISFrame.Unknown),
        }
    )
    if rdata:
        metadict.update(
            {
                "_site_name": rdata["value"]["timeSeries"][0]["sourceInfo"]["siteName"] or NWISFrame.Unknown,
                "_coords": (
                    rdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["latitude"] or NWISFrame.Unknown,
                    rdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["longitude"] or NWISFrame.Unknown,
                ),
                "_var_description": rdata["value"]["timeSeries"][0]["variable"]["variableDescription"] or NWISFrame.Unknown,
            }
        )
    return metadict


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


def build_url(
    STAID,
    start_date,
    end_date,
    param,
    stat_code,
    service,
    access,
) -> str:
    service_urls = {
        "dv": f"https://nwis.waterservices.usgs.gov/nwis/dv/?format=json&sites={STAID}&startDT={start_date}&endDT={end_date}&statCd={stat_code}&parameterCd={param}&siteStatus=all&access={access}",
        "iv": f"https://nwis.waterservices.usgs.gov/nwis/iv/?format=json&sites={STAID}&parameterCd={param}&startDT={start_date}&endDT={end_date}&siteStatus=all&access={access}",
    }
    return service_urls[service]


def get_nwis(
    STAID: str,
    start_date: str,
    end_date: str,
    param: str,
    stat_code: str = "00003",
    # stat_code: str =
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
        statistical code, by default daily values "00003"
        Use "32400" for midnight values
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

    url = build_url(
        STAID=STAID,
        start_date=start_date,
        end_date=end_date,
        param=param,
        stat_code=stat_code,
        service=service,
        access=access,
    )
    response = query_url(url)
    rdata = response.json()
    dataframe = process_nwis_response(url, response, rdata)

    dataframe = NWISFrame(dataframe)
    dataframe._metadict = create_metadict(
        dataframe=dataframe,
        STAID=STAID,
        start_date=start_date,
        end_date=end_date,
        param=param,
        stat_code=stat_code,
        service=service,
        access=access,
        gap_tol=gap_tol,
        gap_fill=gap_fill,
        resolve_masking=resolve_masking,
        url=url,
        rdata=rdata,
    )

    if gap_fill and bool(gap_tol):
        dataframe = dataframe.fill_gaps(gap_tol)
    if resolve_masking:
        dataframe.resolve_masks()
    dataframe.check_quals()
    dataframe.check_approval()
    dataframe.check_gaps()
    return dataframe


def getNWISmeteoDataMULTI(
    STAID: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Pull air temp, RH, net solar, and wind speed data down from NWIS based on site, start date end date.

    Parameters
    ----------
    STAID : str
        NWIS site ID
    start_date : str
        Start of the data pull 'YYYY-MM-DD'
    end_date : str
        End date of the data pull 'YYYY-MM-DD'

    Returns
    -------
    pd.DateFrame

    Notes
    -----
    Met Station at Milk R Eastern Crossing Intl Bndry, post 2019-10-02.
    Previous data housed at 06135000

    Some common meteorology measurement parameter values:
            - 00020 - air temperature - degC
            - 00052 - Relative Humidity - percent
            - 62609 - Net solar radiation - W/m^2
            - 62625 - Wind speed - m/s

    """

    print(f"S - Loading NWIS air temp, relative humidity, net solar radiation, and wind speed at {STAID}")
    url = f"https://waterdata.usgs.gov/nwis/dv?cb_00020=on&cb_00052=on&cb_62609=on&cb_62625=on&format=rdb&site_no={STAID}&referred_module=sw&period=&begin_date={start_date}&end_date={end_date}"
    print(url)
    dataframe = pd.read_csv(url, sep="\t", comment="#")
    dataframe.drop(labels=[0], inplace=True, axis=0)
    dataframe.drop(
        labels=[
            "site_no",
            "agency_cd",
            "297970_00052_00003_cd",
            "297971_62609_00003_cd",
            "297973_00020_00003_cd",
            "297974_62625_00003_cd",
        ],
        inplace=True,
        axis=1,
    )

    dataframe["datetime"] = pd.to_datetime(dataframe["datetime"])
    dataframe = dataframe.set_index("datetime")
    dataframe = dataframe.rename(columns={"297971_62609_00003": "Rnet", "297970_00052_00003": "Rh", "297973_00020_00003": "T", "297974_62625_00003": "U"})
    dataframe["Rnet"] = pd.to_numeric(dataframe["Rnet"])
    dataframe["Rh"] = pd.to_numeric(dataframe["Rh"])
    dataframe["T"] = pd.to_numeric(dataframe["T"])
    dataframe["U"] = pd.to_numeric(dataframe["U"])
    idx = pd.date_range(start_date, end_date)
    dataframe = dataframe.reindex(idx, fill_value=np.NaN)
    return dataframe


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
    gap_data.fill_gaps("15min")
    pause = 2
