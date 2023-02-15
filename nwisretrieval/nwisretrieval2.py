import pandas as pd
import numpy as np
import requests


def get_nwis(
    STAID: str,
    start_date: str,
    end_date: str,
    param: str,
    stat_code: str = "32400",
    service: str = "iv",
    access: int = 0,
    resolve_masking: bool = False,
    gap_tol: str | None = None,
    gap_fill: bool | None = None,
):
    service_urls = {
        "dv": f"https://nwis.waterservices.usgs.gov/nwis/dv/?format=json&sites={STAID}&startDT={start_date}&endDT={end_date}&statCd={stat_code}&parameterCd={param}&siteStatus=all&access={access}",
        "iv": f"https://nwis.waterservices.usgs.gov/nwis/iv/?format=json&sites={STAID}&parameterCd={param}&startDT={start_date}&endDT={end_date}&siteStatus=all&access={access}",
    }
    try:
        url = service_urls[service]
    except KeyError as e:
        raise SystemExit(f"Invalid service: {service}  Exiting program now.") from e

    response = requests.request("GET", url)
    rdata = response.json()
    dataframe = pd.json_normalize(rdata, ["value", "timeSeries", "values", "value"])
    if dataframe.empty is True:
        raise SystemExit(f"Critical error!  Response status code: {response.status_code}\n No data found at: {url}")

    dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"], infer_datetime_format=True)
    dataframe.set_index("dateTime", inplace=True)
    dataframe["value"] = pd.to_numeric(dataframe["value"])

    metadict = {
        "STAID": STAID,
        "start_date": start_date,
        "end_date": end_date,
        "param": param,
        "stat_code": stat_code,
        "service": service,
        "access": access,
        "URL": url,
        "gap_tol": gap_tol,
        "gap_fill": gap_fill,
        "resolve_masking": resolve_masking,
        "gap_flag": None,
        "site_name": rdata["value"]["timeSeries"][0]["sourceInfo"]["siteName"],
        "coords": (
            rdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["latitude"],
            rdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["longitude"],
        ),
        "var_description": rdata["value"]["timeSeries"][0]["variable"]["variableDescription"],
    }
    dataframe.nwismeta._metadict = metadict

    if gap_fill and bool(gap_tol):
        dataframe.nwisframe.fill_gaps(gap_tol)

    return dataframe


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

    def __init__(self, pandas_obj) -> None:
        self._validate(pandas_obj)
        self._obj: pd.DataFrame = pandas_obj

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
    def param(self):
        return self._obj.nwismeta._metadict.get("param")

    @property
    def stat_code(self):
        return self._obj.nwismeta._metadict.get("stat_code")

    @property
    def service(self):
        return self._obj.nwismeta._metadict.get("service")

    @property
    def url(self):
        return self._obj.nwismeta._metadict.get("url")

    @property
    def site_name(self):
        return self._obj.nwismeta._metadict.get("site_name")

    @property
    def coords(self):
        return self._obj.nwismeta._metadict.get("coords")

    @property
    def var_description(self):
        return self._obj.nwismeta._metadict.get("var_description")

    @property
    def gaps(self):
        return self._obj.nwismeta._metadict.get("gap_flag")

    @property
    def approval_level(self) -> str:
        """Checks approval level of data. If ANY records are provisional, set to "Provisional" level.

        Parameters
        ----------
        None

        Returns
        -------
        str
            Return "Provisional" or "Approved".

        Notes
        -----
        """
        unique_quals = list(pd.unique(self._obj["qualifiers"].apply(frozenset)))
        return next(
            ("Provisional" for approval in unique_quals if "P" in approval),
            "Approved",
        )

    @property
    def qualifier(self) -> str:
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
        unique_quals = list(pd.unique(self._obj["qualifiers"].apply(frozenset)))
        for qual in unique_quals:
            if "Ice" in qual or "i" in qual:
                return "Ice"
        return "None"

    def check_gaps(
        self,
        interval: str,
    ) -> None:
        """Void function.  Check time-series for gaps and update self.gap_flag as True/False

        Parameters
        ----------
        interval : str | None, optional
            Override for self.gap_tol set in __init__, by default None, if None, fallback on self.gap_tol, by default 15min

        Returns
        -------
        None
        """
        idx = pd.date_range(self._obj.nwismeta._metadict["start_date"], self._obj.nwismeta._metadict["start_date"], freq=interval)
        if idx.difference(self._obj.index).empty:
            self.gap_flag = False
            return None

        print(f"Gaps detected at: {self.STAID}")
        self.gap_flag = True
        return None

    # @staticmethod
    def fill_gaps(
        self,
        interval: str | None = None,
    ) -> None:
        """Void function.  Fill data gaps in instance time-series data.

        Parameters
        ----------
        interval : str | None, optional
            Override for self.gap_tol set in __init__, by default None, if None, fallback on self.gap_tol, by default 15min

        Returns
        -------
        None
        """
        if interval is None:
            interval = self._obj.nwismeta._metadict["gap_tol"]
        self._obj = self._obj.asfreq(freq=interval)
        # self.check_gaps(cls.gap_tol)
        # self._obj.nwismeta._metadict.update(gap_flag=True)
        return None

    # def resolve_masks(self) -> None:
    #     """Void function.  Convert any values from -999999 to NaN values.
    #     See NWISFrame resolve_masking parameter docstring for further information.

    #     Parameters
    #     ----------
    #     None

    #     Returns
    #     -------
    #     None
    #     """

    #     self.data["value"] = self.data["value"].where(self.data["value"] != -999999.0, np.NaN)
    #     self.resolve_masking = True
    #     return None

    # def fill_gaps(
    #     self,
    #     interval: str | None = None,
    # ) -> None:
    #     """Void function.  Fill data gaps in instance time-series data.

    #     Parameters
    #     ----------
    #     interval : str | None, optional
    #         Override for self.gap_tol set in __init__, by default None, if None, fallback on self.gap_tol, by default 15min

    #     Returns
    #     -------
    #     None
    #     """
    #     if interval is None:
    #         interval = self.gap_tol
    #     self.data = self.data.asfreq(freq=interval)
    #     self.check_gaps(self.gap_tol)
    #     self.gap_fill = True
    #     return None


# data = get_nwis("12301933", start_date="2022-02-01", end_date="2022-02-10", param="00060", service="iv")
gap_data = get_nwis(STAID="12301933", start_date="2023-01-03", end_date="2023-01-04", param="63680", gap_fill=False, gap_tol="15min")
gap_data.nwisframe.fill_gaps("15min")
gap_data.nwisframe.fill_gaps("15min")
gap_data.nwisframe.check_gaps("15min")
print(gap_data.nwisframe.site_name)
print(gap_data.nwisframe.coords)
print(gap_data.nwisframe.var_description)
pause = 2
