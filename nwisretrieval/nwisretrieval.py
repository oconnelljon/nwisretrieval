import warnings

import numpy as np
import pandas as pd
import requests
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

    @properties cannot be set by assignment e.g. STAID = "12301933" or self.STAID = "12301933"
    However, they are accessed like a class variable e.g. my_dataframe.STAID returns "12301933"
    """

    # register custom dataframe properties in this list.
    # Allows for propogation of metadata after dataframe manipulations e.g. slicing, asfreq, copy(), etc.
    # _metadict is created and assigned to the dataframe from get_nwis.
    _metadata = ["_metadict"]

    @property
    def _constructor(self):
        # Override of pd.DataFrame _constructor property.
        # Allows for the retention of subclasses through data manipulations.
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

    # @property
    # def mask_flag(self):
    #     return self._metadict.get("_mask_flag")

    @property
    def approval(self):
        return self.check_approval()

    @property
    def qualifier(self):
        return self.check_quals()

    def check_quals(self) -> str:
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
        unique_quals = list(pd.unique(self["qualifiers"].apply(frozenset)))
        for qual in unique_quals:
            if "Ice" in qual or "i" in qual:
                return "Ice"
        return "None"

    def check_gaps(
        self,
        gap_tol: str | None = None,
    ) -> bool | str:
        """gap_flag property calls this function to check if there are gaps in the time-series data

        Parameters
        ----------
        gap_tol : str | None, optional
            Gap tolerance, by default None
            If no gap tolerance is specified, fall back on self.gap_tolerance property

        Returns
        -------
        bool | None
            True if gaps are present in the index and false if no gaps are found.

            If neither gap_tol or self.gap_tolerance property are specified, returns string "unknown"
        """
        gap_tol = self._resolve_gaptolerance(gap_tol)
        if gap_tol == "unknown":
            warnings.warn(f"Warning: No gap tolerance specified for {self.STAID}.")
            return "unknown"
        if self.gap_data(gap_tol).empty:
            self._metadict["_gap_flag"] = False
            return False
        self._metadict["_gap_flag"] = True
        # print(f"Gaps detected at: {self.STAID} with a tolerance of {gap_tol}")
        return True

    def gap_data(self, gap_tol):
        gap_tol = self._resolve_gaptolerance(gap_tol)
        idx = pd.date_range(self.start_date, self.end_date, freq=gap_tol)
        return idx.difference(self.index)

    def fill_gaps(
        self,
        gap_tol: str | None = None,
    ):
        gap_tol = self._resolve_gaptolerance(gap_tol)
        try:
            self = self.asfreq(freq=gap_tol)
            self._metadict["_gap_flag"] = False
            self._metadict["_gap_tolerance"] = gap_tol
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
        unique_quals = list(pd.unique(self["qualifiers"].apply(frozenset)))
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
        self._metadict["_mask_flag"] = True
        return None

    def _resolve_gaptolerance(self, gap_tol) -> str:
        """
        If no gap_tol, fall back on self.gap_tolerance property.

        If gap_tol is None and self.gap_tolerance is None, return None

        If neither gap_tol or self.gap_tolerance, return "unknown"
        """
        if gap_tol is None:
            return self.gap_tolerance if self.gap_tolerance is not None else "unknown"
        return gap_tol


def query_url(
    url: str,
) -> Response:
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
):
    service_urls = {
        "dv": f"https://nwis.waterservices.usgs.gov/nwis/dv/?format=json&sites={STAID}&startDT={start_date}&endDT={end_date}&statCd={stat_code}&parameterCd={param}&siteStatus=all&access={access}",
        "iv": f"https://nwis.waterservices.usgs.gov/nwis/iv/?format=json&sites={STAID}&parameterCd={param}&startDT={start_date}&endDT={end_date}&siteStatus=all&access={access}",
    }
    url = service_urls[service]
    response = query_url(url)
    rdata = response.json()

    dataframe = pd.json_normalize(rdata, ["value", "timeSeries", "values", "value"])
    if dataframe.empty is True:
        print(f"Critical error!  Response status code: {response.status_code}\n No data found at: {url}")
        raise SystemExit
    dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"].array, infer_datetime_format=True)
    dataframe.set_index("dateTime", inplace=True)
    dataframe = dataframe.tz_localize(None)
    dataframe["value"] = pd.to_numeric(dataframe["value"])

    metadata = {
        "_STAID": STAID,
        "_start_date": start_date,
        "_end_date": end_date,
        "_parame": param,
        "_stat_code": stat_code,
        "_service": service,
        "_access_level": access,
        "_url": url,
        "_gap_tolerance": gap_tol,
        "_gap_fill": gap_fill,
        "_resolve_masking": resolve_masking,
        "_gap_flag": "unknown",
        "_approval": "Provisional",
        "_mask_flag": "unknown",
        "_site_name": rdata["value"]["timeSeries"][0]["sourceInfo"]["siteName"],
        "_coords": (
            rdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["latitude"],
            rdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["longitude"],
        ),
        "_var_description": rdata["value"]["timeSeries"][0]["variable"]["variableDescription"],
    }

    # Custom NWISFrame class inherits from pd.DataFrame and assigns metadata to _metadict.
    dataframe = NWISFrame(dataframe)
    dataframe._metadict = metadata

    if gap_fill and bool(gap_tol):
        dataframe = dataframe.fill_gaps(gap_tol)
    if resolve_masking:
        dataframe.resolve_masks()
    dataframe.check_quals()
    dataframe.check_approval()
    dataframe.check_gaps()

    return dataframe


if __name__ == "__main__":
    # Just some test data down here.
    # gap_data = get_nwis(STAID="12301933", start_date="2023-01-03", end_date="2023-01-04", param="63680")
    data = get_nwis(
        STAID="12301250",
        start_date="2023-01-02",
        end_date="2023-01-03",
        param="00060",
        service="dv",
        gap_tol="D",
    )
    pause = 2
