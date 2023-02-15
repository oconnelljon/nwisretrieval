import pandas as pd
import numpy as np
import requests
import json
from typing import Tuple
from requests.models import Response

from rich import print
from rich.console import Console, ConsoleOptions, RenderResult
from rich.table import Table

# import json
# with open('data.txt', 'w') as f:
#   json.dump(data, f, ensure_ascii=False)


class NWISFrame:
    """
    Container for time-series data downloaded from NWIS: https://nwis.waterservices.usgs.gov/

    Provides a pandas dataframe of time-series data for a single parameter from a single site and accompanying metadata.
    Supports rich!

    Parameters
    ----------
    STAID : str
        NWIS station ID, e.g. "12301933"
    start_date : str
        Start date to being downloading data. e.g. "2022-01-01"
    end_date : str
        End date to being downloading data.  e.g. "2022-01-05"
    param : str
        Parameter to download. discharge=00060, reservoir height=62614.
    stat_code: str
        Statistical code to query data.  00003=mean, 32400=mean at midnight
    service : str, optional, by default "iv"
        Instantanious, "iv" or Daily Value, "dv"
    access : int, optional, by default 0
        Data access level.  0=public, 1=coop, 2=USGS internal
        Options 1 and 2 require USGS network access.
    gap_tol : str, optional, by default "15min"
        Gap tolerance to check for missing values in time-series
        Use "H" for hourly data and "D" for daily data.
        Sets self.gap_flag as True when gaps are found.
        For more information and valid parameters: https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
    gap_fill : bool, optional, by default False
        Set True to fill data gaps as defined by gap_tol
        Gaps are filled with NaN values.
    resolve_masking : bool, optional, by default False
        Set True to resolve NWIS masking of Ice qualified data to NaN values.
        NWIS serves public data with an Ice qualifier as -999999 masked values.
        Setting access to 1 "coop" or 2 "internal" returns actual values.

    Notes
    -----
    Returned metadata:
        site_name = Station name
        url = Query URL
        dec_lat = Decimal latitude
        dec_long = Decimal longitude
        var_description = Queried parameter description.
        approval_flag = Provisional if any queried data is provisional, else data has been Approved.
        qualifier_flag = None if no qualifiers present, otherwise just checks for ICE at the moment.  Need to include other qualifiers.
        gap_flag = True if any gap is found in the queried data.
    """

    def __init__(
        self,
        STAID: str,
        start_date: str,
        end_date: str,
        param: str,
        stat_code: str,
        service: str = "iv",
        access: int = 0,
        gap_tol: str = "15min",
        gap_fill: bool = False,
        resolve_masking: bool = False,
    ):
        self.STAID = STAID
        self.start_date = start_date
        self.end_date = end_date
        self.param = param
        self.stat_code = stat_code
        self.service = service
        self.access = access
        self.gap_tol = gap_tol
        self.gap_fill = gap_fill
        self.resolve_masking = resolve_masking
        self.data, self.station_info = self.getNWIS(
            STAID=self.STAID,
            start_date=self.start_date,
            end_date=self.end_date,
            param=self.param,
            service=self.service,
            stat_code=self.stat_code,
            access=self.access,
        )
        self.dec_lat = self.station_info.get("dec_lat", "empyty")
        self.dec_long = self.station_info.get("dec_long", "empyty")
        self.url = self.station_info.get("query_url", "empyty")
        self.site_name = self.station_info.get("site_name", "empyty")
        self.var_description = self.station_info.get("var_description", "empyty")
        self.approval_level = self.check_approval()
        self.qualifier_flag = self.check_quals()
        if self.service == "dv":
            self.gap_tol = "D"
        self.gap_flag = False
        self.check_gaps(interval=self.gap_tol)
        if gap_fill:
            self.fill_gaps(interval=self.gap_tol)
        if self.resolve_masking:
            self.resolve_masks()

    # def __str__(self):
    #     return f"STAID: {self.STAID} \n Station Name: {self.site_name} \n Query URL: {self.url} \n Start Date: {self.start_date} \n End Date: {self.end_date} \n Parameter code: {self.param} \n Dec. latitude: {self.dec_lat} \n Dec. Longitude: {self.dec_long} \n Approval level: {self.approval_flag} \n Qualifier: {self.qualifier_flag} \n Gaps: {self.gap_flag} \n Gap fill: {self.gap_fill} \n "

    def __repr__(self):
        return f"NWISFrame(STAID={self.STAID}, start_date={self.start_date}, end_date={self.end_date}, param={self.param}, service={self.service}, access={self.access})"

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        """
        When "from rich import print" is enabled, replaces __repr__ output as formatted table when calling print({NWISFrame instance})

        Parameters
        ----------
        console : Console
            rich console instance
        options : ConsoleOptions
            rich console options

        Returns
        -------
        RenderResult
            rich RenderResult object

        Yields
        ------
        Iterator[RenderResult]
            rich table to display __repr__ data

        Example
        -------
        from rich import print
        from nwisretrieval import NWISFrame

        data = NWISFrame(STAID="12301933", start_date="2023-01-03", end_date="2023-01-04", param="63680", access=0, gap_fill=False)
        print(data)
        ┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃ Attribute      ┃ Value                                                                                                                                                 ┃
        ┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
        │ STAID          │ 12301933                                                                                                                                              │
        │ Station name   │ Kootenai River bl Libby Dam nr Libby MT                                                                                                               │
        │ Query URL      │ https://nwis.waterservices.usgs.gov/nwis/iv/?format=json&sites=12301933&parameterCd=63680&startDT=2023-01-03&endDT=2023-01-04&siteStatus=all&access=0 │
        │ Start Date     │ 2023-01-03                                                                                                                                            │
        │ End Date       │ 2023-01-04                                                                                                                                            │
        │ Parameter      │ 63680                                                                                                                                                 │
        │ Latitude       │ 48.40066389                                                                                                                                           │
        │ Longitude      │ -115.3187194                                                                                                                                          │
        │ Approval level │ Provisional                                                                                                                                           │
        │ Qualifier      │ None                                                                                                                                                  │
        │ Gaps           │ False                                                                                                                                                 │
        │ Gap fill       │ True                                                                                                                                                  │
        └────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

        Notes
        -----
        This function is called automatically when print(NWISFrame) is used.
        """
        repr_table = Table()
        repr_table.add_column("Attribute", style="blue")
        repr_table.add_column("Value", style="cyan")
        repr_table.add_row("STAID", self.STAID)
        repr_table.add_row("Station name", self.site_name)
        repr_table.add_row("Query URL", self.url)
        repr_table.add_row("Start Date", self.start_date)
        repr_table.add_row("End Date", self.end_date)
        repr_table.add_row("Parameter", self.param)
        repr_table.add_row("Latitude", str(self.dec_lat))
        repr_table.add_row("Longitude", str(self.dec_long))
        repr_table.add_row("Approval level", str(self.approval_level))
        repr_table.add_row("Qualifier", str(self.qualifier_flag))
        repr_table.add_row("Gaps", str(self.gap_flag))
        repr_table.add_row("Gap fill", str(self.gap_fill))
        repr_table.add_row("Resolve masking", str(self.resolve_masking))
        repr_table.add_row("__repr__", self.__repr__())
        yield repr_table

    def getNWIS(
        self,
        STAID: str,
        start_date: str,
        end_date: str,
        param: str,
        stat_code: str,
        service: str,
        access: int,
    ) -> Tuple[pd.DataFrame, dict]:
        """Retrieve time-series data and metadata from nwis.waterservices.usgs.gov

        Parameters
        ----------
        STAID : str
            Station ID
        start_date : str
            Inclusive start date of data pull
        end_date : str
            Inclusive end date of data pull
        param : str, optional
            Parameter to query
        service : str, optional
            Instantanious, "iv" or Daily Value, "dv"
        access : int, optional
            Data access level.  0=public, 1=coop, 2=USGS internal

        Returns
        -------
        Tuple
            Tuple of pandas DataFrame first and station metadata second

        Notes
        -----

        """
        url = self.construct_url(
            STAID=STAID,
            start_date=start_date,
            end_date=end_date,
            param=param,
            stat_code=stat_code,
            service=service,
            access=access,
        )
        response = self.query_url(url)
        jdata = json.loads(response.text)

        data_frame = pd.json_normalize(jdata["value"]["timeSeries"][0]["values"][0], ["value"])
        if data_frame.empty is True:
            print(f"Critical error!  Response status code: {response.status_code}\n No data found at: {url}")
            raise SystemExit

        data_frame.columns = data_frame.columns.str.lower()
        data_frame = data_frame.set_index(self._df_time_helper(data_frame))
        data_frame["value"] = pd.to_numeric(data_frame["value"])
        del data_frame["datetime"]

        station_info = {
            "query_url": url,
            "site_name": jdata["value"]["timeSeries"][0]["sourceInfo"]["siteName"],
            "dec_lat": jdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["latitude"],
            "dec_long": jdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["longitude"],
            "va_description": jdata["value"]["timeSeries"][0]["variable"]["variableDescription"],
        }
        return data_frame, station_info

    def query_url(
        self,
        url: str,
    ) -> Response:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Critical error!  No data found at: {url}\n Reason: {response.reason}")
            raise SystemExit
        return response

    def construct_url(
        self,
        STAID: str,
        start_date: str,
        end_date: str,
        param: str,
        stat_code: str,
        service: str,
        access: int,
    ) -> str:
        if service not in {"dv", "iv"}:
            print(f"Critical error!  Invalid service: {service}")
            raise SystemExit
        service_urls = {
            "dv": f"https://nwis.waterservices.usgs.gov/nwis/dv/?format=json&sites={STAID}&startDT={start_date}&endDT={end_date}&statCd={stat_code}&parameterCd={param}&siteStatus=all&access={access}",
            "iv": f"https://nwis.waterservices.usgs.gov/nwis/iv/?format=json&sites={STAID}&parameterCd={param}&startDT={start_date}&endDT={end_date}&siteStatus=all&access={access}",
        }
        return str(service_urls.get(service))  # add str() to solve linting type error

    def resolve_masks(self) -> None:
        """Void function.  Convert any values from -999999 to NaN values.
        See NWISFrame resolve_masking parameter docstring for further information.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        self.data["value"] = self.data["value"].where(self.data["value"] != -999999.0, np.NaN)
        self.resolve_masking = True
        return None

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
        unique_quals = list(pd.unique(self.data["qualifiers"].apply(frozenset)))
        for qual in unique_quals:
            if "Ice" in qual or "i" in qual:
                return "Ice"
        return "None"

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
        unique_quals = list(pd.unique(self.data["qualifiers"].apply(frozenset)))
        return next(
            ("Provisional" for approval in unique_quals if "P" in approval),
            "Approved",
        )

    def check_gaps(
        self,
        interval: str | None = None,
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
        if interval is None:
            interval = self.gap_tol
        idx = pd.date_range(self.start_date, self.end_date, freq=interval)
        if idx.difference(self.data.index).empty:
            self.gap_flag = False
            return None

        print(f"Gaps detected at: {self.STAID}")
        self.gap_flag = True
        return None

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
            interval = self.gap_tol
        self.data = self.data.asfreq(freq=interval)
        self.check_gaps(self.gap_tol)
        self.gap_fill = True
        return None

    def _df_time_helper(self, data: pd.DataFrame) -> pd.DatetimeIndex:
        """Helper function to convert datetime to DatetimeIndex.
        String manipulation of dates before conversion is much faster."""
        time_df = pd.DataFrame(data["datetime"].str.split("T").to_list(), columns=["date", "time_tz"])
        time_df["time"] = time_df["time_tz"].apply(lambda x: x[:8])
        time_df["datetime"] = pd.to_datetime(time_df["date"] + " " + time_df["time"])
        time_df = time_df.set_index("datetime")
        return time_df.index.copy()

    def _resample_df(self, data: pd.DataFrame) -> pd.DataFrame:
        """Helper function to resample dataframe to daily values.
        No DVs for St Mary Canal or River in NWIS, need to improvise."""
        resample_data = pd.DataFrame()
        resample_data["value"] = data["value"].resample("D").mean()
        resample_data["qualifiers"] = data["qualifiers"].resample("D").apply(lambda x: x.mode())
        return resample_data


if __name__ == "__main__":
    LakeSherburneID = "05015500"
    StMaryCanalID = "05018500"  # st Mary Canal NWIS ID, this is diverted into the Milk River for use downstream
    StMaryRiverID = "05020500"

    SM_canal = NWISFrame(STAID="05020500", start_date="2022-07-01", end_date="2022-10-02", param="62614", stat_code="32400", access=2, resolve_masking=False, service="dv")

    # data_ice = NWISFrame(STAID="12301250", start_date="2023-01-02", end_date="2023-01-03", param="00060", access=0, resolve_masking=False)
    # print(data_ice)
    # data_gaps = NWISFrame(STAID="12301933", start_date="2023-01-03", end_date="2023-01-04", param="63680", access=0, resolve_masking=False)
    # print(data_gaps)
    pause = 2
