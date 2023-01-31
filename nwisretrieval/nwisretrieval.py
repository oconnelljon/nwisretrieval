import pandas as pd
import requests
import json
from typing import Tuple


# import json
# with open('data.txt', 'w') as f:
#   json.dump(data, f, ensure_ascii=False)


class NWISFrame:
    def __init__(self, STAID: str, start_date: str, end_date: str, param: str = "00060", service: str = "iv", access: int = 0, gap_freq: str = "15min"):
        self.STAID = STAID
        self.start_date = start_date
        self.end_date = end_date
        self.param = param
        self.service = service
        self.access = access
        self.gap_freq = gap_freq
        self.data, self.station_info = self.getNWIS(self.STAID, self.start_date, self.end_date, self.param, self.service, self.access)
        self.dec_lat = self.station_info.get("dec_lat")
        self.dec_long = self.station_info.get("dec_long")
        self.url = self.station_info.get("query_url")
        self.site_name = self.station_info.get("site_name")
        self.var_description = self.station_info.get("var_description")
        self.approval_flag = self.station_info.get("approval_flag")
        self.qualifier_flag = self.station_info.get("qualifier_flag")
        self.gaps = False
        self.check_gaps(interval=self.gap_freq)

    def __str__(self):
        return f"STAID: {self.STAID} \n Station Name: {self.site_name} \n Query URL: {self.url} \n Start Date: {self.start_date} \n End Date: {self.end_date} \n Parameter code: {self.param} \n Dec. latitude: {self.dec_lat} \n Dec. Longitude: {self.dec_long} \n Approval level: {self.approval_flag} \n Qualifier: {self.qualifier_flag} \n Gaps: {self.gaps}"

    def __repr__(self):
        return f"NWISFrame(STAID={self.STAID}, start_date={self.start_date}, end_date={self.end_date}, param={self.param}, service={self.service}, access={self.access})"

    def getNWIS(
        self,
        STAID: str,
        start_date: str,
        end_date: str,
        param: str,
        service: str,
        access: int,
    ) -> Tuple:
        """Retrieve time-series data from nwis.waterservices.usgs.gov

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
            Instantainous values = iv, Daily values = dv
        access : int, optional
            Data access level.  0=public, 1=coop, 2=USGS internal

        Returns
        -------
        Tuple
            Tuple of pandas DataFrame first and station metadata second
        """
        url = f"https://nwis.waterservices.usgs.gov/nwis/{service}/?format=json&sites={STAID}&parameterCd={param}&startDT={start_date}&endDT={end_date}&siteStatus=all&access={access}"
        response = requests.get(url)
        jdata = json.loads(response.text)

        try:
            data_frame = pd.json_normalize(jdata["value"]["timeSeries"][0]["values"][0], ["value"])
        except IndexError as e:
            print(f"Critical error!  No data found at: {url}")
            raise SystemExit from e

        data_frame.columns = data_frame.columns.str.lower()
        data_frame = data_frame.set_index(self._df_time_helper(data_frame))
        approval_flag, qualifier_flag = self.check_quals(data=data_frame, qualifier_col="qualifiers")
        # data_frame["value"] = data_frame["value"].where(data_frame["value"] != -999999.0, np.NaN)
        data_frame["value"] = pd.to_numeric(data_frame["value"])

        station_info = {
            "query_url": url,
            "site_name": jdata["value"]["timeSeries"][0]["sourceInfo"]["siteName"],
            "dec_lat": jdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["latitude"],
            "dec_long": jdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["longitude"],
            "va_description": jdata["value"]["timeSeries"][0]["variable"]["variableDescription"],
            "approval_flag": approval_flag,
            "qualifier_flag": qualifier_flag,
        }
        del data_frame["datetime"]
        return data_frame, station_info

    def check_quals(self, data: pd.DataFrame, qualifier_col: str):
        # add additional qualifier checks here
        provisional = "Approved"
        ice = "None"
        unique_quals = list(pd.unique(data[qualifier_col].apply(frozenset)))
        for f_set in unique_quals:
            if "P" in f_set:
                provisional = "Provisional"
            if "Ice" in f_set or "i" in f_set:
                ice = "Ice"
        return provisional, ice

    def check_gaps(self, interval: str | None = None):
        if interval is None:
            interval = self.gap_freq
        idx = pd.date_range(self.start_date, self.end_date, freq=interval)
        if idx.difference(self.data.index).empty:
            self.gaps = False
            return
        print(f"Gaps detected at: {self.STAID}")
        self.gaps = True
        return

    def fill_gaps(self, interval: str | None = None):
        if interval is None:
            interval = self.gap_freq
        self.data = self.data.asfreq(freq=interval)
        self.check_gaps(self.gap_freq)
        return

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


# data = NWISFrame(STAID="05020500", start_date="2022-01-04", end_date="2022-01-05", access=2)
# data2 = NWISFrame(STAID="05020500", start_date="2023-01-04", end_date="2023-01-05", access=2)
data3 = NWISFrame(STAID="12301933", start_date="2023-01-03", end_date="2023-01-04", param="63680", access=2)
pause = 2
