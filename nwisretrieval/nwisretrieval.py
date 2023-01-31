import pandas as pd
import numpy as np
import requests
import json
from typing import Tuple, List
from dataclasses import dataclass


# import json
# with open('data.txt', 'w') as f:
#   json.dump(data, f, ensure_ascii=False)


class NWISFrame:
    def __init__(self, STAID: str, start_date: str, end_date: str, param="00600", service="iv", access="0"):
        self.STAID = STAID
        self.start_date = start_date
        self.end_date = end_date
        self.param = param
        self.service = service
        self.access = access
        self.data, self.station_info = self.getNWIS(self.STAID, self.start_date, self.end_date)
        self.dec_lat = self.station_info.get("dec_lat")
        self.dec_long = self.station_info.get("dec_long")
        self.url = self.station_info.get("query_url")
        self.site_name = self.station_info.get("site_name")
        self.var_description = self.station_info.get("var_description")

    def getNWIS(
        self,
        STAID: str,
        start_date: str,
        end_date: str,
        param="00060",
        service="iv",
        access=0,
    ) -> Tuple:
        """_summary_

        Parameters
        ----------
        STAID : str
            Station ID
        start_date : str
            Inclusive start date of data pull
        end_date : str
            Inclusive end date of data pull
        param : str, optional
            Parameter to query, by default "00060" - discharge cfs
        service : str, optional
            Instantainous values = iv, Daily values = dv, by default "iv"
        access : int, optional
            Data access level.  0=public, 1=coop, 2=USGS internal, by default 0

        Returns
        -------
        Tuple
            _description_
        """
        url = f"https://nwis.waterservices.usgs.gov/nwis/{service}/?format=json&sites={STAID}&parameterCd={param}&startDT={start_date}&endDT={end_date}&siteStatus=all&access={access}"
        response = requests.get(url)
        jdata = json.loads(response.text)
        station_info = {
            "query_url": url,
            "site_name": jdata["value"]["timeSeries"][0]["sourceInfo"]["siteName"],
            "dec_lat": jdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["latitude"],
            "dec_long": jdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["longitude"],
            "va_description": jdata["value"]["timeSeries"][0]["variable"]["variableDescription"],
        }

        data_frame = pd.json_normalize(jdata["value"]["timeSeries"][0]["values"][0], ["value"])
        #  Split qualifiers column to qualifier and approval?
        data_frame.columns = data_frame.columns.str.lower()
        data_frame["value"] = pd.to_numeric(data_frame["value"])
        data_frame = data_frame.set_index(self._df_time_helper(data_frame))
        data_frame["value"] = data_frame["value"].where(data_frame["value"] != -999999.0, np.NaN)
        idx = pd.date_range(start_date, end_date)
        # data_frame = data_frame.reindex(idx, fill_value=np.NaN)
        del data_frame["datetime"]

        return data_frame, station_info

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


data = NWISFrame(STAID="12301933", start_date="2022-05-01", end_date="2022-10-01")
pause = 2
