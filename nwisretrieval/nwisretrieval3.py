# import pandas as pd
# import numpy as np
# import requests


# def get_nwis(
#     STAID: str,
#     start_date: str,
#     end_date: str,
#     param: str,
#     stat_code: str = "32400",
#     service: str = "iv",
#     access: int = 0,
# ):
#     service_urls = {
#         "dv": f"https://nwis.waterservices.usgs.gov/nwis/dv/?format=json&sites={STAID}&startDT={start_date}&endDT={end_date}&statCd={stat_code}&parameterCd={param}&siteStatus=all&access={access}",
#         "iv": f"https://nwis.waterservices.usgs.gov/nwis/iv/?format=json&sites={STAID}&parameterCd={param}&startDT={start_date}&endDT={end_date}&siteStatus=all&access={access}",
#     }
#     url = service_urls[service]

#     response = requests.request("GET", url)
#     rdata = response.json()
#     dataframe = pd.json_normalize(rdata, ["value", "timeSeries", "values", "value"])
#     dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"], infer_datetime_format=True)
#     dataframe.set_index("dateTime", inplace=True)
#     dataframe["value"] = pd.to_numeric(dataframe["value"])

#     metadict = {
#         "start_date": start_date,
#         "end_date": end_date,
#         "gap_flag": None,
#         "gap_tol": None,
#     }
#     # Save metadata to dataframe
#     dataframe._metadata = metadict
#     return dataframe


# @pd.api.extensions.register_dataframe_accessor("nwismeta")
# class NWISMeta:
#     def __init__(self, pandas_obj) -> None:
#         self._obj = pandas_obj
#         self._metadict: dict = {}


# @pd.api.extensions.register_dataframe_accessor("nwisframe")
# class NWISFrame:
#     def __init__(self, pandas_obj) -> None:
#         self._obj = pandas_obj

#     @property
#     def start_date(self):
#         return self._obj._metadata.get("start_date")

#     @property
#     def end_date(self):
#         return self._obj._metadata.get("end_date")

#     def fill_gaps(
#         self,
#         interval: str | None = None,
#     ) -> None:
#         if interval is None:
#             interval = self._obj._metadata["gap_tol"]
#         # reassignment of self._obj loses original _metadict
#         self._obj = self._obj.asfreq(freq=interval)
#         return None

#     def check_gaps(
#         self,
#         interval: str,
#     ) -> None:
#         idx = pd.date_range(self.start_date, self.start_date, freq=interval)
#         if idx.difference(self._obj.index).empty:
#             self.gap_flag = False
#             return None


# gap_data = get_nwis(STAID="12301933", start_date="2023-01-03", end_date="2023-01-04", param="63680")
# gap_data.nwisframe.fill_gaps("15min")
# gap_data.nwisframe.check_gaps("15min")
# pause = 2

# Notice that we add the new property 'time_0' to _metadata
# and that we overwrite the __init__() function.
import pandas as pd
import requests


def get_nwis(
    STAID: str,
    start_date: str,
    end_date: str,
    param: str,
    stat_code: str = "32400",
    service: str = "iv",
    access: int = 0,
):
    service_urls = {
        "dv": f"https://nwis.waterservices.usgs.gov/nwis/dv/?format=json&sites={STAID}&startDT={start_date}&endDT={end_date}&statCd={stat_code}&parameterCd={param}&siteStatus=all&access={access}",
        "iv": f"https://nwis.waterservices.usgs.gov/nwis/iv/?format=json&sites={STAID}&parameterCd={param}&startDT={start_date}&endDT={end_date}&siteStatus=all&access={access}",
    }
    url = service_urls[service]

    response = requests.request("GET", url)
    rdata = response.json()
    dataframe = pd.json_normalize(rdata, ["value", "timeSeries", "values", "value"])
    dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"], infer_datetime_format=True)
    dataframe.set_index("dateTime", inplace=True)
    dataframe["value"] = pd.to_numeric(dataframe["value"])

    metadata = {
        "start_date": start_date,
        "end_date": end_date,
        "gap_flag": None,
        "gap_tol": None,
    }
    # Save metadata to dataframe
    dataframe = NWISFrame(dataframe, metadata=metadata)
    # dataframe._metadata[0] = metadict
    # dataframe._metadict = metadict
    return dataframe


class NWISFrame(pd.DataFrame):

    _metadata = ["time_0"]

    def __init__(self, *args, **kwargs):
        if "time_0" in kwargs:
            time_0 = kwargs["time_0"]
            kwargs.pop("time_0")
        else:
            time_0 = 0
        super(NWISFrame, self).__init__(*args, **kwargs)
        self.time_0 = time_0

    @property
    def _constructor(self):
        return NWISFrame


# df = NWISFrame({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}, time_0=5 * 1)
gap_data = get_nwis(STAID="12301933", start_date="2023-01-03", end_date="2023-01-04", param="63680")
print(gap_data.start_date)
gap_data.time_0
pause = 2
