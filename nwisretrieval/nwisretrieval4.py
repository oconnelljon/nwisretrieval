import pandas as pd
import numpy as np
import requests


# class SubclassedDataFrame2(pd.DataFrame):
#     _metadata = ["added_prop"]

#     @property
#     def _constructor(self):
#         return SubclassedDataFrame2


# df = SubclassedDataFrame2({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]})
# df.added_property = "property"
# df.added_property


# class CustomFrame(pd.DataFrame):
#     _metadata = ["_metadict", "start_date", " end_date"]

#     @property
#     def _constructor(self):
#         return CustomFrame

#     @property
#     def start_date(self):
#         return self._metadict.get("start_date")

#     @property
#     def end_date(self):
#         return self._metadict.get("end_date")


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
        "start": start_date,
        "end": end_date,
        "gap_flag": None,
        "gap_tol": None,
    }
    # Save metadata to dataframe
    dataframe = NWISFrame(dataframe)
    # dataframe._metadata[0] = metadata
    # dataframe.start_date = start_date
    # dataframe.end_date = end_date
    dataframe._metadict = metadata
    return dataframe


# @pd.api.extensions.register_dataframe_accessor("nwismeta")
# class NWISMeta:
#     def __init__(self, pandas_obj) -> None:
#         self._obj = pandas_obj
#         self._metadict: dict = {}


# @pd.api.extensions.register_dataframe_accessor("nwisframe")
class NWISFrame(pd.DataFrame):
    # register custom properties in this list.  Allows for propogation of metadata after dataframe operations.
    _metadata = ["_metadict", "start", " end", "gap_tol", "gap_flag"]

    # def __init__(self, *args, **kwargs):
    #     if "metadata" in kwargs:
    #         self._metadict = kwargs["metadata"]
    #         kwargs.pop("metadata")
    #     super(NWISFrame, self).__init__(*args, **kwargs)
    #     self.start_date = self._metadict["start_date"]
    #     self.end_date = self._metadict["end_date"]

    @property
    def _constructor(self):
        return NWISFrame

    @property
    def start_date(self):
        return str(self._metadict.get("start"))
        # return str(self._metadata[0].get("start_date"))

    @property
    def end_date(self):
        return str(self._metadict.get("end"))

    # @property
    # def gap_tol(self):
    #     return str(self._metadict.get("gap_tol"))

    def fill_gaps(
        self,
        interval: str | None = None,
    ):
        # if interval is None:
        #     interval = self.gap_tol
        # reassignment of self._obj loses original _metadict
        # metadata = self._metadict
        self = self.asfreq(freq=interval)  # , NWISFrame(metadata=self._metadict)
        return self

    def check_gaps(
        self,
        interval: str,
    ) -> None:
        # if interval is None:
        #     interval = self.gap_tol
        idx = pd.date_range(self.start_date, self.start_date, freq=interval)
        if idx.difference(self.index).empty:
            self.gap_flag = False
            return None


gap_data = get_nwis(STAID="12301933", start_date="2023-01-03", end_date="2023-01-04", param="63680")
print(gap_data.start_date)
gap_data = gap_data.fill_gaps("15min")
gap_data.check_gaps("15min")
pause = 2
