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
        "_STAID": STAID,
        "_start_date": start_date,
        "_end_date": end_date,
        "_parame": param,
        "_stat_code": stat_code,
        "_service": service,
        "_access_level": access,
        "_url": url,
        # "_gap_tol": gap_tol,
        # "_gap_fill": gap_fill,
        # "_resolve_masking": resolve_masking,
        "_gap_flag": None,
        "_site_name": rdata["value"]["timeSeries"][0]["sourceInfo"]["siteName"],
        "_coords": (
            rdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["latitude"],
            rdata["value"]["timeSeries"][0]["sourceInfo"]["geoLocation"]["geogLocation"]["longitude"],
        ),
        "_var_description": rdata["value"]["timeSeries"][0]["variable"]["variableDescription"],
    }
    dataframe = NWISFrame(dataframe)
    dataframe._metadict = metadata
    return dataframe


class NWISFrame(pd.DataFrame):
    # register custom properties in this list.  Allows for propogation of metadata after dataframe manipulations e.g. slicing, asfreq, etc.
    _metadata = ["_metadict"]

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
        return str(self._metadict.get("_gap_tol"))

    @property
    def gaps(self):
        return self._metadict.get("_gap_flag")

    def fill_gaps(
        self,
        interval: str | None = None,
    ):
        if interval is None:
            interval = self.gap_tolerance
        self = self.asfreq(freq=interval)
        return self

    def check_gaps(
        self,
        interval: str,
    ) -> None:
        if interval is None:
            interval = self.gap_tolerance
        idx = pd.date_range(self.start_date, self.start_date, freq=interval)
        if idx.difference(self.index).empty:
            self.gap_flag = False
            return None


gap_data = get_nwis(STAID="12301933", start_date="2023-01-03", end_date="2023-01-04", param="63680")
print(gap_data.STAID)
print(gap_data.start_date)
print(gap_data.end_date)
print(gap_data.param)
print(gap_data.stat_code)
print(gap_data.url)
print(gap_data.site_name)
print(gap_data.coords)
print(gap_data.var_description)
print(gap_data.gap_tolerance)
print(gap_data.gaps)

gap_data = gap_data.fill_gaps("15min")
gap_data.check_gaps("15min")
pause = 2
