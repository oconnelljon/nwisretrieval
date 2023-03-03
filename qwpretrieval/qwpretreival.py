import pandas as pd

# url = "https://www.waterqualitydata.us/data/Result/search?siteid=USGS-433615110440001&startDateLo=10-01-2020&startDateHi=10-01-2023&pCode=00400&mimeType=csv"
# data = pd.read_csv(url)
# pause = 2


def construct_url(
    staid: str | int,
    start_date: str,
    end_date: str,
    pcode: str | list | None = None,
) -> str:
    if isinstance(pcode, list):
        pcode = ";".join(pcode)
        return f"https://www.waterqualitydata.us/data/Result/search?siteid=USGS-{staid}&startDateLo={start_date}&startDateHi={end_date}&pCode={pcode}&mimeType=csv"
    if pcode is None:
        return f"https://www.waterqualitydata.us/data/Result/search?siteid=USGS-{staid}&startDateLo={start_date}&startDateHi={end_date}&mimeType=csv"
    return f"https://www.waterqualitydata.us/data/Result/search?siteid=USGS-{staid}&startDateLo={start_date}&startDateHi={end_date}&pCode={pcode}&mimeType=csv"


def getqwp(
    staid: str | int,
    start_date: str,
    end_date: str,
    pcode: str | list | None = None,
) -> pd.DataFrame:
    url = construct_url(
        staid=staid,
        start_date=start_date,
        end_date=end_date,
        pcode=pcode,
    )
    dataframe = pd.read_csv(url, dtype={"USGSPCode": str})
    if dataframe.empty is True:
        print(f"Warning!  No data found at: {url}")
    dataframe["dateTime"] = (
        dataframe["ActivityStartDate"] + " " + dataframe["ActivityStartTime/Time"]
    )
    dataframe["dateTime"] = pd.to_datetime(
        dataframe["dateTime"].array,
        infer_datetime_format=True,
    )
    dataframe.set_index("dateTime", inplace=True)
    dataframe = dataframe.loc[
        :,
        [
            "MonitoringLocationIdentifier",
            "CharacteristicName",
            "ResultMeasureValue",
            "ResultMeasure/MeasureUnitCode",
            "ResultDetectionConditionText",
            "USGSPCode",
        ],
    ]
    dataframe.rename(
        columns={
            "MonitoringLocationIdentifier": "staid",
            "CharacteristicName": "param_name",
            "ResultMeasureValue": "value",
            "ResultMeasure/MeasureUnitCode": "units",
            "ResultDetectionConditionText": "detection_condition",
            "USGSPCode": "pcode",
        },
        inplace=True,
    )
    return dataframe


data = getqwp(
    staid="433615110440001",
    start_date="01-01-2020",
    end_date="01-01-2023",
    # pcode=None,
    # pcode=["00400", "00010", "00925", "00935", "01040"],
)
print(data)
pause = 2
