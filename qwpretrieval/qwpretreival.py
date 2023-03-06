import warnings

import pandas as pd

# url = "https://www.waterqualitydata.us/data/Result/search?siteid=USGS-433615110440001&startDateLo=10-01-2020&startDateHi=10-01-2023&pCode=00400&mimeType=csv"
# data = pd.read_csv(url)
# pause = 2


def _get_base_url(service: str) -> str:
    """Get the base url for the selected QWP service.

    Parameters
    ----------
    service : str
        Web service to query QWP with.

    Returns
    -------
    str
        QWP web service base url.
    """
    WEB_SERVICES_DICT = {
        "project": "https://www.waterqualitydata.us/data/Project/search?",
        "site metadata": "https://www.waterqualitydata.us/data/Station/search?",
        "results": "https://www.waterqualitydata.us/data/Result/search?",
        "result detection": "https://www.waterqualitydata.us/data/ResultDetectionQuantitationLimit/search?",
        "activity": "https://www.waterqualitydata.us/data/Activity/search?",
        "activity metric": "https://www.waterqualitydata.us/data/ActivityMetric/search?",
        "biological metric": "https://www.waterqualitydata.us/data/BiologicalMetric/search?",
        "project weighting": "https://www.waterqualitydata.us/data/ProjectMonitoringLocationWeighting/search?",
    }
    return WEB_SERVICES_DICT.get(service, "Invalid Service")


def _construct_url_query(**kwargs) -> str:
    """Convert kwargs to parameter=argument pairs to url query.
    kwargs are validated and and returned as part of the query if they are
    valid QWP parameter/query pairs.

    Returns
    -------
    str
        parmeter=argument pairs concantenated with '&'
        to be appened to base service url for querying QWP data.
    Notes
    -----
    getqwp() is expecting csv data to be queried. mimeType is the parameter
    QWP uses to specify the format of the queried data. The mimeType is hardcoded
    here until other formats are supported.
    """
    kwargs["mimeType"] = "csv"
    if "pCode" in kwargs and isinstance(kwargs["pCode"], list):
        kwargs["pCode"] = ";".join(kwargs["pCode"])
    if "siteid" in kwargs:
        # QWP needs an agency identifier before the STAID
        kwargs["siteid"] = "USGS-" + kwargs["siteid"]
    valid_kwargs = _validate_url_kwargs(kwargs)
    REST_param_args = ["=".join([key, val]) for key, val in valid_kwargs.items()]
    return "&".join(REST_param_args)


def _validate_url_kwargs(kwargs) -> dict:
    """Validate kwargs for QWP paremeter=argument pairs.
    Removes any kwargs that do not have valid keys in REST_PARAMETERS.

    Parameters
    ----------
    kwargs : dict
        parameter: argument pairs to validate before concantenating
        to base service url.

    Returns
    -------
    dict
        Valid parameter=argument pairs to query QWP.
        QWP can only accept these parameters, so ignore anything else.
    """
    REST_PARAMETERS = {
        "bBox",
        "lat",
        "long",
        "within",
        "countrycode",
        "statecode",
        "countycode",
        "organization",
        "siteid",
        "huc",
        "sampleMedia",
        "characteristicName",
        "pCode",
        "activityId",
        "startDateLo",
        "startDateHi",
        "mimeType",
        "Zip",
        "providers",
        "sorted",
        "dataProfile",
    }
    valid_keys = REST_PARAMETERS & set(kwargs.keys())
    return {key: kwargs[key] for key in valid_keys}


def _construct_url(service: str, **kwargs) -> str:
    """Construct URL with a base and query parameter=argument pairs.

    Parameters
    ----------
    service : str
        QWP service to query.

    Returns
    -------
    str
        URL to query QWP with.
    """
    base_url = _get_base_url(service)
    url_args = _construct_url_query(**kwargs)
    return base_url + url_args


def _set_datetime_index(dataframe: pd.DataFrame) -> None:
    """Combine date and time columns to a DateTimeIndex.  Operates in place.

    Parameters
    ----------
    dataframe : pd.DataFrame
        DataFrame returned from QWP query

    Returns
    -------
    None
        Operates on dataframe in place, no return value.
    """
    dataframe["dateTime"] = (
        dataframe["ActivityStartDate"] + " " + dataframe["ActivityStartTime/Time"]
    )
    dataframe["dateTime"] = pd.to_datetime(
        dataframe["dateTime"].array,
        infer_datetime_format=True,
    )
    dataframe.set_index("dateTime", inplace=True)
    return None


def getqwp(
    siteid: str | int,
    startDateLo: str,
    service: str,
    **kwargs,
) -> pd.DataFrame:
    """Query the QWP and return a pandas DataFrame.
    Parameter kwargs can be any valid parameter-argument pair for QWP.
    If pCode is not specified, all parameters are queried.
    Pass multiple pCodes as a list.

    Parameters
    ----------
    siteid : str | int
        Station ID
    startDateLo : str
        Date to begin data query in the format MM-DD-YYYY
    service : str
        QWP service to query.  Currently only "results" have been tested.
        Other potentially valid services:
        "project"
        "site metadata"
        "results"
        "result detection"
        "activity"
        "activity metric"
        "biological metric"
        "project weighting"

    Returns
    -------
    pd.DataFrame
        DateTime indexed dataframe of QWP query results.

    Notes
    -----
    The function accepts any valid parameter=argument pairs from table 1 found here:
    https://www.waterqualitydata.us/webservices_documentation/

    pCodes can be found here:
    http://water.nv.gov/hearings/past/Spring%20Valley%202006/exhibits/SNWA/5__Hydrochemistry_and_Geochemistry/Data/USGS/USGS_NWIS/ParameterCodes.htm

    The function argument naming convention is meant to follow that of QWP parameters
    and thus do not follow classic "pythonic" conventions.
    """
    url = _construct_url(
        siteid=siteid,
        startDateLo=startDateLo,
        service=service,
        **kwargs,
    )
    # USGS has leading '0's in pCodes. Treat them as strings to avoid dropping them.
    dataframe = pd.read_csv(url, dtype={"USGSPCode": str})
    if dataframe.empty is True:
        warnings.warn(f"\nNo data for station {siteid} at:\n{url}", stacklevel=2)
    # Combine date and time columns and form dateTime index.
    _set_datetime_index(dataframe)
    #  QWP attaches an agency identifier to the siteid. Remove that identifier.
    dataframe["MonitoringLocationIdentifier"] = dataframe[
        "MonitoringLocationIdentifier"
    ].str.slice(5)
    # Remove excessive columns and rename.
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


if __name__ == "__main__":
    data = getqwp(
        siteid="433615110440001",
        startDateLo="01-01-2020",
        startDateHi="01-01-2023",
        service="results",
        # pCode=["00400", "00010"],  # , "00925", "00935", "01040"
    )
    # print(data)
    # data.to_csv("data.txt")
    pause = 2
