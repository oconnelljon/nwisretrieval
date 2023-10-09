from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Union

from dataclass_wizard import JSONWizard


@dataclass
class NWISjson(JSONWizard):
    """
    Data dataclass

    """

    name: str
    declared_type: str
    scope: str
    value: "Value"
    nil: bool
    global_scope: bool
    type_substituted: bool


@dataclass
class Value:
    """
    Value dataclass

    """

    query_info: "QueryInfo"
    time_series: List["TimeSeries"]


@dataclass
class QueryInfo:
    """
    QueryInfo dataclass

    """

    query_url: str
    criteria: "Criteria"
    note: List["Note"]


@dataclass
class Criteria:
    """
    Criteria dataclass

    """

    location_param: str
    variable_param: str
    time_param: "TimeParam"
    parameter: str


@dataclass
class TimeParam:
    """
    TimeParam dataclass

    """

    begin_date_time: datetime
    end_date_time: datetime


@dataclass
class Note:
    """
    Note dataclass

    """

    value: Union[str, datetime]
    title: str


@dataclass
class TimeSeries:
    """
    TimeSeries dataclass

    """

    source_info: "SourceInfo"
    variable: "Variable"
    values: List["Values"]
    name: str


@dataclass
class SourceInfo:
    """
    SourceInfo dataclass

    """

    site_name: str
    site_code: List["SiteCode"]
    time_zone_info: "TimeZoneInfo"
    geo_location: "GeoLocation"
    note: str
    site_type: str
    site_property: List["SiteProperty"]


@dataclass
class SiteCode:
    """
    SiteCode dataclass

    """

    value: Union[int, str]
    network: str
    agency_code: str


@dataclass
class TimeZoneInfo:
    """
    TimeZoneInfo dataclass

    """

    default_time_zone: "DefaultTimeZone"
    daylight_savings_time_zone: "DaylightSavingsTimeZone"
    site_uses_daylight_savings_time: bool


@dataclass
class DefaultTimeZone:
    """
    DefaultTimeZone dataclass

    """

    zone_offset: str
    zone_abbreviation: str


@dataclass
class DaylightSavingsTimeZone:
    """
    DaylightSavingsTimeZone dataclass

    """

    zone_offset: str
    zone_abbreviation: str


@dataclass
class GeoLocation:
    """
    GeoLocation dataclass

    """

    geog_location: "GeogLocation"
    local_site_xy: str


@dataclass
class GeogLocation:
    """
    GeogLocation dataclass

    """

    srs: str
    latitude: float
    longitude: float


@dataclass
class SiteProperty:
    """
    SiteProperty dataclass

    """

    value: Union[str, date, int]
    name: str


@dataclass
class Variable:
    """
    Variable dataclass

    """

    variable_code: List["VariableCode"]
    variable_name: str
    variable_description: str
    value_type: str
    unit: "Unit"
    options: "Options"
    note: str
    no_data_value: float
    variable_property: str
    oid: Union[int, str]


@dataclass
class VariableCode:
    """
    VariableCode dataclass

    """

    value: Union[int, str]
    network: str
    vocabulary: str
    variable_id: int
    default: bool


@dataclass
class Unit:
    """
    Unit dataclass

    """

    unit_code: str


@dataclass
class Options:
    """
    Options dataclass

    """

    option: List["Option"]


@dataclass
class Option:
    """
    Option dataclass

    """

    value: str
    name: str
    option_code: Union[int, str]


@dataclass
class Values:
    """
    Value dataclass

    """

    # value: List['Value']
    qualifier: List["Qualifier"]
    quality_control_level: str
    method: List["Method"]
    source: str
    offset: str
    sample: str
    censor_code: str


# @dataclass
# class Value:
#     """
#     Value dataclass

#     """
#     value: Union[float, str]
#     qualifiers: List[str]
#     date_time: datetime


@dataclass
class Qualifier:
    """
    Qualifier dataclass

    """

    qualifier_code: str
    qualifier_description: str
    qualifier_id: int
    network: str
    vocabulary: str


@dataclass
class Method:
    """
    Method dataclass

    """

    method_description: str
    method_id: int
