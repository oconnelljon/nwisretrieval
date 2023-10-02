import json
from pathlib import Path

import numpy as np
import pandas as pd
import pandas.testing as pd_testing
import pytest
from dataclass_wizard import JSONWizard
from rich import print

import nwisretrieval.nwisretrieval as nwis

# from unittest.mock import patch
# from nwisretrieval.nwisretrieval import NWISJson


@pytest.fixture
def requests_json_return_data():
    with open(
        Path(
            "tests/test_data/get_nwis_site=12340500_service=dv_parameterCd=00060_startDT=20230101_endDt=20230401_format=json.json"
        ),
        mode="r",
        encoding="utf-8",
    ) as f:
        return json.load(f)


@pytest.fixture
def dummy_station_ice_provisional_15min_nogap():  # approval_flag: str, qualifier_flag: str
    """Generates dictionary of dummy station metadata.
    Returns
    -------
    dict
        Dictionary of dummy station metadata
    """
    dataframe = pd.DataFrame(
        {
            "dateTime": ["2023-01-03 16:45:00", "2023-01-03 17:00:00", "2023-01-03 17:15:00"],
            "value": [2, -999999, -999999],
            "qualifiers": [["P"], ["P", "Ice"], ["P", "Ice"]],
        }
    )
    dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"].array, infer_datetime_format=True)
    dataframe.set_index("dateTime", inplace=True)

    metadata = {
        "_STAID": "12301933",
        "_start_date": "2023-01-03",
        "_end_date": "2023-01-03",
        "_parame": "00060",
        "_stat_code": "stat_code",
        "_service": "service",
        "_access_level": "0",
        "_url": "url",
        "_gap_tolerance": "15min",
        "_gap_fill": False,
        "_resolve_masking": False,
        "_gap_flag": "unknown",
        "_approval": "Provisional",
        "_mask_flag": "unknown",
        "_site_name": "Libby Dam dummy site",
        "_coords": (
            "1234",
            "1234",
        ),
        "_var_description": "Var description",
    }

    # Wrap pandas dataframe with custom NWISFrame class and assign metadata to _metadict.
    dataframe = nwis.NWISFrame(dataframe)
    dataframe._metadict = metadata
    return dataframe


@pytest.fixture
def dummy_station_ice_provisional_1Day_gap():  # approval_flag: str, qualifier_flag: str
    """Generates dummy station. Qualifier = Ice, approval = Provisional, gaps = True, gap_tol = 1 day"""
    dataframe = pd.DataFrame(
        {
            "dateTime": ["2023-01-03", "2023-01-05", "2023-01-06"],
            "value": [3, 5, 6],
            "qualifiers": [["P"], ["P", "Ice"], ["P", "Ice"]],
        }
    )
    dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"].array, infer_datetime_format=True)
    dataframe.set_index("dateTime", inplace=True)

    metadata = {
        "_STAID": "12301933",
        "_start_date": "2023-01-03",
        "_end_date": "2023-01-06",
        "_parame": "00060",
        "_stat_code": "stat_code",
        "_service": "service",
        "_access_level": "0",
        "_url": "url",
        "_gap_tolerance": "D",
        "_gap_fill": False,
        "_resolve_masking": False,
        "_gap_flag": "unknown",
        "_approval": "Provisional",
        "_mask_flag": "unknown",
        "_site_name": "Libby Dam dummy site",
        "_coords": (
            "1234",
            "1234",
        ),
        "_var_description": "Var description",
    }

    # Wrap pandas dataframe with custom NWISFrame class and assign metadata to _metadict.
    dataframe = nwis.NWISFrame(dataframe)
    dataframe._metadict = metadata
    return dataframe


@pytest.fixture
def dummy_station_ice_provisional_1Day_nogap():  # approval_flag: str, qualifier_flag: str
    """Generates dummy station. Qualifier = Ice, approval = Provisional, gaps = True, gap_tol = 1 day"""
    dataframe = pd.DataFrame(
        {
            "dateTime": ["2023-01-03", "2023-01-04", "2023-01-05", "2023-01-06"],
            "value": [3, 4, 5, 6],
            "qualifiers": [["P"], ["P"], ["P", "Ice"], ["P", "Ice"]],
        }
    )
    dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"].array, infer_datetime_format=True)
    dataframe.set_index("dateTime", inplace=True)

    metadata = {
        "_STAID": "12301933",
        "_start_date": "2023-01-03",
        "_end_date": "2023-01-06",
        "_parame": "00060",
        "_stat_code": "stat_code",
        "_service": "service",
        "_access_level": "0",
        "_url": "url",
        "_gap_tolerance": "D",
        "_gap_fill": False,
        "_resolve_masking": False,
        "_gap_flag": False,
        "_approval": "Provisional",
        "_mask_flag": "unknown",
        "_site_name": "Libby Dam dummy site",
        "_coords": (
            "1234",
            "1234",
        ),
        "_var_description": "Var description",
    }

    # Wrap pandas dataframe with custom NWISFrame class and assign metadata to _metadict.
    dataframe = nwis.NWISFrame(dataframe)
    dataframe._metadict = metadata
    return dataframe


@pytest.fixture
def dummy_station_ice_approved_1Day_nogap():  # approval_flag: str, qualifier_flag: str
    """Generates dummy station. Qualifier = Ice, approval = Provisional, gaps = True, gap_tol = 1 day"""
    dataframe = pd.DataFrame(
        {
            "dateTime": ["2023-01-03", "2023-01-04", "2023-01-05", "2023-01-06"],
            "value": [3, 4, 5, 6],
            "qualifiers": [["A"], ["A"], ["A", "Ice"], ["A", "Ice"]],
        }
    )
    dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"].array, infer_datetime_format=True)
    dataframe.set_index("dateTime", inplace=True)

    metadata = {
        "_STAID": "12301933",
        "_start_date": "2023-01-03",
        "_end_date": "2023-01-06",
        "_parame": "00060",
        "_stat_code": "stat_code",
        "_service": "service",
        "_access_level": "0",
        "_url": "url",
        "_gap_tolerance": "D",
        "_gap_fill": False,
        "_resolve_masking": False,
        "_gap_flag": False,
        "_approval": "Provisional",
        "_mask_flag": "unknown",
        "_site_name": "Libby Dam dummy site",
        "_coords": (
            "1234",
            "1234",
        ),
        "_var_description": "Var description",
    }

    # Wrap pandas dataframe with custom NWISFrame class and assign metadata to _metadict.
    dataframe = nwis.NWISFrame(dataframe)
    dataframe._metadict = metadata
    return dataframe


@pytest.fixture
def prov_nogap_qual():
    # provisonal data with no gaps but has qualifiers.
    dataframe = pd.DataFrame(
        {
            "dateTime": ["2023-01-03 16:45:00", "2023-01-03 17:00:00", "2023-01-03 17:15:00"],
            "value": [2, -999999, -999999],
            "qualifiers": [["P"], ["P", "Ice"], ["P", "Ice"]],
        }
    )
    dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"].array, infer_datetime_format=True)
    dataframe.set_index("dateTime", inplace=True)
    return dataframe


@pytest.fixture
def expected_ice_provisional_15min_nogap():
    # provisonal data with no gaps but has qualifiers
    dataframe = pd.DataFrame(
        {
            "dateTime": ["2023-01-03 16:45:00", "2023-01-03 17:00:00", "2023-01-03 17:15:00"],
            "value": [2, np.NaN, np.NaN],
            "qualifiers": [["P"], ["P", "Ice"], ["P", "Ice"]],
        }
    )
    dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"].array, infer_datetime_format=True)
    dataframe.set_index("dateTime", inplace=True)
    return dataframe


@pytest.fixture
def prov_gap_noqual():
    # provisonal data with gaps but no qualifiers
    # df = pd.read_csv("test_data/test_data_provisional_no-gap_ICE-quals_12301250_20230104-20230105.csv")
    # df["datetime"] = pd.to_datetime(df["datetime"])
    dataframe = pd.DataFrame(
        {
            "dateTime": ["2023-01-03 16:30:00", "2023-01-03 17:00:00", "2023-01-03 17:45:00"],
            "value": [1630, 1700, 1745],
            "qualifiers": [["P"], ["P"], ["P"]],
        }
    )
    dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"].array, infer_datetime_format=True)
    dataframe.set_index("dateTime", inplace=True)
    return dataframe


@pytest.fixture
def prov_gap_noqual_expected():
    dataframe = pd.DataFrame(
        {
            "dateTime": [
                "2023-01-03 16:30:00",
                "2023-01-03 16:45:00",
                "2023-01-03 17:00:00",
                "2023-01-03 17:15:00",
                "2023-01-03 17:30:00",
                "2023-01-03 17:45:00",
            ],
            "value": [1630, np.NaN, 1700, np.NaN, np.NaN, 1745],
            "qualifiers": [["P"], ["P"], ["P"], ["P"], ["P"], ["P"]],
        }
    )
    dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"].array, infer_datetime_format=True)
    dataframe.set_index("dateTime", inplace=True)
    return dataframe


def test_resolve_masks(
    dummy_station_ice_provisional_15min_nogap, expected_ice_provisional_15min_nogap
):
    dummy_station_ice_provisional_15min_nogap.resolve_masks()
    pd_testing.assert_frame_equal(
        dummy_station_ice_provisional_15min_nogap, expected_ice_provisional_15min_nogap
    )


def test_fill_gaps(dummy_station_ice_provisional_1Day_gap):
    expected_index = pd.DatetimeIndex(
        ["2023-01-03", "2023-01-04", "2023-01-05", "2023-01-06"], name="dateTime", freq="D"
    )
    dummy_station_ice_provisional_1Day_gap = dummy_station_ice_provisional_1Day_gap.fill_gaps()
    assert all(dummy_station_ice_provisional_1Day_gap.index == expected_index)


def test_check_quals(dummy_station_ice_provisional_1Day_gap):
    assert dummy_station_ice_provisional_1Day_gap.qualifier == "Ice"


# def test_property_gap_flag(dummy_station_ice_provisional_1Day_gap):
#     assert dummy_station_ice_provisional_1Day_gap.gap_flag is True
#     dummy_station_ice_provisional_1Day_gap = dummy_station_ice_provisional_1Day_gap.fill_gaps()
#     assert dummy_station_ice_provisional_1Day_gap.gap_flag is False


def test_check_gaps(
    dummy_station_ice_provisional_1Day_gap, dummy_station_ice_provisional_1Day_nogap
):
    assert dummy_station_ice_provisional_1Day_gap.check_gaps("D") is True
    assert dummy_station_ice_provisional_1Day_nogap.check_gaps("D") is False


def test_check_approval(
    dummy_station_ice_provisional_1Day_nogap, dummy_station_ice_approved_1Day_nogap
):
    assert dummy_station_ice_provisional_1Day_nogap.check_approval() == "Provisional"
    assert dummy_station_ice_approved_1Day_nogap.check_approval() == "Approved"


def test_STAID(dummy_station_ice_provisional_1Day_gap):
    assert (
        dummy_station_ice_provisional_1Day_gap._metadict.get("_STAID")
        == dummy_station_ice_provisional_1Day_gap.STAID
    )


def test_start_date(dummy_station_ice_provisional_1Day_gap):
    assert (
        str(dummy_station_ice_provisional_1Day_gap._metadict.get("_start_date"))
        == dummy_station_ice_provisional_1Day_gap.start_date
    )


def test_end_date(dummy_station_ice_provisional_1Day_gap):
    assert (
        str(dummy_station_ice_provisional_1Day_gap._metadict.get("_end_date"))
        == dummy_station_ice_provisional_1Day_gap.end_date
    )


def test_param(dummy_station_ice_provisional_1Day_gap):
    assert (
        dummy_station_ice_provisional_1Day_gap._metadict.get("_param")
        == dummy_station_ice_provisional_1Day_gap.param
    )


def test_stat_code(dummy_station_ice_provisional_1Day_gap):
    assert (
        dummy_station_ice_provisional_1Day_gap._metadict.get("_stat_code")
        == dummy_station_ice_provisional_1Day_gap.stat_code
    )


def test_service(dummy_station_ice_provisional_1Day_gap):
    assert (
        dummy_station_ice_provisional_1Day_gap._metadict.get("_service")
        == dummy_station_ice_provisional_1Day_gap.service
    )


def test_url(dummy_station_ice_provisional_1Day_gap):
    assert (
        dummy_station_ice_provisional_1Day_gap._metadict.get("_url")
        == dummy_station_ice_provisional_1Day_gap.url
    )


def test_site_name(dummy_station_ice_provisional_1Day_gap):
    assert (
        dummy_station_ice_provisional_1Day_gap._metadict.get("_site_name")
        == dummy_station_ice_provisional_1Day_gap.site_name
    )


def test_coords(dummy_station_ice_provisional_1Day_gap):
    assert (
        dummy_station_ice_provisional_1Day_gap._metadict.get("_coords")
        == dummy_station_ice_provisional_1Day_gap.coords
    )


def test_var_description(dummy_station_ice_provisional_1Day_gap):
    assert (
        dummy_station_ice_provisional_1Day_gap._metadict.get("_var_description")
        == dummy_station_ice_provisional_1Day_gap.var_description
    )


def test_gap_tolerance(dummy_station_ice_provisional_1Day_gap):
    assert (
        str(dummy_station_ice_provisional_1Day_gap._metadict.get("_gap_tolerance"))
        == dummy_station_ice_provisional_1Day_gap.gap_tolerance
    )


def test_nwisframe_validate_kwargs():
    input_regular_call = dict(
        sites="12340500",
        parameterCd="00060",
        startDT="2023-01-01",
        endDT="2023-04-01",
        format="json",
    )
    expected_regular_call = dict(
        sites="12340500",
        parameterCd="00060",
        startDT="2023-01-01",
        endDT="2023-04-01",
        format="json",
    )
    input_none_in_query = dict(
        sites="12340500",
        startDT="2023-01-01",
        parameterCd=None,
        endDT=None,
    )
    expected_none_in_query = dict(
        sites="12340500",
        startDT="2023-01-01",
    )
    input_none_and_bad_parameters_in_query = dict(
        sites="12340500",
        format="json",
        mouse="2023-01-01",
        parameterCd=None,
        house=None,
    )
    expected_none_and_bad_parameters_in_query = dict(
        sites="12340500",
        format="json",
        mouse="2023-01-01",
    )
    input_only_none_values = dict(
        sites=None,
        startDT=None,
        mouse=None,
    )
    expected_only_none_values = {}

    output_only_none_values = nwis.NWISFrame._remove_nones(**input_only_none_values)
    output_data_regular_call = nwis.NWISFrame._remove_nones(**input_regular_call)
    output_data_none_in_query = nwis.NWISFrame._remove_nones(**input_none_in_query)
    output_data_none_and_bad_parameters_in_query = nwis.NWISFrame._remove_nones(
        **input_none_and_bad_parameters_in_query
    )
    assert expected_regular_call == output_data_regular_call
    assert expected_none_in_query == output_data_none_in_query
    assert expected_none_and_bad_parameters_in_query == output_data_none_and_bad_parameters_in_query
    assert expected_only_none_values == output_only_none_values


def test_nwisframe_getnwis():
    data = nwis.NWISFrame.get_nwis(
        sites="12340500",
        service="dv",
        parameterCd="00060",
        startDT="2023-01-01",
        endDT="2023-04-01",
        format="json",
    )
    return


def test_nwisframe__merge_kwargs():
    input_norm = {
        "format": "json",
        "sites": "12323233",
        "startDT": "2022-07-01",
        "endDT": "2022-08-01",
        "statCd": "00003",
        "parameterCd": "00060",
        "service": "dv",
    }
    input_data_normal = {
        "format": "json",
        "sites": "12340500",
        "startDT": "2022-01-01",
        "endDT": "2022-05-01",
        "statCd": "00003",
        "parameterCd": "00060",
        "service": "dv",
    }
    expected_data_normal = {
        "format": "json",
        "sites": "12340500",
        "startDT": "2022-01-01",
        "endDT": "2022-05-1",
        "statCd": "00003",
        "parameterCd": "00060",
        "service": "dv",
    }
    output_data_normal = nwis.NWISFrame._merge_kwargs(input_data_normal)
    assert expected_data_normal == output_data_normal


def test_nwis_item_generator(requests_json_return_data):
    output_queryURL = next(
        nwis.NWISFrame.item_generator(data=requests_json_return_data, key="queryURL")
    )
    output_siteName = next(
        nwis.NWISFrame.item_generator(data=requests_json_return_data, key="siteName")
    )
    assert (
        output_queryURL
        == "http://nwis.waterservices.usgs.gov/nwis/dv/format=json&sites=12323233&startDT=2022-07-01&endDT=2022-08-01&statCd=00003&parameterCd=00060"
    )
    assert output_siteName == "Blacktail Creek above Grove Gulch, at Butte, MT"


def test_experiment():
    string = """
    {
      "my_str": 20,
      "ListOFINT": ["1", "2", 3],
      "isActiveTupleee": ["true", false, 1]
    }
    """
    string2 = """
    {
      "my_str": 20,
      "ListOFINT": ["1", "2", {"alpha": "bravo"}],
      "isActiveTupleee": ["true", false, 1]
    }
    """

    from dataclasses import dataclass, field
    from dataclass_wizard import JSONWizard, fromdict, LoadMeta
    from typing import Dict

    # @dataclass
    # class NWISJasn(JSONWizard):
    #     my_str: str | None
    #     is_active_tupleee: tuple[bool, ...]
    #     list_of_int: list[int] = field(default_factory=list)

    @dataclass
    class QueryInfo:
        query_info: dict
        # query_url: str
        # criteria: dict[str, str, str, str]
        # note: list

    @dataclass
    class TimeSeries:
        TimeSeries: list

    # instance1 = NWISJasn.from_json(string2)
    from typing import List

    @dataclass
    class NWISHelper:
        a: str

    @dataclass
    class NWISjson:
        name: str
        declaredType: List["NWISHelper"]
        scope: str
        value: str
        nil: str
        globalScope: str
        typeSubstituted: str

    LoadMeta(key_transform="CAMEL").bind_to(NWISjson)
    with open(
        "tests/test_data/get_nwis_site=12340500_service=dv_parameterCd=00060_startDT=20230101_endDt=20230401_format=json.json",
        "r",
        encoding="utf-8",
    ) as f:
        jdata = json.load(f)
        mouse = {
            "name": "str",
            "declaredType": {"a": "b"},
            "scope": "str",
            "value": "Dict",
            "nil": "bool",
            "globalScope": "bool",
            "typeSubstituted": "bool",
        }
        data = fromdict(cls=NWISjson, d=mouse)
    return
    # instance2 = NWISJasn.from_json(string2)
    # assert instance1 == instance2


if __name__ == "__main__":
    string = """
    {
      "my_str": 20,
      "ListOFINT": ["1", "2", 3],
      "isActiveTupleee": ["true", false, 1]
    }
    """
    string2 = """
    {
      "my_str": 20,
      "ListOFINT": ["1", "2", {"alpha": "bravo"}],
      "isActiveTupleee": ["true", false, 1]
    }
    """

    from dataclasses import dataclass, field
    from dataclass_wizard import JSONWizard, fromdict, LoadMeta
    from typing import Dict

    # @dataclass
    # class NWISJasn(JSONWizard):
    #     my_str: str | None
    #     is_active_tupleee: tuple[bool, ...]
    #     list_of_int: list[int] = field(default_factory=list)
    from typing import List

    @dataclass
    class NWISObject:
        ...

    @dataclass
    class TimeParam(NWISObject):
        begin_date_time: str
        end_date_time: str

    @dataclass
    class Criteria(NWISObject):
        location_param: str
        variable_param: str
        time_param: TimeParam

    @dataclass
    class Note(NWISObject):
        value: str

    @dataclass
    class TimeRange(Note):
        ...

    @dataclass
    class MethodID(Note):
        ...

    @dataclass
    class RequestDT(Note):
        ...

    @dataclass
    class RequestID(Note):
        ...

    @dataclass
    class Disclaimer(Note):
        ...

    @dataclass
    class Server(Note):
        ...

    @dataclass
    class Sites(Note):
        ...

    @dataclass
    class QueryInfo(NWISObject):
        query_url: str
        criteria: Criteria
        note: list[Sites | TimeRange | MethodID | RequestDT | RequestID | Disclaimer | Server]

    @dataclass
    class Value(NWISObject):
        query_info: QueryInfo
        time_series: str

    @dataclass
    class NWISjson(NWISObject, JSONWizard):
        class _(JSONWizard.Meta):
            # Set tag key in JSON object; defaults to '__tag__' if not specified.
            tag_key = "title"
            auto_assign_tags = True

        name: str
        declared_type: str
        scope: str
        value: Value
        nil: str
        global_scope: str
        type_substituted: str


    # LoadMeta(key_transform="CAMEL").bind_to(NWISjson)
    # @dataclass
    # class NWIS(JSONWizard):
    #     name: str
    #     declared_type: str
    #     scope: str
    #     value: str  # Value
    #     nil: str
    #     global_scope: str
    #     type_substituted: str

    with open(
        "tests/test_data/get_nwis_site=12340500_service=dv_parameterCd=00060_startDT=20230101_endDt=20230401_format=json.json",
        "r",
        encoding="utf-8",
    ) as f:
        jdata = json.load(f)

        data = fromdict(cls=NWISjson, d=jdata)
        cat = 2

        mouse = {
            "levels": [
                {
                    "maps": {
                        "types": "SquareRoom",
                        "name": "Level 1",
                        "width": 100,
                        "height": 100,
                    },
                    "waves": [
                        {
                            "enemies": [
                                {
                                    "types": "Wizard",
                                    "name": "Gandalf",
                                },
                                {
                                    "types": "Archer",
                                    "name": "Legolass",
                                },
                            ]
                        }
                    ],
                }
            ]
        }
