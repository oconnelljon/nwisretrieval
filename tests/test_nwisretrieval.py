import json
from pathlib import Path

import numpy as np
import pandas as pd
import pandas.testing as pd_testing
import pytest
from rich import print

from dataclass_wizard import fromdict
from nwisretrieval.nwis import NWISjson, NWISFrame


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


def test_experiment(requests_json_return_data):
    ts_data, meta = NWISFrame.process_nwis_response(requests_json_return_data)
    data = NWISFrame(ts_data, meta)
    pause = 2


if __name__ == "__main__":
    with open(
        "tests/test_data/get_nwis_site=12340500_service=dv_parameterCd=00060_startDT=20230101_endDt=20230401_format=json.json",
        "r",
        encoding="utf-8",
    ) as f:
        jdata = json.load(f)

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
