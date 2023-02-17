import pytest
import numpy as np
import pandas as pd
import pandas.testing as pd_testing
import nwisretrieval.nwisretrieval4 as nwis
from unittest.mock import patch
from rich import print


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
            "dateTime": ["2023-01-03 16:30:00", "2023-01-03 16:45:00", "2023-01-03 17:00:00", "2023-01-03 17:15:00", "2023-01-03 17:30:00", "2023-01-03 17:45:00"],
            "value": [1630, np.NaN, 1700, np.NaN, np.NaN, 1745],
            "qualifiers": [["P"], ["P"], ["P"], ["P"], ["P"], ["P"]],
        }
    )
    dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"].array, infer_datetime_format=True)
    dataframe.set_index("dateTime", inplace=True)
    return dataframe


def test_resolve_masks(dummy_station_ice_provisional_15min_nogap, expected_ice_provisional_15min_nogap):
    assert dummy_station_ice_provisional_15min_nogap._metadict["_mask_flag"] == "unknown"
    assert dummy_station_ice_provisional_15min_nogap.mask_flag == "unknown"
    dummy_station_ice_provisional_15min_nogap.resolve_masks()
    pd_testing.assert_frame_equal(dummy_station_ice_provisional_15min_nogap, expected_ice_provisional_15min_nogap)
    assert dummy_station_ice_provisional_15min_nogap._metadict["_mask_flag"] is True


def test_fill_gaps(dummy_station_ice_provisional_1Day_gap):
    expected_index = pd.DatetimeIndex(["2023-01-03", "2023-01-04", "2023-01-05", "2023-01-06"], name="dateTime", freq="D")
    dummy_station_ice_provisional_1Day_gap = dummy_station_ice_provisional_1Day_gap.fill_gaps()
    assert all(dummy_station_ice_provisional_1Day_gap.index == expected_index)


def test_check_quals(dummy_station_ice_provisional_1Day_gap):
    assert dummy_station_ice_provisional_1Day_gap.qualifier == "Ice"


def test_property_gap_flag(dummy_station_ice_provisional_1Day_gap):
    assert dummy_station_ice_provisional_1Day_gap.gap_flag is True
    dummy_station_ice_provisional_1Day_gap = dummy_station_ice_provisional_1Day_gap.fill_gaps()
    assert dummy_station_ice_provisional_1Day_gap.gap_flag is False


def test_check_gaps(dummy_station_ice_provisional_1Day_gap, dummy_station_ice_provisional_1Day_nogap):
    assert dummy_station_ice_provisional_1Day_gap.check_gaps("D") is True
    assert dummy_station_ice_provisional_1Day_nogap.check_gaps("D") is False


def test_STAID(dummy_station_ice_provisional_1Day_gap):
    assert dummy_station_ice_provisional_1Day_gap._metadict.get("_STAID") == dummy_station_ice_provisional_1Day_gap.STAID


def test_start_date(dummy_station_ice_provisional_1Day_gap):
    assert str(dummy_station_ice_provisional_1Day_gap._metadict.get("_start_date")) == dummy_station_ice_provisional_1Day_gap.start_date


def test_end_date(dummy_station_ice_provisional_1Day_gap):
    assert str(dummy_station_ice_provisional_1Day_gap._metadict.get("_end_date")) == dummy_station_ice_provisional_1Day_gap.end_date


def test_param(dummy_station_ice_provisional_1Day_gap):
    assert dummy_station_ice_provisional_1Day_gap._metadict.get("_param") == dummy_station_ice_provisional_1Day_gap.param


def test_stat_code(dummy_station_ice_provisional_1Day_gap):
    assert dummy_station_ice_provisional_1Day_gap._metadict.get("_stat_code") == dummy_station_ice_provisional_1Day_gap.stat_code


def test_service(dummy_station_ice_provisional_1Day_gap):
    assert dummy_station_ice_provisional_1Day_gap._metadict.get("_service") == dummy_station_ice_provisional_1Day_gap.service


def test_url(dummy_station_ice_provisional_1Day_gap):
    assert dummy_station_ice_provisional_1Day_gap._metadict.get("_url") == dummy_station_ice_provisional_1Day_gap.url


def test_site_name(dummy_station_ice_provisional_1Day_gap):
    assert dummy_station_ice_provisional_1Day_gap._metadict.get("_site_name") == dummy_station_ice_provisional_1Day_gap.site_name


def test_coords(dummy_station_ice_provisional_1Day_gap):
    assert dummy_station_ice_provisional_1Day_gap._metadict.get("_coords") == dummy_station_ice_provisional_1Day_gap.coords


def test_var_description(dummy_station_ice_provisional_1Day_gap):
    assert dummy_station_ice_provisional_1Day_gap._metadict.get("_var_description") == dummy_station_ice_provisional_1Day_gap.var_description


def test_gap_tolerance(dummy_station_ice_provisional_1Day_gap):
    assert str(dummy_station_ice_provisional_1Day_gap._metadict.get("_gap_tolerance")) == dummy_station_ice_provisional_1Day_gap.gap_tolerance


def test_mask_flag(dummy_station_ice_provisional_1Day_gap):
    assert dummy_station_ice_provisional_1Day_gap._metadict.get("_mask_flag") == dummy_station_ice_provisional_1Day_gap.mask_flag
