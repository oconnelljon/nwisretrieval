import pytest
import numpy as np
import pandas as pd
import pandas.testing as pd_testing
from nwisretrieval.nwisretrieval import NWISFrame
from unittest.mock import patch
from rich import print


@pytest.fixture
def dummy_station_info(approval_flag: str, qualifier_flag: str) -> dict:
    """Generates dictionary of dummy station metadata.

    Parameters
    ----------
    approval_flag : str
        "Provisional" or "Approval
    qualifier_flag : str
        "Ice" or None

    Returns
    -------
    dict
        Dictionary of dummy station metadata
    """
    return {
        "query_url": "url",
        "site_name": "siteName",
        "dec_lat": "latitude",
        "dec_long": "longitude",
        "va_description": "variableDescription",
        "approval_flag": "Provisional",
        "qualifier_flag": "Ice",
    }


@pytest.fixture
def prov_nogap_qual():
    # provisonal data with no gaps but has qualifiers.
    test_data = pd.DataFrame(
        {
            "datetime": ["2023-01-03 16:45:00", "2023-01-03 17:00:00", "2023-01-03 17:15:00"],
            "value": [2, -999999, -999999],
            "qualifiers": [["P"], ["P", "Ice"], ["P", "Ice"]],
        }
    )
    return test_data.set_index("datetime")


@pytest.fixture
def prov_nogap_qual_expected():
    # provisonal data with no gaps but has qualifiers
    test_data = pd.DataFrame(
        {
            "datetime": ["2023-01-03 16:45:00", "2023-01-03 17:00:00", "2023-01-03 17:15:00"],
            "value": [2, np.NaN, np.NaN],
            "qualifiers": [["P"], ["P", "Ice"], ["P", "Ice"]],
        }
    )
    return test_data.set_index("datetime")


@pytest.fixture
def prov_gap_noqual():
    # provisonal data with gaps but no qualifiers
    # df = pd.read_csv("test_data/test_data_provisional_no-gap_ICE-quals_12301250_20230104-20230105.csv")
    # df["datetime"] = pd.to_datetime(df["datetime"])
    test_data = pd.DataFrame(
        {
            "datetime": ["2023-01-03 16:30:00", "2023-01-03 17:00:00", "2023-01-03 17:45:00"],
            "value": [1630, 1700, 1745],
            "qualifiers": [["P"], ["P"], ["P"]],
        }
    )
    return test_data.set_index("datetime")


@pytest.fixture
def prov_gap_noqual_expected():
    expected_data = pd.DataFrame(
        {
            "datetime": ["2023-01-03 16:30:00", "2023-01-03 16:45:00", "2023-01-03 17:00:00", "2023-01-03 17:15:00", "2023-01-03 17:30:00", "2023-01-03 17:45:00"],
            "value": [1630, np.NaN, 1700, np.NaN, np.NaN, 1745],
            "qualifiers": [["P"], ["P"], ["P"], ["P"], ["P"], ["P"]],
        }
    )
    return expected_data.set_index("datetime")


@patch("nwisretrieval.nwisretrieval.NWISFrame.getNWIS")
def test_resolve_masks(mock_getNWIS, prov_nogap_qual, prov_nogap_qual_expected, dummy_station_info):
    mock_getNWIS.return_value = (prov_nogap_qual, dummy_station_info)
    nwis = NWISFrame(STAID="12301933", start_date="2023-01-03", end_date="2023-01-04", param="63680")
    nwis.resolve_masks()
    pd_testing.assert_frame_equal(nwis.data, prov_nogap_qual_expected)


@patch("nwisretrieval.nwisretrieval.NWISFrame.getNWIS")
def test_check_quals(mock_getNWIS, prov_nogap_qual, prov_nogap_qual_expected, dummy_station_info):
    mock_getNWIS.return_value = (prov_nogap_qual, dummy_station_info)
    nwis = NWISFrame(STAID="12301933", start_date="2023-01-03", end_date="2023-01-04", param="63680")
    nwis.resolve_masks()
    pd_testing.assert_frame_equal(nwis.data, prov_nogap_qual_expected)
