import pandas as pd


class WrappedDataFrame:
    _metadata = ["_metadict"]

    def __init__(self, df):
        self._df = df
        self._metadict = {
            "_STAID": "STAID",
            "_start_date": "start_date",
            "_end_date": "end_date",
        }

    @property
    def _constructor(self):
        return WrappedDataFrame

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return getattr(self, attr)
        return getattr(self._df, attr)

    def __getitem__(self, item):
        return self._df[item]

    def __setitem__(self, item, data):
        self._df[item] = data


dataframe = pd.DataFrame(
    {
        "dateTime": ["2023-01-03 16:45:00", "2023-01-03 17:00:00", "2023-01-03 17:30:00"],
        "value": [2, -999999, -999999],
        "qualifiers": [["P"], ["P", "Ice"], ["P", "Ice"]],
    }
)
dataframe["dateTime"] = pd.to_datetime(dataframe["dateTime"].array, infer_datetime_format=True)
dataframe.set_index("dateTime", inplace=True)


new_df = WrappedDataFrame(dataframe)
new_df._metadict.update({"A": "B"})
remix = new_df.asfreq(freq="15min")
pause = 2
