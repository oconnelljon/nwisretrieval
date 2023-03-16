import pandas as pd

elevation_df = pd.DataFrame(
    {
        "elevation": [1000, 2000, 3000, 4000, 5000, 10000],
        "coeff_a": [1, 2, 3, 4, 5, 10],
        "coeff_b": [10, 20, 30, 40, 50, 100],
        "coeff_c": [100, 200, 300, 400, 500, 1000],
    },
)

disharge_df = pd.DataFrame(
    {
        "elevation": [500, 2000, 3001, 3999, 4001, 4002, 5001, 7002],
    },
)

df3 = pd.merge_asof(disharge_df, elevation_df, on="elevation", direction="forward")
