import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import fpl.constants.fields as fld
from fpl.extract.historical import add_avg_prev_season, TOTAL_POINTS
pd.options.display.max_columns = 5


def test_simple():
    df_simple = pd.DataFrame({
        fld.PLAYER_ID: [100, 100, 100],
        fld.SEASON_ID: [9, 10, 11],
        TOTAL_POINTS: [20, 50, 100]
    })
    df_avg = pd.DataFrame({fld.STAT_POINTS_AVG_SEASON_PREV: [np.nan, 0.53, 1.32]})
    df_result = add_avg_prev_season(df_simple)
    df_expected = pd.concat([df_simple, df_avg], axis=1)

    assert_frame_equal(df_result, df_expected)
