import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import constants.fields as fld
from pipeline.transform.clean_historical import add_avg_prev_season, TOTAL_POINTS
pd.options.display.max_columns = 5


def test_simple():
    df_input = pd.DataFrame({
        fld.PLAYER_ID: [100, 100, 100],
        fld.SEASON_ID: [9, 10, 11],
        TOTAL_POINTS: [20, 50, 100]
    })

    df_expected = pd.DataFrame({
        fld.PLAYER_ID: [100, 100, 100, 100],
        fld.SEASON_ID: [9, 10, 11, 12],
        TOTAL_POINTS: [20, 50, 100, np.nan],
        fld.STAT_POINTS_AVG_SEASON_PREV: [np.nan, 0.53, 1.32, 2.63]
    })

    df_result = add_avg_prev_season(df_input)

    assert_frame_equal(df_result, df_expected)
