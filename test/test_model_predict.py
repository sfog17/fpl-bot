import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import constants.fields as fld
from fpl.model.predict import predict_simple


def test_predict_simple():
    df_simple = pd.DataFrame({
        fld.RESULT_POINTS_PREV: [np.nan, np.nan, 10, 10],
        fld.STAT_POINTS_AVG_SEASON_PREV: [np.nan, 5, 5, 5]
    })

    df_expected = pd.DataFrame({
        fld.RESULT_POINTS_PREV: [np.nan, np.nan, 10, 10],
        fld.STAT_POINTS_AVG_SEASON_PREV: [np.nan, 5, 5, 5],
        fld.ML_PREDICT: [0.0, 5.0, 10.0, 10.0]
    })


    assert_frame_equal(predict_simple(df_simple), df_expected)


