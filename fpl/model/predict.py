import numpy as np
import pandas as pd
import pipeline.transform.features
import constants.fields as fld
from constants import FILE_PROC_FEATURES, FILE_PREDICTIONS


def predict_simple(df: pd.DataFrame):
    # Use Last Game (or Last Season) to predict points for current gameweek)
    df[fld.ML_PREDICT] = np.where(
        df[fld.RESULT_POINTS_PREV].notnull(), 
        df[fld.RESULT_POINTS_PREV], 
        np.where(
            df[fld.STAT_POINTS_AVG_SEASON_PREV].notnull(),
            df[fld.STAT_POINTS_AVG_SEASON_PREV],
            0
        )
    )

    return df


def run(reload_data: bool):
    """ Produce a list of predictions for the dataset
    
    Arguments:
        reload_data {bool} -- If True, process previous steps (extract/transfom)
    """
    if reload_data:
        pipeline.transform.features.run(reload_data=True)

    df_features = pd.read_csv(FILE_PROC_FEATURES, encoding='utf-8-sig')

    df_combined = predict_simple(df_features)
    df_combined.to_csv(FILE_PREDICTIONS, index=False, encoding='utf-8-sig')
    return df_combined
