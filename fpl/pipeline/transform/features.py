import pandas as pd
import constants.fields as fld
from constants import FILE_INTER_HISTORICAL, FILE_INTER_BOOTSTRAP, FILE_PROC_FEATURES
import pipeline.transform.clean_bootstrap
import pipeline.transform.clean_historical


def prepare_features(df_bootstrap: pd.DataFrame, df_historical: pd.DataFrame):

    # Merge Bootstrap (current season) + Historical
    df_combined = df_bootstrap.merge(df_historical, how='left', on=[fld.PLAYER_ID, fld.SEASON_ID])

    # Create New Features - Other module

    # Select Fields

    return df_combined


def run(reload_data: bool):
    """ Merge datasets and create features
    
    Arguments:
        reload_data {bool} -- If True, process previous steps (extract_raw data)
    """
    if reload_data:
        pipeline.transform.clean_bootstrap.run()
        pipeline.transform.clean_historical.run()

    df_bootstrap = pd.read_csv(FILE_INTER_BOOTSTRAP, encoding='utf-8-sig')
    df_historical = pd.read_csv(FILE_INTER_HISTORICAL)

    df_combined = prepare_features(df_bootstrap, df_historical)
    df_combined.to_csv(FILE_PROC_FEATURES, index=False, encoding='utf-8-sig')
