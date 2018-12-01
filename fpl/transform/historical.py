import json
import logging
import pandas as pd
from fpl.config import DIR_RAW_HIST, FILE_INTER_HISTORICAL

HISTORY_PAST = 'history_past'  # History Previous Seasons

logger = logging.getLogger(__name__)

def transform_historical(dir_data_raw_hist):
    list_df = []
    for file_path in dir_data_raw_hist.glob('*.json'):
        logger.info(f'Processing file {file_path.name}')
        with open(file_path) as file_in:
            for line in file_in:
                player_json = json.loads(line)
                df_history = pd.DataFrame(player_json['history_past'])
                list_df.append(df_history)
    df_all_history = pd.concat(list_df)
    df_all_history_clean = df_all_history.drop_duplicates()
    return df_all_history_clean


def main():
    output_historical = transform_historical(DIR_RAW_HIST)
    output_historical.to_csv(FILE_INTER_HISTORICAL, index=False)