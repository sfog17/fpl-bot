from pathlib import Path
import pandas as pd


def assert_df_csv_no_index(df: pd.DataFrame, csv_path: Path):
    """ Check that a pandas DataFrame output an expected CSV file """
    tmp_file = Path('') / 'temp.csv'
    df.to_csv(tmp_file, index=False)
    df_calculated = pd.read_csv(tmp_file)
    df_expected = pd.read_csv(csv_path)
    tmp_file.unlink()
    pd.testing.assert_frame_equal(df_calculated, df_expected)
