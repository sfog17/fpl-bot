from fpl.extract.bootstrap import get_week_info
import pandas as pd
pd.options.display.max_columns = 20
pd.options.display.width = 300


def test_print():
    print(get_week_info('test-resources/example_bootstrap-static.json'))