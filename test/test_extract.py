from pathlib import Path
from fpl.extract.bootstrap import get_week_info
import pandas as pd
pd.options.display.max_columns = 20
pd.options.display.width = 300


def test_print():
    print(get_week_info(Path(__file__).parent.joinpath('test-resources/example_bootstrap-static.json')))


# past points and minutes initialised at 0
# Check no empty "chance of playing"
# check points diff betzeen last and current event
