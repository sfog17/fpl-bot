import pathlib

# Structure Data
DIR_DATA = pathlib.Path(__file__).parent.parent.parent.joinpath('data')

# Data - Raw
DIR_RAW_PLAYER_DETAILS = DIR_DATA.joinpath('raw', 'player-details')
DIR_RAW_BOOTSTRAP = DIR_DATA.joinpath('raw', 'bootstrap')
DIR_RAW_FIXTURES = DIR_DATA.joinpath('raw', 'fixtures')
DIR_MANAGER_HISTORY = DIR_DATA.joinpath('raw', 'manager')

FILE_INTER_BOOTSTRAP = DIR_DATA.joinpath('intermediate', 'bootstrap.csv')
FILE_INTER_HISTORICAL = DIR_DATA.joinpath('intermediate', 'player-details.csv')

FILE_PROC_FEATURES = DIR_DATA.joinpath('processed', 'features.csv')

FILE_PREDICTIONS = DIR_DATA.joinpath('results', 'predictions.csv')