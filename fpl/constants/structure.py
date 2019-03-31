import pathlib

# Structure Data
DIR_DATA = pathlib.Path(__file__).parent.parent.parent.joinpath('data')

DIR_RAW_HIST = DIR_DATA.joinpath('raw', 'historical')
DIR_RAW_BOOTSTRAP = DIR_DATA.joinpath('raw', 'bootstrap')
DIR_RANKINGS = DIR_DATA.joinpath('raw', 'rankings')

FILE_INTER_BOOTSTRAP = DIR_DATA.joinpath('intermediate', 'bootstrap.csv')
FILE_INTER_HISTORICAL = DIR_DATA.joinpath('intermediate', 'historical.csv')

FILE_PROC_FEATURES = DIR_DATA.joinpath('processed', 'features.csv')

DIR_DATA_RESULTS = DIR_DATA.joinpath('results')

# URL Scrape
URL_BASE = 'https://fantasy.premierleague.com/drf/'
URL_BOOTSTRAP = URL_BASE + 'bootstrap-static'
URL_PLAYERS = URL_BASE + 'element-summary'
