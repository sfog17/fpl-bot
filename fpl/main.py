from fpl.utils import download
import fpl.transform.historical
import fpl.transform.bootstrap
import fpl.transform.features

if __name__ == '__main__':
    # download.get_historical('fpl_api_historical')
    # download.get_rankings('rankings_test', 10)
    # fpl.transform.historical.main()
    # fpl.transform.bootstrap.main()
    fpl.transform.features.main()