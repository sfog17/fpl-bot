import logging
import fpl.extract.bootstrap
import fpl.extract.historical

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # download.get_historical('fpl_api_historical')
    # download.get_rankings('rankings_test', 10)
    # fpl.extract.historical.run()
    fpl.extract.bootstrap.run()
    # fpl.transform.features.main()
