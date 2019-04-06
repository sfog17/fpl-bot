import logging
import fpl.collect.download
import fpl.extract.bootstrap
import fpl.extract.historical
import fpl.transform.features
import fpl.model.predict

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # fpl.collect.download.get_historical('fpl_api_historical')
    # fpl.collect.download.get_rankings('rankings_test', 10)
    # fpl.extract.historical.run()
    # fpl.transform.features.run(reload_data=False)
    fpl.model.predict.run(reload_data=True)
