import pandas as pd 
import typing 
import requests
import datetime
import logging 
import logging.config
import time 
import json 
import os

logger = logging.getLogger(__name__)
from settings import LOGGING_CONFIG


BASE_URL_CHARGECLOUD = "https://new-poi.chargecloud.de"
SCRAPING_INTERVAL = 10
CITIES_CC = ['koeln','dortmund','bonn','muenster','kiel',
            'chemnitz','krefeld','leverkusen','heidelberg',
            'solingen','ingolstadt','pforzheim','goettingen',
            'erlangen','tuebingen','bayreuth','bocholt','dormagen',
            'rastatt','hof','weinheim','bruchsal','nettetal','ansbach',
            'schwabach','ettlingen','crailsheim','deggendorf','forchheim',
            'bretten','buehl','zirndorf','roth','calw','herzogenaurach','wertheim',
            'kitzingen','lichtenfels']


DIR_SAVE_API_RESULTS =  "../data/scraped_data"
SAVE_RAW = True


def scrape_cp_cities(cities: typing.List[str], 
                     dir_save: str, 
                     save_raw: bool = False): 
    """Scrape charging point information for a given list of cities

    Args:
        cities (typing.List[str], optional): list of cities to scrape.
        dir_save (str): directory for saving scraped cities. 
        save_raw (bool, optional): whether to save API result as raw json (True) or pickle file (False). Defaults to False.
    """    
    data_cities = {}
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")        

    for city in cities: 
        r = requests.get(BASE_URL_CHARGECLOUD + "/" + city)
        try: 
            data = json.loads(r.text)
            data_cities[city] = data
            logger.debug(f"Successfully scraped '{city}' at {now}.")
        except: 
            logger.error(f"Error occurred while scraping '{city}' at {now}.")

    if save_raw: 
        fname = now + "_cp_data_cities.json"
        path_save = os.path.join(dir_save, fname)
        with open(path_save, 'w', encoding='utf-8') as f:
            json.dump(data_cities, f)
    else: 
        fname = now + "_cp_data_cities.pkl"
        path_save = os.path.join(dir_save, fname)
        pd.to_pickle(data_cities, path_save)
	

def call_chargecloud_api(scraping_interval: typing.Union[int, float] = SCRAPING_INTERVAL, 
                         cities: typing.List[str] = CITIES_CC, 
                         dir_save_api_results: str = DIR_SAVE_API_RESULTS, 
                         save_raw: str = SAVE_RAW): 
    """Scrape a given list of cities from chargecloud API in a given interval.

    Args:
        scraping_interval (typing.Union[int, float], optional): interval in minutes between API lookups. Defaults to SCRAPING_INTERVAL.
        cities (typing.List[str], optional): list of cities to scrape
        dir_save (str, optional): directory for saving scraped cities. Defaults to "../data/scraped_data".
        save_raw (bool, optional): whether to save API result as raw json (True) or pickle file (False). Defaults to False.

    """    
    while True: 
        
        now = datetime.datetime.now().strftime("%Y/%m/%d_%H:%M:%S")
        info_msg = f"Scraping cities: {now}"
        logger.info(info_msg)
        
        scrape_cp_cities(cities=cities, dir_save=dir_save_api_results, save_raw=save_raw)
        
        # API call of all cities takes approx. 15 seconds, therefore subtract it from specified interval
        sleep = max(scraping_interval*60 - 15, 0)
        time.sleep(sleep)


if __name__ == '__main__':
    logging.config.dictConfig(LOGGING_CONFIG)
    call_chargecloud_api()