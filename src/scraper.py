import requests
import datetime 
import json 
import time 
import logging
import typing 
import os
import pandas as pd

logger = logging.getLogger(__name__)

BASE_URL_CHARGECLOUD = "https://new-poi.chargecloud.de"
SCRAPING_INTERVAL = 10
CITIES = ['koeln','dortmund','bonn','muenster','kiel',
          'chemnitz','krefeld','leverkusen','heidelberg',
          'solingen','ingolstadt','pforzheim','goettingen',
          'erlangen','tuebingen','bayreuth','bocholt','dormagen',
          'rastatt','hof','weinheim','bruchsal','nettetal','ansbach',
          'schwabach','ettlingen','crailsheim','deggendorf','forchheim',
          'bretten','buehl','zirndorf','roth','calw','herzogenaurach','wertheim',
          'kitzingen','lichtenfels']


def scrape_cp_cities(cities: typing.List[str], 
                     dir_save: str = "../data/scraped_data", 
                     save_raw: bool = False): 
    """Scrape charging point information for a given list of cities

    Args:
        cities (typing.List[str], optional): list of cities to scrape.
        dir_save (str, optional): directory for saving scraped cities. Defaults to "../data/scraped_data".
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
            logger.error(f"Error occured while scraping '{city}' at {now}.")

    if save_raw: 
        #TODO: Implement saving raw jsons
        pass 
    else: 
        fname = now + "_cp_data_cities.pkl"
        path_save = os.path.join(dir_save, fname)
        pd.to_pickle(data_cities, path_save)
	

def scraper(scraping_interval: typing.Union[int, float] = SCRAPING_INTERVAL, 
            cities: typing.List[str] = CITIES): 
    """Scrape a given list of cities from chargecloud API in a given interval.

    Args:
        scraping_interval (typing.Union[int, float], optional): Interval in minutes between API lookups. Defaults to SCRAPING_INTERVAL.
    """    
    while True: 
        
        now = datetime.datetime.now().strftime("%Y/%m/%d_%H:%M:%S")
        info_msg = f"Scraping cities: {now}"
        logger.info(info_msg)
        print(info_msg)
        
        scrape_cp_cities(cities=cities)
        
        time.sleep(scraping_interval*60 - 15)