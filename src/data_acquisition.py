import requests
import datetime 
import json 
import time 
import logging
import typing 
import os
import pandas as pd
import osmnx as ox
from geopandas import GeoDataFrame


import logging.config
from settings import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)


logger = logging.getLogger(__name__)

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

# deal with city names containing Umlaute for Nominatim geocoder
CITIES_OSM = {**{city_cc: city_cc for city_cc in CITIES_CC}, 
              **{"koeln": "köln", "muenster": "Münster", 
                 "tuebingen": "Tübingen", "buehl": "Bühl", "goettingen": "Göttingen"}}

TAGS = {"amenity": ["supermarket", "fast_food", "highway_services", "fuel"], 
        "shop": ["mall", "doityourself"]}


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
                         dir_save_api_results: str = "../data/scraped_data"): 
    """Scrape a given list of cities from chargecloud API in a given interval.

    Args:
        scraping_interval (typing.Union[int, float], optional): interval in minutes between API lookups. Defaults to SCRAPING_INTERVAL.
        cities (typing.List[str], optional): list of cities to scrape
        dir_save (str, optional): directory for saving scraped cities. Defaults to "../data/scraped_data".

    """    
    while True: 
        
        now = datetime.datetime.now().strftime("%Y/%m/%d_%H:%M:%S")
        info_msg = f"Scraping cities: {now}"
        logger.info(info_msg)
        
        scrape_cp_cities(cities=cities, dir_save=dir_save_api_results, save_raw=True)
        
        # API call of all cities takes approx. 15 seconds, therefore subtract it from specified interval
        sleep = max(scraping_interval*60 - 15, 0)
        time.sleep(sleep)

def _append_poi_category(gdf_poi: GeoDataFrame,
                         tags: typing.Dict[str, list], 
                         col_poi_id: str = "id_poi"
                         ) -> GeoDataFrame: 
    """Append poi category from tag search to POI locations

    Args:
        gdf_poi (GeoDataFrame): POI locations
        tags (typing.Dict[str, list]): mapping containing osm key as key and list of osm values as value
        col_poi_id (str, optional): name of column uniquely identifying each POI. Defaults to "id_poi".

    Returns:
        GeoDataFrame: POI locations with POI category appended
    """    
    for key in tags: 
        if key not in set(gdf_poi.columns): 
            gdf_poi[key] = [None] * gdf_poi.shape[0]
    
    poi_category = pd.concat([gdf_poi.set_index(col_poi_id)[key] for key in tags]).dropna()
    allowed_values = [v for val in tags.values() for v in val]
    poi_category = poi_category[poi_category.isin(allowed_values)]
    poi_category = poi_category.groupby(col_poi_id).first()
    poi_category.name = "poi_cat"
    
    gdf_poi = gdf_poi.merge(poi_category, how="left", left_on=col_poi_id, right_index=True, validate="1:1")
    return gdf_poi


def _postprocess_osm_data(gdf_poi: GeoDataFrame, 
                          city_osm: str,
                          tags: typing.Dict[str, list],
                          cols_relevant: typing.List[str] = None
                          )  -> GeoDataFrame: 
    """Postprocess OSM POI locations: appending poi id and poi category, appending lat/lon coordinates, transform POI to 
    EPSG:25832
    
    Args:
        gdf_poi (GeoDataFrame): POI locations
        city_osm (str): name of city used for Nominatim Geocoding
        tags (typing.Dict[str, list]): mapping containing osm key as key and list of osm values as value
        cols_relevant (typing.List[str], optional): list of relevant columns. If not specified all columns are returned. Defaults to None.

    Returns:
        GeoDataFrame: [description]
    """    
    gdf_poi["city"] = city_osm
    gdf_poi.reset_index(drop=False, inplace=True)
    
    gdf_poi["id_poi"] = gdf_poi["element_type"] + "/" + gdf_poi["osmid"].astype(str)
    gdf_poi.drop(columns=["ways", "nodes"], errors="ignore", inplace=True)
    
    gdf_poi = _append_poi_category(gdf_poi=gdf_poi, tags=tags)
    
    if cols_relevant is None: 
        cols_relevant = gdf_poi.columns 
    
    repr_point =  gdf_poi.representative_point()
    gdf_poi["longitude"] = repr_point.x
    gdf_poi["latitude"] = repr_point.y
    gdf_poi["geom_type"] = gdf_poi.geom_type
    
    gdf_poi.to_crs("EPSG:25832", inplace=True)
    
    return gdf_poi


def get_poi_osm_data(tags: dict = TAGS, 
                     cities: typing.Dict[str, str] = CITIES_OSM, 
                     dir_save: str = "../data/osm", 
                     cols_relevant: list = ["geometry", "id_poi", "poi_cat", "longitude", "latitude"]): 
    """Get Points-of-Interest (POI) data from OpenstreetMap (OSM) with specified tags in specified cities 

    Args:
        tags (dict, optional): tags (OSM key-value combinations) to . Defaults to TAGS.
        cities (typing.List[str], optional):  list of cities. Defaults to CITIES_OSM.
        dir_save (str, optional): directory for saving poi data. Defaults to "../data/osm".
        cols_relevant (list, optiona): list of relevant columns to save. Defaults to ["poi_id", "geometry", "poi_cat"]
        
    """    
    poi_cities = {}
    for city_osm in cities.values(): 
        try: 
            logger.info(f"Acquiring POI data for city '{city_osm}.'")
            gdf_poi = ox.geometries_from_place(f"{city_osm}, Germany", tags=tags)
            if gdf_poi.shape[0] == 0: 
                raise ValueError(f"No data for city '{city_osm}'")
            logger.debug(f"Shape of POI data for city '{city_osm}': {gdf_poi.shape}")
            gdf_poi = _postprocess_osm_data(gdf_poi=gdf_poi, 
                                            city_osm=city_osm, 
                                            tags=tags, 
                                            cols_relevant=cols_relevant)
            poi_cities[city_osm] = gdf_poi
        except ValueError as e:
            logger.error(f"Error while acquiring POI for city '{city_osm}'. Nominatim Geocoder found no results.")
            continue
    
    gdf_pois_combined = pd.concat(poi_cities.values())
    logger.debug(f"Combined POI GeoDataFrame of shape: {gdf_pois_combined.shape}")
    fullpath_shp_file = os.path.join(dir_save, "poi_osm_{geom_type}.zip")
    
    # shapefile can only consist one geometry type (Point, Polygon)
    for geom_type, gdf_geom_type in gdf_pois_combined.groupby("geom_type"): 
        logger.info(f"Saving combined POI to GeoJSON to '{fullpath_shp_file.format(geom_type=geom_type.lower())}'.")
        gdf_geom_type[cols_relevant].to_file(fullpath_shp_file.format(geom_type=geom_type.lower()))  
    
        