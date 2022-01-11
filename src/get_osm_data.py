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


DIR_SAVE_OSM = "../data/osm"


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
                     dir_save: str = DIR_SAVE_OSM, 
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