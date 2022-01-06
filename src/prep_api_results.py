import pandas as pd
from pandas import DataFrame
import numpy as np
from functools import reduce
import glob
import os 
from os.path import basename
import json
import typing 
import zipfile
from zipfile import ZipFile


def expand_df_dicts(df: DataFrame, 
                    *, 
                    index_col: str, 
                    index_new_df: str,
                    col_dicts: str, 
                    fill_value: typing.Union[str, dict] = "-", 
                    drop_col_dicts: bool = True,
                    drop_col_status: str = True, 
                    append_timestamp: bool = False,
                    cols_return: list = None
                    ) -> DataFrame: 
    """Flatten DataFrame containing JSON/dictionary structure of ChargeCloud API request

    Args:
        df (DataFrame): DataFrame with JSON structure as column
        index_col (str): name of index column
        index_new_df (str): name of column of flattened DataFrame
        col_dicts (str): column containing dictionary structure
        fill_value (typing.Union[str, dict], optional): fill value for missing values in dictionary structure. Either a string as fill value or dictionary containing column name as key and respective fill value as value. Defaults to "-".
        drop_col_dicts (bool, optional): whether to drop column containing dictionary structure. Defaults to True.
        drop_col_status (str, optional): whether to drop column containing status information. Defaults to True.
        append_timestamp (bool, optional): whether to append timestamp of API request. Defaults to False.
        cols_return (list, optional): column(s) of returned DataFrame. If not specified all columns are returned. Defaults to None.

    Returns:
        DataFrame: flattened DataFrame
    """    
    df_unstacked = pd.DataFrame.from_dict(df.set_index(index_col)[col_dicts].to_dict(), 
                                          orient="index")
    
    df_expanded = (pd.DataFrame(df_unstacked
                               .stack().droplevel(level=1)
                               .reset_index(drop=False))
                   .rename(columns={"index": index_new_df, 0: "dict_info"})
                  )
    for key in reduce(set().union, df_expanded["dict_info"].map(lambda d: set(d.keys()))):
        if isinstance(fill_value, dict): 
            df_expanded[key] = df_expanded["dict_info"].map(lambda t: t.get(key, fill_value[key]))
        else: 
            df_expanded[key] = df_expanded["dict_info"].map(lambda t: t.get(key, fill_value))
            
    if drop_col_dicts: 
        df_expanded = df_expanded.drop(columns="dict_info")
    if drop_col_status and "status" in df_expanded.columns: 
        df_expanded = df_expanded.drop(columns="status")
    
    if append_timestamp and "timestamp" in df.columns: 
        df_expanded = df_expanded.merge(df.set_index("id")[["timestamp"]],
                                        left_on=index_new_df,
                                        right_index=True)
    if isinstance(cols_return, list): 
        _cols_return = cols_return 
    else: 
        _cols_return = df_expanded.columns
    
    df_result = df_expanded[_cols_return]
    
    return df_result



def _construct_df_from_json(data: dict) -> DataFrame:
    """Construct DataFrame from JSON ChargeCloud API result

    Args:
        data (dict): mapping containing city as key and ChargeCloud API result for city as value

    Returns:
        DataFrame: DataFrame of API results
    """    
    return pd.DataFrame.from_dict([{key: val for key, val in cs.items()} 
                                   for city in data for cs in data[city]["data"]])


def _construct_df_from_json_with_ts(data: dict, 
                                    cols_return: list = ["id", "timestamp", "evses"]
                                    )-> DataFrame: 
    """Construct DataFrame from JSON ChargeCloud API result with timestamp of API request

    Args:
        data (dict): mapping containing city as key and ChargeCloud API result for city as value
        cols_return (list, optional): columns of returned DataFrame. If not specified all columns are returned. Defaults to ["id", "timestamp", "evses"].

    Returns:
        DataFrame: DataFrame of API results
    """    
    list_df_cities = list()
    for city in data: 
        df_cs_city = pd.DataFrame.from_dict([{key: val for key, val in cs.items()} for cs in data[city]["data"]])
        df_cs_city["timestamp"] = data[city]["timestamp"]
        list_df_cities.append(df_cs_city)
    df_cs_cities = pd.concat(list_df_cities, ignore_index=True) 
    df_cs_cities["timestamp"] = pd.to_datetime(df_cs_cities["timestamp"])
    if isinstance(cols_return, list): 
        _cols_return = cols_return 
    else: 
        _cols_return = df_cs_cities.columns    
    return df_cs_cities.drop(columns=df_cs_cities.columns.difference(_cols_return))


def extract_master_data_cs(data: dict) -> DataFrame:
    """Extract charging station master data from API result

    Args:
        data (dict): mapping containing city as key and ChargeCloud API result for city as value

    Returns:
        DataFrame: master data of charging stations
    """    
    df_cs = _construct_df_from_json(data)
    df_cs["latitude"] = df_cs["coordinates"].map(lambda d: float(d.get("latitude", np.nan)))
    df_cs["longitude"] = df_cs["coordinates"].map(lambda d: float(d.get("longitude", np.nan)))
    df_cs.drop(columns="coordinates", inplace=True)
    
    df_cs["operator_name"] = df_cs["operator"].map(lambda d: d.get("name", "-"))
    df_cs["operator_hotline"] = df_cs["operator"].map(lambda d: d.get("hotline", "-"))
    df_cs.drop(columns="operator", inplace=True)
    
    df_cs["open_24_7"] = df_cs["opening_times"].map(lambda d: bool(d.get("twentyfourseven", False)))
    df_cs["opening_times_expanded"] = df_cs["opening_times"].map(lambda d: d.get("regular_hours", "-"))
    df_cs.drop(columns="opening_times", inplace=True)
    return df_cs


def _read_api_results(fullpath_query_result: str) -> dict:
    """Read ChargeCloud API result of one query interval

    Args:
        fullpath_query_result (str): path to file of ChargeCloud API result

    Returns:
        dict: dictionary containing cities as keys and ChargeCloud API results as values
    """        
    _, file_extension = os.path.splitext(fullpath_query_result)
    
    if file_extension == ".pkl": 
        return pd.read_pickle(fullpath_query_result)
    elif file_extension == ".json": 
        with open(fullpath_query_result, 'r') as f: 
            return json.load(f)
        

def extract_master_data(fullpath_query_result: str, 
                        drop_col_status: bool = True,
                        flatten_results: bool = True
                        ) -> typing.Tuple[DataFrame, DataFrame, DataFrame]: 
    """Extract charging station, charging point and connector master data from API result

    Args:
        fullpath_query_result (str): path to file of ChargeCloud API result
        drop_col_status (bool, optional): whether to drop status column from API result. Defaults to True.
        flatten_results (bool, optional): whether to drop columns containing non-flat datastructures (list, dicts). Defaults to True.

    Returns:
        typing.Tuple[DataFrame, DataFrame, DataFrame]: [description]
    """    
    data = _read_api_results(fullpath_query_result=fullpath_query_result)

    df_md_cs = extract_master_data_cs(data)
    
    df_md_cp = expand_df_dicts(df_md_cs,
                               index_col="id",
                               col_dicts="evses", 
                               index_new_df="id_cs", 
                               drop_col_dicts=flatten_results,
                               drop_col_status=drop_col_status)
    
    df_md_conn = expand_df_dicts(df_md_cp, 
                                 index_col="id", 
                                 col_dicts="connectors",
                                 index_new_df="id_cp", 
                                 drop_col_dicts=flatten_results, 
                                 drop_col_status=drop_col_status)
    
    if flatten_results: 
        df_md_cs.drop(columns=["evses", "opening_times_expanded", "directions"], inplace=True)
        df_md_cp.drop(columns=["connectors", "capabilities", ], inplace=True)
    
    return df_md_cs, df_md_cp, df_md_conn


def extract_status(fullpath_query_result: str,
                   return_type: str = "both"
                   )-> typing.Union[DataFrame, typing.Tuple[DataFrame, DataFrame, DataFrame]]: 
    """Extract status information from charging points and connectors

    Args:
        fullpath_query_result (str): path to result of ChargeCloud API result
        return_type (str, optional): Any of {'status_cps', 'status_connectors', 'both'}. Whether to return status of chargingpoints, status of connectors or both. Defaults to "both".

    Raises:
        ValueError: Raised if return_type is none of {'status_cps', 'status_connectors', 'both'}

    Returns:
        typing.Union[DataFrame, typing.Tuple[DataFrame, DataFrame, DataFrame]]: Status of chargingpoints, status of connectors or both
    """    
    data = _read_api_results(fullpath_query_result=fullpath_query_result)
    
    if data: 
        df_cs = _construct_df_from_json_with_ts(data)
        df_status_cps = expand_df_dicts(df_cs,
                                       index_col="id",
                                       col_dicts="evses", 
                                       index_new_df="id_cs", 
                                       drop_col_dicts=False,
                                       drop_col_status=False,
                                       append_timestamp=True, 
                                       cols_return=["id", "status", "parkingsensor_status", "timestamp", "connectors"]
                                       )        
        if return_type in ["both", "status_connectors"]: 
            df_status_conn = expand_df_dicts(df_status_cps,
                                             index_col="id",
                                             col_dicts="connectors", 
                                             index_new_df="id_cs", 
                                             drop_col_dicts=True,
                                             drop_col_status=False,
                                             append_timestamp=True, 
                                             cols_return=["id", "status", "timestamp"]
                                             )
        
        df_status_cps.drop(columns=["connectors"], inplace=True)
        
        if return_type == "status_cps": 
            return df_status_cps
        elif return_type == "status_connectors": 
            return df_status_conn
        elif return_type == "both": 
            return df_status_cps, df_status_conn
        else: 
            err_msg = "Parameter 'return_type' must be any of {'status_cps', 'status_connectors', 'both'}, "
            err_msg += f"but is actually '{return_type}"
            raise ValueError(err_msg)



def _postprocess_master_data(files_results: list, 
                             dir_master_data: str, 
                             output_format: str = "csv", 
                             storage_options: dict = None): 
    """Postprocess master data of chargingstations, chargingpoints and connectors from API results

    Args:
        files_results (list): list of paths of ChargeCloud API results
        dir_master_data (str): directory to save master data results to
        output_format (str, optional): output format for master data. Defaults to "csv".
        storage_options (dict, optional): storage options when writing to S3. 


    Raises:
        NotImplementedError: Raised if invalid output format is provided.
    """    
    # as results are sorted by date take the latest API result for most recent master data
    df_md_cs, df_md_cp, df_md_conn = extract_master_data(files_results[-1])
    
    if output_format == "csv": 
        fullpath_cs = os.path.join(dir_master_data, "charging_stations.csv")
        df_md_cs.to_csv(fullpath_cs, sep=";", index=False, storage_options=storage_options)
        
        fullpath_cp = os.path.join(dir_master_data, "charging_points.csv")
        df_md_cp.to_csv(fullpath_cp, sep=";", index=False,  storage_options=storage_options)
        
        fullpath_conn = os.path.join(dir_master_data, "connectors.csv")
        df_md_conn.to_csv(fullpath_conn, sep=";", index=False, storage_options=storage_options)
    else: 
        raise NotImplementedError("Other output formats other '.csv' are not yet implemented.")


def _postprocess_status_data(files_results: str, 
                             dir_status_cps: str, 
                             dir_status_connectors: str, 
                             storage_options: dict = None): 
    """Postprocess status data of chargingpoints and connectors from API results

    Args:
        files_results (list): list of paths of ChargeCloud API results
        dir_status_cps (str): directory to save chargingpoint status data
        dir_status_connectors (str): directory to save connector status data
        storage_options (dict, optional): storage options when writing to S3. 
    
    """    
    status_cps = []
    status_connectors = []
    
    for f in files_results: 
        df_status_cp, df_status_conn = extract_status(f, return_type="both")
        
        status_cps.append(df_status_cp)
        status_connectors.append(df_status_conn)
        
    df_status_cps = pd.concat(status_cps)
    df_status_conns = pd.concat(status_connectors)    
    
    fullpath_status_cps = os.path.join(dir_status_cps, "status_cps.csv")
    df_status_cps.to_csv(fullpath_status_cps, index=False, sep=";", storage_options=storage_options)
    
    fullpath_status_conn = os.path.join(dir_status_connectors, "status_connectors.csv")
    df_status_conns.to_csv(fullpath_status_conn, index=False, sep=";", storage_options=storage_options)


def _archive_raw_results(files_result: str, dir_zip: str, name_zipfile: str = None, compression=zipfile.ZIP_BZIP2): 
    """Convert raw results to ZIP-file for archieval purposes

    Args:
        files_results (list): list of paths of ChargeCloud API results
        dir_zip (str): directory to save zip archive 
        name_zipfile (str, optional): name of zipfile. If not provided date of last API result is incorporated in ZIP file. Defaults to None.
        compression (int, optional): ZIPfile compression type. Defaults to zipfile.ZIP_BZIP2
    """    
    
    #https://thispointer.com/python-how-to-create-a-zip-archive-from-multiple-files-or-directory/
   
    if name_zipfile is None: 
       query_datetime = basename(files_result[-1]).split("_cp_data_cities", maxsplit=1)[0]
       name_zipfile = "archive_cp_data_cities_" + query_datetime + ".zip"
    
    fullpath_zip = os.path.join(dir_zip, name_zipfile)
    
    with ZipFile(fullpath_zip, 'w', compression=compression) as zip_obj:
        for f in files_result: 
            zip_obj.write(f)

    
def postprocess_api_results(dir_api_results: str, 
                            dir_status_cps: str, 
                            dir_status_connectors: str, 
                            dir_master_data: str, 
                            dir_zip_file: str = None):
    """Postprocess ChargeCloud API result. Extract master data (chargingstations, chargingpoints and connectors) and 
    status information (chargingpoints, connectors) and archive raw API results. 

    Args:
        dir_api_results (str): directory containing API results.
        dir_status_cps (str): directory to save chargingpoint status data
        dir_status_connectors (str): directory to save connector status data
        dir_master_data (str): directory to save master data results to
        dir_zip (str, optional): directory to save zip archive. If not specified raw files will not be archived. Defaults to None. 
        
    """    
    files_results = glob.glob(dir_api_results)
    files_results.sort()
    
    _postprocess_master_data(files_results=files_results, dir_master_data=dir_master_data)
    
    _postprocess_status_data(files_results=files_results, 
                             dir_status_cps=dir_status_cps, 
                             dir_status_connectors=dir_status_connectors)
    
    _archive_raw_results(files_result=files_results, dir_zip_file=dir_zip_file)