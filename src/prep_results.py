import pandas as pd
from pandas import DataFrame
import numpy as np
from functools import reduce


def expand_df_dicts(df: DataFrame, 
                    *, 
                    index_col: str, 
                    index_new_df: str,
                    col_dicts: str, 
                    fill_value: str = "-", 
                    drop_col_dicts: bool = True,
                    drop_col_status: str = True, 
                    append_timestamp: bool = False,
                    cols_return: list = None
                    ) -> DataFrame: 
    
    df_unstacked = pd.DataFrame.from_dict(df.set_index(index_col)[col_dicts].to_dict(), orient="index")
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
    return df_expanded.drop(columns=df_expanded.columns.difference(_cols_return))



def _construct_df_from_json(data: dict) -> DataFrame:
    return pd.DataFrame.from_dict([{key: val for key, val in cs.items()} 
                                   for city in data for cs in data[city]["data"]])


def _construct_df_from_json_with_ts(data: dict, 
                                    cols_return: list = ["id", "timestamp", "evses"]
                                    )-> DataFrame: 
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


def extract_master_data(data: dict, 
                        drop_col_status: bool = True,
                        drop_col_dicts: bool = True
                        )-> DataFrame: 
    df_md_cs = extract_master_data_cs(data)
    
    df_md_cp = expand_df_dicts(df_md_cs,
                               index_col="id",
                               col_dicts="evses", 
                               index_new_df="id_cs", 
                               drop_col_dicts=drop_col_dicts,
                               drop_col_status=drop_col_status)
    
    df_md_conn = expand_df_dicts(df_md_cp, 
                                 index_col="id", 
                                 col_dicts="connectors",
                                 index_new_df="id_cp", 
                                 drop_col_dicts=drop_col_dicts, 
                                 drop_col_status=drop_col_status)
    
    if drop_col_dicts: 
        df_md_cs.drop(columns="evses", inplace=True)
        df_md_cp.drop(columns="connectors", inplace=True)
    
    return {"charging_stations": df_md_cs, "charging_points": df_md_cp, "connectors": df_md_conn}


def extract_status(file: str,
                   return_status_conn: str = False): 
    data = pd.read_pickle(file)
    if data: 
        df_cs = _construct_df_from_json_with_ts(data)
        df_status_cps = expand_df_dicts(df_cs,
                                       index_col="id",
                                       col_dicts="evses", 
                                       index_new_df="id_cs", 
                                       drop_col_dicts=False,
                                       drop_col_status=False,
                                       append_timestamp=True, 
                                       cols_return=["id", "status", "timestamp", "connectors"]
                                       )        
        if return_status_conn: 
            df_status_conn = expand_df_dicts(df_status_cps,
                                           index_col="id",
                                           col_dicts="connectors", 
                                           index_new_df="id_cs", 
                                           drop_col_dicts=True,
                                           drop_col_status=False,
                                           append_timestamp=True, 
                                           cols_return=["id", "status", "timestamp"]
                                           )
            return df_status_conn
        else: 
            return df_status_cps.drop(columns=["connectors"])
        

def apply_diff_ts(ser_ts): 
    return ser_ts.shift(-1) - ser_ts

def apply_grp_counter_bool_shift(ser_status_num): 
    return ser_status_num.ne(ser_status_num.shift(1).fillna(method="bfill")).cumsum()

def transform_df_charging_events(df_status, mapping_status):
    df_status["status_numerical"] = df_status["status"].map(mapping_status)
    df_status["event_counter"] = df_status.groupby("id")["status_numerical"].apply(apply_grp_counter_bool_shift).to_numpy()
    df_status = df_status.sort_values(by=["id", "timestamp"])
    
    df_charging_events = df_status.groupby(["id", "event_counter"]).first()
    df_charging_events.rename(columns={"timestamp": "ts_start"}, inplace=True)
    df_charging_events["ts_end"] = df_charging_events.groupby("id")["ts_start"].shift(-1)
    df_charging_events["delta_t"] = df_charging_events["ts_end"] - df_charging_events["ts_start"]
    return df_charging_events