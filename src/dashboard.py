import streamlit as st
import pandas as pd 
import configparser
import psycopg2
import geopandas as gpd 
import numpy as np
import altair as alt 



CONFIG_FILE = "../config.cfg"

# can only set this once, first thing to set
st.set_page_config(layout="wide")

config = configparser.ConfigParser()
config.read(CONFIG_FILE)

@st.experimental_singleton
def get_db_connection(): 
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    return conn


@st.experimental_memo
def get_master_data(_conn): 
    
    df_cs = pd.read_sql(con=_conn, sql="select * from charging_station")
    df_cp = pd.read_sql(con=_conn, sql="select * from charging_point")
    df_conn = pd.read_sql(con=_conn, sql="select * from connector")
    gdf_poi = gpd.read_postgis(con=_conn, sql="select * from poi")
    
    return df_cs, df_cp, df_conn, gdf_poi

STATUS_NUMERICAL = {"AVAILABLE": 0, 
                    "RESERVED": 0.5,
                    "CHARGING": 1,
                    "OUTOFORDER": -1, 
                    "UNKNOWN": np.nan}

@st.experimental_memo
def get_status_data(_conn, id, query_cp=True):
    table = "status_chargingpoints" if query_cp else "status_connectors"
    col_id = "id_chargingpoint" if query_cp else "id_conn"
    col_status = "status_cp" if query_cp else "status_connector"
    sql = f"select query_time, {col_status} as status from {table} where {col_id} = '{id}'"
    df_status = pd.read_sql(con=_conn, sql=sql)
    
    df_status["status_num"] = df_status["status"].map(STATUS_NUMERICAL)
    return df_status 


conn = get_db_connection()

df_cs, df_cp, df_conn, gdf_poi = get_master_data(conn)


    
# st.dataframe(df_cs)
# st.dataframe(df_cp)
# st.dataframe(df_conn)
# st.dataframe(gdf_poi)

@st.experimental_memo
def get_single_cs(cs_id, df_cs): 
    return df_cs.query(f"id_cs=={cs_id}")

@st.experimental_memo
def get_cps(cs_id, df_cp): 
    return df_cp.query(f"id_cs=={cs_id}").reset_index(drop=True)

@st.experimental_memo
def get_connectors(cp_id, df_conn): 
    return df_conn.query(f"id_cp=='{cp_id}'").reset_index(drop=True)


st.sidebar.image("https://iconoir.com/source/icons/ev-plug-charging.svg")
cs_id = st.sidebar.selectbox('Select charging station by id:', df_cs["id_cs"])
df_cs_selected = get_single_cs(cs_id=cs_id, df_cs=df_cs)
ser_cs = df_cs_selected.iloc[0]
df_cp_selected = get_cps(cs_id=cs_id, df_cp=df_cp)
cp_id = st.sidebar.selectbox('Select charging point by id:', df_cp_selected["id_cp"])

df_connectors_selected = get_connectors(cp_id=cp_id, df_conn=df_conn)
df_status = get_status_data(id=cp_id, _conn=conn)


#st.dataframe(df_status)

with st.container():
    st.title("Charging station utilization")
    st.header(f"Charging station {ser_cs.operator_name}: {ser_cs.address}, {ser_cs.postal_code} {ser_cs.city}.")
    st.header(f"Charging point: {cp_id}")
    #st.dataframe(df_connectors_selected)
    #st.write("""See the code and plots for five libraries at once.""")

col1, col2, col3 = st.columns(3)

brush = alt.selection(type='interval',  encodings=['y'])

c = (alt.Chart(df_status).mark_point().encode(
     x='query_time', y='status_num', color=alt.condition(brush, 'status', alt.ColorValue('gray'))).add_selection(
    brush
)
     .interactive()
     .properties(
    title="Utilization Data for Charging Point",
    width=400,
    height=300,
)
     )

with col1:
    df_plot = df_cs_selected
    st.map(df_plot)

with col2: 
    #st.slider("Filter results", df_status.query_time.min(), df_status.query_time.max(), value, step)
    st.altair_chart(c, use_container_width=True)
    bar_chart_input = pd.DataFrame(df_status["status"].value_counts())
    bar_chart_input["status_percentage"] = np.round(bar_chart_input["status"]/bar_chart_input["status"].sum() * 100)/100
    bar_chart_input.drop(columns="status", inplace=True)
    bar_chart_input.index.name = "status"
    bar_chart_input.reset_index(drop=False, inplace=True)
    #st.bar_chart(bar_chart_input)
    
    
    base = alt.Chart(bar_chart_input).encode(
    theta=alt.Theta("status_percentage:Q", stack=True),
    radius=alt.Radius("status_percentage", scale=alt.Scale(type="sqrt", zero=True, rangeMin=20)),
    color="status:N",
)

    c1 = base.mark_arc(innerRadius=20, stroke="#fff")

    c2 = base.mark_text(radiusOffset=10, ).encode(text=alt.Text("status_percentage:Q", format='.0%'))
 
    c = (c1 + c2).properties(
    title="Percentage of utilization",
    width=400,
    height=300,
)
     
    st.altair_chart(c, use_container_width=True)

