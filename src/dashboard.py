import streamlit as st
import pandas as pd 


# can only set this once, first thing to set
st.set_page_config(layout="wide")

df_cs = pd.read_csv("../data/charging_stations.csv", sep=";")
df_cp = pd.read_csv("../data/charging_points.csv", sep=";")
df_conn = pd.read_csv("../data/connectors.csv", sep=";")

with st.container():
    st.title("Python Data Visualization Tour")
    st.header("Popular plots in popular plotting libraries")
    st.write("""See the code and plots for five libraries at once.""")
    
    
#st.dataframe(df_cs)

#st.dataframe(df_cp)
#st.dataframe(df_conn)


a = st.sidebar.selectbox('Select charging station by id:', df_cs["id"])