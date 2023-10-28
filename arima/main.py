#!/usr/bin/env python3
"""
    Running Auto ARIMA on cards
"""

# Boilerplate
__author__ = "Ben"
__version__ = "1.0"
__license__ = "MIT"

from database.db import ETGDatabase

import streamlit as st
import pandas as pd
from plotly import graph_objects as go
import pmdarima as pm

@st.cache_data
def load_data(what: list[str]):
    db = ETGDatabase()
    df = db.get_tables(what)
    return df

@st.cache_data
def get_all_card_names(df: pd.DataFrame):
    return list(sorted(df["name"].unique()))

def main() -> int:
    st.set_page_config(layout='wide')
    df = load_data(["Cards", "Prices"])
    page = st.sidebar.selectbox("What would you like to see", ("MTG Prices", ))
    
    if page == "MTG Prices":
        cards = st.sidebar.multiselect("Which cards would you like to look at", get_all_card_names(df))
        
        fig = go.Figure(
            layout=dict(
                width=1400,
                height=800,
            )
        )
        for card in cards:
            sub_table = df[df["name"] == card]
            st.dataframe(sub_table)
            for card_id in sub_table["id"].unique():
                small_table = sub_table[sub_table["id"] == card_id]
                name = f"{card} - {small_table.iloc[0]['set_name']}"
                
                if small_table.iloc[0]["border_color"] == "borderless":
                    name += " (Borderless)"

                if not small_table["usd"].isnull().any  ():
                    fig.add_trace(go.Scatter(x=small_table["utc"], y=small_table["usd"], name=f"{name}"))

                    model = pm.auto_arima(small_table["usd"])
                    forecasts = model.predict(14)
                    date_range = pd.date_range(start=small_table.iloc[-1]["utc"], periods=14)
                    fig.add_trace(go.Scatter(x=date_range, y=forecasts, name=f"{name} | ARIMA prediction"))

                if not small_table["usd_foil"].isnull().any():
                    fig.add_trace(go.Scatter(x=small_table["utc"], y=small_table["usd_foil"], name=f"{name} (Foil)"))

                    model = pm.auto_arima(small_table["usd_foil"])
                    forecasts = model.predict(14)
                    date_range = pd.date_range(start=small_table.iloc[-1]["utc"], periods=14)
                    fig.add_trace(go.Scatter(x=date_range, y=forecasts, name=f"{name} (Foil) | ARIMA prediction"))

                if not small_table["usd_etched"].isnull().any():
                    fig.add_trace(go.Scatter(x=small_table["utc"], y=small_table["usd_etched"], name=f"{name} (Foil Etched)"))

                    model = pm.auto_arima(small_table["usd_etched"])
                    forecasts = model.predict(14)
                    date_range = pd.date_range(start=small_table.iloc[-1]["utc"], periods=14)
                    fig.add_trace(go.Scatter(x=date_range, y=forecasts, name=f"{name} (Foil Etched) | ARIMA prediction"))


        st.plotly_chart(fig, use_container_width=True)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())