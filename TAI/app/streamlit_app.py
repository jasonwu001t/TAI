# app/streamlit_app.py
import streamlit as st
import pandas as pd
from TAI.source.fred import Fred
from TAI.analytics.plotly_plots import QuickPlot

class StreamlitApp:
    def __init__(self):
        self.fred = Fred()

    def run(self):
        st.title("Streamlit FRED Data Viewer")

        series_code = st.text_input("Enter FRED series code:", "gdp")
        if series_code:
            data = self.fred.get_series(series_code).reset_index()
            data.columns = ['date', 'value']

            st.subheader(f"Data for {series_code}")
            st.write(data)

            qp = QuickPlot(dataframes=[data], labels=[series_code])
            fig = qp.plot_line(title=f"Plot of {series_code}")
            st.plotly_chart(fig)

if __name__ == "__main__":
    app = StreamlitApp()
    app.run()
